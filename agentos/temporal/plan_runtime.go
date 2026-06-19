package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	goredis "github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	artifactrepo "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
)

type planRuntime struct {
	temporalClient planTemporalClient
	closeTemporal  bool
	redis          *goredis.Redis
	postgres       *postgres.Postgres
	planEvents     agentosplan.PlanEventStore
	planLiveEvents agentosplan.PlanEventSubscriber
	planIndex      agentosplan.PlanIndex
	artifactStore  agentosplan.ArtifactStore
	auditStore     agentosplan.AuditStore
	taskQueue      string
}

var (
	errPlanRuntimePlanIndexRequired      = errors.New("agentos temporal plan runtime: plan index is not configured")
	errPlanRuntimePlanEventStoreRequired = errors.New("agentos temporal plan runtime: plan event store is not configured")
	errPlanRuntimeArtifactStoreRequired  = errors.New("agentos temporal plan runtime: artifact store is not configured")
	errPlanRuntimeAuditStoreRequired     = errors.New("agentos temporal plan runtime: audit store is not configured")
)

type planTemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
	Close()
}

// NewPlanRuntime creates the default Temporal implementation of agentos.PlanRuntime.
func NewPlanRuntime(ctx context.Context, cfg RuntimeConfig) (agentos.PlanRuntime, error) {
	fwTemporal := temporalConfig(cfg)
	c, err := client.Dial(client.Options{
		HostPort:  fwTemporal.Address,
		Namespace: fwTemporal.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("agentos temporal plan runtime client: %w", err)
	}

	rt, err := newPlanRuntimeWithClient(ctx, cfg, c, true)
	if err != nil {
		c.Close()

		return nil, err
	}

	return rt, nil
}

// NewPlanRuntimeWithClient adapts an existing Temporal client to agentos.PlanRuntime.
func NewPlanRuntimeWithClient(ctx context.Context, cfg RuntimeConfig, c client.Client) (agentos.PlanRuntime, error) {
	if c == nil {
		return nil, errors.New("agentos temporal plan runtime: nil temporal client")
	}

	return newPlanRuntimeWithClient(ctx, cfg, c, false)
}

func newPlanRuntimeWithClient(ctx context.Context, cfg RuntimeConfig, c planTemporalClient, closeTemporal bool) (*planRuntime, error) {
	fwTemporal := temporalConfig(cfg)
	rt := &planRuntime{
		temporalClient: c,
		closeTemporal:  closeTemporal,
		taskQueue:      fwTemporal.TaskQueue,
	}
	if cfg.RedisURL != "" {
		rdb, err := goredis.New(ctx, cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("agentos temporal plan runtime redis: %w", err)
		}
		rt.redis = rdb
		rt.planLiveEvents = repostream.NewRedisPlanEventStream(rdb)
	}
	if cfg.PostgresURL != "" {
		pg, err := newRuntimePostgres(cfg)
		if err != nil {
			_ = rt.Close()

			return nil, fmt.Errorf("agentos temporal plan runtime postgres: %w", err)
		}
		rt.postgres = pg
		planStore := temporalrepo.NewAgentOSPlanRepo(pg)
		rt.planEvents = planStore
		rt.planIndex = planStore
		rt.auditStore = planStore
		if cfg.ArtifactStore.Backend != "" {
			blobStore, err := artifactrepo.NewBlobStore(ctx, artifactBlobConfig(cfg.ArtifactStore))
			if err != nil {
				_ = rt.Close()

				return nil, fmt.Errorf("agentos temporal plan runtime artifact store: %w", err)
			}
			rt.artifactStore = temporalrepo.NewAgentOSArtifactRepo(pg, blobStore)
		}
	}

	return rt, nil
}

func (r *planRuntime) StartPlan(ctx context.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.AccountID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: account id is required", agentos.ErrInvalidPlanScope)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	if r.planIndex == nil {
		return agentos.RunPlanStatus{}, errPlanRuntimePlanIndexRequired
	}

	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	status, created, err := r.planIndex.CreatePlan(ctx, spec, status)
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}
	if !created && status.LifecycleState != agentos.PlanLifecyclePending {
		return status, nil
	}
	if created {
		if _, err := r.recordPlanAudit(ctx, agentosplan.AuditRecord{
			PlanID:         spec.PlanID,
			Action:         agentosplan.AuditActionPlanStart,
			IdempotencyKey: spec.IdempotencyKey,
			Payload: map[string]any{
				"thread_id":  spec.ThreadID,
				"account_id": spec.AccountID,
				"project_id": spec.ProjectID,
			},
		}); err != nil {
			return agentos.RunPlanStatus{}, err
		}
	}

	_, err = r.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        planWorkflowID(spec.PlanID),
		TaskQueue: r.taskQueue,
	}, PlanWorkflowName, planWorkflowInput{Spec: spec})
	if err != nil && !sdktemporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return agentos.RunPlanStatus{}, fmt.Errorf("agentos temporal plan runtime - start plan workflow: %w", err)
	}

	status.LifecycleState = agentos.PlanLifecycleRunning
	status.UpdatedAt = time.Now().UTC()
	if err := r.planIndex.UpdatePlanStatus(ctx, status, spec.IdempotencyKey); err != nil {
		return agentos.RunPlanStatus{}, err
	}

	return status, nil
}

func (r *planRuntime) StatusPlan(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanStatus, error) {
	_, status, err := r.authorizePlan(ctx, ref)

	return status, err
}

func (r *planRuntime) authorizePlan(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, error) {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}
	if r.planIndex == nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, errPlanRuntimePlanIndexRequired
	}

	spec, status, exists, err := r.planIndex.GetPlan(ctx, ref.PlanID)
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}
	if !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, ref.PlanID)
	}
	if err := agentosplan.ValidatePlanTenantAccess(ref, spec); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}

	return spec, status, nil
}

func (r *planRuntime) SignalPlan(ctx context.Context, ref agentos.PlanRef, signal agentos.Signal) error {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return err
	}
	if err := agentosplan.ValidatePlanSignal(signal); err != nil {
		return err
	}
	if _, _, err := r.authorizePlan(ctx, ref); err != nil {
		return err
	}
	record := planSignalAuditRecord(ref.PlanID, signal)
	exists, err := r.planAuditRecorded(ctx, record)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(ref.PlanID), "", PlanSignalName, signal); err != nil {
		return fmt.Errorf("agentos temporal plan runtime - signal plan workflow: %w", err)
	}
	if _, err := r.recordPlanAudit(ctx, record); err != nil {
		return err
	}

	return nil
}

func (r *planRuntime) ControlPlan(ctx context.Context, ref agentos.PlanRef, control agentos.ControlRequest) error {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return err
	}
	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}
	if control.IdempotencyKey == "" {
		return fmt.Errorf("%w: control idempotency key is required", agentos.ErrInvalidControlOperation)
	}
	if control.ActorID == "" {
		return fmt.Errorf("%w: control actor id is required", agentos.ErrInvalidControlOperation)
	}
	if _, _, err := r.authorizePlan(ctx, ref); err != nil {
		return err
	}
	record := agentosplan.AuditRecord{
		PlanID:         ref.PlanID,
		ActorID:        control.ActorID,
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: control.IdempotencyKey,
		Payload: map[string]any{
			"operation": control.Operation,
			"metadata":  control.Metadata,
		},
	}
	exists, err := r.planAuditRecorded(ctx, record)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(ref.PlanID), "", PlanControlSignalName, control); err != nil {
		return fmt.Errorf("agentos temporal plan runtime - control plan workflow: %w", err)
	}
	if _, err := r.recordPlanAudit(ctx, record); err != nil {
		return err
	}

	return nil
}

func (r *planRuntime) SubscribePlan(ctx context.Context, scope agentos.PlanStreamScope) (agentos.Subscription, error) {
	if r.planEvents == nil {
		return nil, errPlanRuntimePlanEventStoreRequired
	}
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return nil, err
	}
	if _, _, err := r.authorizePlan(ctx, agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}); err != nil {
		return nil, err
	}

	events, err := r.planEvents.ListPlanEvents(ctx, scope, 0)
	if err != nil {
		return nil, err
	}
	if r.planLiveEvents == nil {
		return newPlanReplaySubscription(events), nil
	}

	liveScope := scope
	liveScope.AfterSequence = lastPlanEventSequence(scope.AfterSequence, events)
	live, err := r.planLiveEvents.SubscribePlanEvents(ctx, liveScope)
	if err != nil {
		return nil, err
	}

	return newPlanReplayThenLiveSubscription(events, live), nil
}

func (r *planRuntime) ListPlanAudits(ctx context.Context, scope agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	if r.auditStore == nil {
		return nil, errPlanRuntimeAuditStoreRequired
	}
	if err := agentosplan.ValidatePlanAuditScope(scope); err != nil {
		return nil, err
	}
	if _, _, err := r.authorizePlan(ctx, agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}); err != nil {
		return nil, err
	}

	return r.auditStore.ListAuditRecords(ctx, scope)
}

func (r *planRuntime) ListPlanArtifacts(ctx context.Context, scope agentos.PlanArtifactScope) ([]agentos.ArtifactRef, error) {
	if r.artifactStore == nil {
		return nil, errPlanRuntimeArtifactStoreRequired
	}
	if err := agentosplan.ValidatePlanArtifactScope(scope); err != nil {
		return nil, err
	}
	if _, _, err := r.authorizePlan(ctx, agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}); err != nil {
		return nil, err
	}

	refs, err := r.artifactStore.List(ctx, scope.PlanID)
	if err != nil {
		return nil, err
	}

	return filterPlanArtifactRefs(refs, scope), nil
}

func (r *planRuntime) GetPlanArtifact(ctx context.Context, scope agentos.PlanArtifactScope) (agentos.Artifact, error) {
	if r.artifactStore == nil {
		return agentos.Artifact{}, errPlanRuntimeArtifactStoreRequired
	}
	if err := agentosplan.ValidatePlanArtifactScope(scope); err != nil {
		return agentos.Artifact{}, err
	}
	if scope.ArtifactID == "" {
		return agentos.Artifact{}, fmt.Errorf("%w: artifact id is required", agentos.ErrInvalidArtifact)
	}
	if _, _, err := r.authorizePlan(ctx, agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}); err != nil {
		return agentos.Artifact{}, err
	}

	ref, payload, err := r.artifactStore.Get(ctx, scope.ArtifactID)
	if err != nil {
		return agentos.Artifact{}, err
	}
	if !planArtifactRefMatches(ref, scope) {
		return agentos.Artifact{}, fmt.Errorf("%w: %s", agentos.ErrArtifactNotFound, scope.ArtifactID)
	}

	return agentos.Artifact{Ref: ref, Payload: payload}, nil
}

func (r *planRuntime) Close() error {
	var errs []error
	if r.closeTemporal && r.temporalClient != nil {
		r.temporalClient.Close()
	}
	if r.redis != nil {
		errs = append(errs, r.redis.Close())
	}
	if r.postgres != nil {
		r.postgres.Close()
	}

	return errors.Join(errs...)
}

func (r *planRuntime) recordPlanAudit(ctx context.Context, record agentosplan.AuditRecord) (bool, error) {
	if r.auditStore == nil {
		return false, errPlanRuntimeAuditStoreRequired
	}
	_, created, err := r.auditStore.RecordAudit(ctx, record)

	return created, err
}

func (r *planRuntime) planAuditRecorded(ctx context.Context, record agentosplan.AuditRecord) (bool, error) {
	if r.auditStore == nil {
		return false, errPlanRuntimeAuditStoreRequired
	}
	existing, exists, err := r.auditStore.GetAuditRecord(ctx, record.IdempotencyKey)
	if err != nil || !exists {
		return exists, err
	}
	if err := agentosplan.ValidateAuditIdempotency(existing, record); err != nil {
		return false, err
	}

	return true, nil
}

func planWorkflowID(planID string) string {
	return "agentos-plan-" + planID
}

func planSignalAuditRecord(planID string, signal agentos.Signal) agentosplan.AuditRecord {
	return agentosplan.AuditRecord{
		PlanID:         planID,
		ActorID:        signal.ActorID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: signal.IdempotencyKey,
		Payload: map[string]any{
			"type":    signal.Type,
			"payload": signal.Payload,
		},
	}
}

func filterPlanArtifactRefs(refs []agentos.ArtifactRef, scope agentos.PlanArtifactScope) []agentos.ArtifactRef {
	filtered := make([]agentos.ArtifactRef, 0, len(refs))
	for _, ref := range refs {
		if !planArtifactRefMatches(ref, scope) {
			continue
		}
		filtered = append(filtered, ref)
		if scope.Limit > 0 && len(filtered) >= scope.Limit {
			break
		}
	}

	return filtered
}

func planArtifactRefMatches(ref agentos.ArtifactRef, scope agentos.PlanArtifactScope) bool {
	if ref.PlanID != scope.PlanID {
		return false
	}
	if scope.ArtifactID != "" && ref.ArtifactID != scope.ArtifactID {
		return false
	}
	if scope.NodeID != "" && ref.NodeID != scope.NodeID {
		return false
	}
	if scope.RunID != "" && ref.RunID != scope.RunID {
		return false
	}

	return true
}

func newRuntimePostgres(cfg RuntimeConfig) (*postgres.Postgres, error) {
	if cfg.PostgresPoolMax > 0 {
		return postgres.New(cfg.PostgresURL, postgres.MaxPoolSize(cfg.PostgresPoolMax))
	}

	return postgres.New(cfg.PostgresURL)
}
