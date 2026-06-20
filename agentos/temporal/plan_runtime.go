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
	commandStore   agentosplan.PlanCommandStore
	auditStore     agentosplan.AuditStore
	taskQueue      string
}

var (
	errPlanRuntimePlanIndexRequired      = errors.New("agentos temporal plan runtime: plan index is not configured")
	errPlanRuntimePlanEventStoreRequired = errors.New("agentos temporal plan runtime: plan event store is not configured")
	errPlanRuntimeArtifactStoreRequired  = errors.New("agentos temporal plan runtime: artifact store is not configured")
	errPlanRuntimeCommandStoreRequired   = errors.New("agentos temporal plan runtime: plan command store is not configured")
	errPlanRuntimeAuditStoreRequired     = errors.New("agentos temporal plan runtime: audit store is not configured")

	ErrPlanRuntimePostgresURLRequired              = errors.New("agentos temporal plan runtime: postgres url is required")
	ErrPlanRuntimeArtifactStoreBackendRequired     = errors.New("agentos temporal plan runtime: artifact store backend is required")
	ErrPlanRuntimeArtifactStoreBackendUnknown      = errors.New("agentos temporal plan runtime: artifact store backend is unknown")
	ErrPlanRuntimeArtifactStoreLocalRootRequired   = errors.New("agentos temporal plan runtime: artifact local root is required")
	ErrPlanRuntimeArtifactStoreS3BucketRequired    = errors.New("agentos temporal plan runtime: artifact s3 bucket is required")
	ErrPlanRuntimeArtifactStoreS3RegionRequired    = errors.New("agentos temporal plan runtime: artifact s3 region is required")
	ErrPlanRuntimeArtifactStoreS3AccessKeyRequired = errors.New("agentos temporal plan runtime: artifact s3 access key id is required")
	ErrPlanRuntimeArtifactStoreS3SecretKeyRequired = errors.New("agentos temporal plan runtime: artifact s3 secret access key is required")
)

const (
	planCommandPayloadSignalType  = "type"
	planCommandPayloadPayload     = "payload"
	planCommandPayloadSentAt      = "sent_at"
	planCommandPayloadOperation   = "operation"
	planCommandPayloadMetadata    = "metadata"
	planCommandPayloadRequestedAt = "requested_at"
)

type planTemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
	Close()
}

// NewPlanRuntime creates the default Temporal implementation of agentos.PlanRuntime.
func NewPlanRuntime(ctx context.Context, cfg RuntimeConfig) (agentos.PlanRuntime, error) {
	if err := validatePlanRuntimeConfig(cfg); err != nil {
		return nil, err
	}

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
	if err := validatePlanRuntimeConfig(cfg); err != nil {
		return nil, err
	}

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
	pg, err := newRuntimePostgres(cfg)
	if err != nil {
		_ = rt.Close()

		return nil, fmt.Errorf("agentos temporal plan runtime postgres: %w", err)
	}
	rt.postgres = pg
	planStore := temporalrepo.NewAgentOSPlanRepo(pg)
	rt.planEvents = planStore
	rt.planIndex = planStore
	rt.commandStore = planStore
	rt.auditStore = planStore

	blobStore, err := artifactrepo.NewBlobStore(ctx, artifactBlobConfig(cfg.ArtifactStore))
	if err != nil {
		_ = rt.Close()

		return nil, fmt.Errorf("agentos temporal plan runtime artifact store: %w", err)
	}
	rt.artifactStore = temporalrepo.NewAgentOSArtifactRepo(pg, blobStore)

	return rt, nil
}

func (r *planRuntime) StartPlan(ctx context.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.AccountID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: account id is required", agentos.ErrInvalidPlanScope)
	}
	if spec.ProjectID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: project id is required", agentos.ErrInvalidPlanScope)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	if r.planIndex == nil {
		return agentos.RunPlanStatus{}, errPlanRuntimePlanIndexRequired
	}
	if r.commandStore == nil {
		return agentos.RunPlanStatus{}, errPlanRuntimeCommandStoreRequired
	}
	if r.auditStore == nil {
		return agentos.RunPlanStatus{}, errPlanRuntimeAuditStoreRequired
	}

	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	status, created, err := r.planIndex.CreatePlan(ctx, spec, status)
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}
	if !created && status.LifecycleState != agentos.PlanLifecyclePending {
		return status, nil
	}
	record := planStartAuditRecord(spec)
	command, err := r.recordPlanCommand(ctx, planCommandFromAuditRecord(record))
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}
	if command.Status == agentosplan.PlanCommandDelivered {
		_, err := r.recordPlanAudit(ctx, record)

		return status, err
	}

	if err := r.executePlanWorkflow(ctx, spec); err != nil {
		markErr := r.markPlanCommandFailed(ctx, command, err)

		return agentos.RunPlanStatus{}, errors.Join(err, markErr)
	}
	if _, err := r.recordPlanAudit(ctx, record); err != nil {
		return agentos.RunPlanStatus{}, err
	}
	if err := r.markPlanCommandDelivered(ctx, command); err != nil {
		return agentos.RunPlanStatus{}, err
	}

	return status, nil
}

func (r *planRuntime) StatusPlan(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanStatus, error) {
	_, status, err := r.authorizePlan(ctx, ref)

	return status, err
}

func (r *planRuntime) DescribePlan(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanDescription, error) {
	spec, status, err := r.authorizePlan(ctx, ref)
	if err != nil {
		return agentos.RunPlanDescription{}, err
	}

	return agentosplan.DescribeRunPlan(spec, status)
}

func (r *planRuntime) authorizePlan(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, error) {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}
	if r.planIndex == nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, errPlanRuntimePlanIndexRequired
	}

	spec, status, exists, err := r.planIndex.GetPlanByRef(ctx, ref)
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}
	if !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, ref.PlanID)
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
	record := planSignalAuditRecord(ref, signal)
	command, err := r.recordPlanCommand(ctx, planCommandFromAuditRecord(record))
	if err != nil {
		return err
	}
	if command.Status == agentosplan.PlanCommandDelivered {
		_, err := r.recordPlanAudit(ctx, record)

		return err
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(ref.PlanID), "", PlanSignalName, signal); err != nil {
		markErr := r.markPlanCommandFailed(ctx, command, err)

		return errors.Join(fmt.Errorf("agentos temporal plan runtime - signal plan workflow: %w", err), markErr)
	}
	if _, err := r.recordPlanAudit(ctx, record); err != nil {
		return err
	}
	if err := r.markPlanCommandDelivered(ctx, command); err != nil {
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
	record := planControlAuditRecord(ref, control)
	command, err := r.recordPlanCommand(ctx, planCommandFromAuditRecord(record))
	if err != nil {
		return err
	}
	if command.Status == agentosplan.PlanCommandDelivered {
		_, err := r.recordPlanAudit(ctx, record)

		return err
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(ref.PlanID), "", PlanControlSignalName, control); err != nil {
		markErr := r.markPlanCommandFailed(ctx, command, err)

		return errors.Join(fmt.Errorf("agentos temporal plan runtime - control plan workflow: %w", err), markErr)
	}
	if _, err := r.recordPlanAudit(ctx, record); err != nil {
		return err
	}
	if err := r.markPlanCommandDelivered(ctx, command); err != nil {
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
	replaySequence := lastPlanEventSequence(scope.AfterSequence, events)
	liveScope.AfterSequence = replaySequence
	live, err := r.planLiveEvents.SubscribePlanEvents(ctx, liveScope)
	if err != nil {
		return nil, err
	}

	catchUpScope := scope
	catchUpScope.AfterSequence = replaySequence
	catchUpEvents, err := r.planEvents.ListPlanEvents(ctx, catchUpScope, 0)
	if err != nil {
		_ = live.Close()

		return nil, err
	}
	replayEvents := append(append([]agentos.PlanEvent(nil), events...), catchUpEvents...)
	liveAfterSequence := lastPlanEventSequence(replaySequence, catchUpEvents)

	return newPlanReplayThenLiveSubscriptionAfter(replayEvents, live, liveAfterSequence), nil
}

func (r *planRuntime) ListPlanEvents(ctx context.Context, scope agentos.PlanEventScope) ([]agentos.PlanEvent, error) {
	if r.planEvents == nil {
		return nil, errPlanRuntimePlanEventStoreRequired
	}
	if err := agentosplan.ValidatePlanEventScope(scope); err != nil {
		return nil, err
	}
	if _, _, err := r.authorizePlan(ctx, agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}); err != nil {
		return nil, err
	}

	return r.planEvents.ListPlanEvents(ctx, planEventStreamScope(scope), scope.Limit)
}

func (r *planRuntime) ListPlanDebugTraces(ctx context.Context, scope agentos.PlanDebugTraceScope) ([]agentos.PlanDebugTrace, error) {
	if r.planEvents == nil {
		return nil, errPlanRuntimePlanEventStoreRequired
	}
	if err := agentosplan.ValidatePlanDebugTraceScope(scope); err != nil {
		return nil, err
	}
	if _, _, err := r.authorizePlan(ctx, agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}); err != nil {
		return nil, err
	}

	events, err := r.planEvents.ListPlanEvents(ctx, planDebugTraceStreamScope(scope), 0)
	if err != nil {
		return nil, err
	}

	traces, err := agentosplan.BuildPlanDebugTraces(events)
	if err != nil {
		return nil, err
	}
	if scope.Limit > 0 && len(traces) > scope.Limit {
		traces = traces[:scope.Limit]
	}

	return traces, nil
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

	return r.artifactStore.List(ctx, scope)
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

	ref, payload, err := r.artifactStore.Get(ctx, scope)
	if err != nil {
		return agentos.Artifact{}, err
	}

	return agentos.Artifact{Ref: ref, Payload: payload}, nil
}

// RecoverPlanCommands redelivers recoverable RunPlan control-plane commands
// from the durable outbox.
func (r *planRuntime) RecoverPlanCommands(ctx context.Context, limit int) (PlanCommandRecoveryResult, error) {
	reconciler := newPlanCommandReconciler(r.temporalClient, r.taskQueue, r.commandStore, r.auditStore, r.planIndex)

	return reconciler.Recover(ctx, limit)
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

func (r *planRuntime) recordPlanCommand(ctx context.Context, command agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRecord, error) {
	if r.commandStore == nil {
		return agentosplan.PlanCommandRecord{}, errPlanRuntimeCommandStoreRequired
	}

	stored, _, err := r.commandStore.RecordPlanCommand(ctx, command)

	return stored, err
}

func (r *planRuntime) markPlanCommandDelivered(ctx context.Context, command agentosplan.PlanCommandRecord) error {
	if r.commandStore == nil {
		return errPlanRuntimeCommandStoreRequired
	}

	_, err := r.commandStore.MarkPlanCommandDelivered(ctx, agentosplan.PlanCommandRefFromRecord(command))

	return err
}

func (r *planRuntime) markPlanCommandFailed(ctx context.Context, command agentosplan.PlanCommandRecord, cause error) error {
	if r.commandStore == nil {
		return errPlanRuntimeCommandStoreRequired
	}

	_, err := r.commandStore.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(command), cause.Error())

	return err
}

func planWorkflowID(planID string) string {
	return "agentos-plan-" + planID
}

func (r *planRuntime) executePlanWorkflow(ctx context.Context, spec agentos.RunPlanSpec) error {
	return executePlanWorkflow(ctx, r.temporalClient, r.taskQueue, spec)
}

func executePlanWorkflow(ctx context.Context, temporalClient planTemporalClient, taskQueue string, spec agentos.RunPlanSpec) error {
	if temporalClient == nil {
		return errors.New("agentos temporal plan runtime: temporal client is not configured")
	}

	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        planWorkflowID(spec.PlanID),
		TaskQueue: taskQueue,
	}, PlanWorkflowName, planWorkflowInput{Spec: spec})
	if err != nil && !sdktemporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return fmt.Errorf("agentos temporal plan runtime - start plan workflow: %w", err)
	}

	return nil
}

func planStartAuditRecord(spec agentos.RunPlanSpec) agentosplan.AuditRecord {
	return agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Action:         agentosplan.AuditActionPlanStart,
		IdempotencyKey: spec.IdempotencyKey,
		Payload: map[string]any{
			"thread_id":  spec.ThreadID,
			"account_id": spec.AccountID,
			"project_id": spec.ProjectID,
		},
	}
}

func planSignalAuditRecord(ref agentos.PlanRef, signal agentos.Signal) agentosplan.AuditRecord {
	payload := map[string]any{
		planCommandPayloadSignalType: signal.Type,
		planCommandPayloadPayload:    signal.Payload,
	}
	if !signal.SentAt.IsZero() {
		payload[planCommandPayloadSentAt] = signal.SentAt
	}

	return agentosplan.AuditRecord{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		ActorID:        signal.ActorID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: signal.IdempotencyKey,
		Payload:        payload,
	}
}

func planCommandFromAuditRecord(record agentosplan.AuditRecord) agentosplan.PlanCommandRecord {
	return agentosplan.PlanCommandRecord{
		PlanID:         record.PlanID,
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		ActorID:        record.ActorID,
		Action:         record.Action,
		IdempotencyKey: record.IdempotencyKey,
		Payload:        record.Payload,
		Status:         agentosplan.PlanCommandPending,
	}
}

func planEventStreamScope(scope agentos.PlanEventScope) agentos.PlanStreamScope {
	return agentos.PlanStreamScope{
		PlanID:        scope.PlanID,
		AccountID:     scope.AccountID,
		ProjectID:     scope.ProjectID,
		NodeID:        scope.NodeID,
		RunID:         scope.RunID,
		AfterSequence: scope.AfterSequence,
	}
}

func planDebugTraceStreamScope(scope agentos.PlanDebugTraceScope) agentos.PlanStreamScope {
	return agentos.PlanStreamScope{
		PlanID:        scope.PlanID,
		AccountID:     scope.AccountID,
		ProjectID:     scope.ProjectID,
		NodeID:        scope.NodeID,
		RunID:         scope.RunID,
		AfterSequence: scope.AfterSequence,
	}
}

func validatePlanRuntimeConfig(cfg RuntimeConfig) error {
	if cfg.PostgresURL == "" {
		return ErrPlanRuntimePostgresURLRequired
	}

	return validateArtifactStoreConfig(cfg.ArtifactStore, artifactStoreValidationErrors{
		backendRequired:   ErrPlanRuntimeArtifactStoreBackendRequired,
		backendUnknown:    ErrPlanRuntimeArtifactStoreBackendUnknown,
		localRootRequired: ErrPlanRuntimeArtifactStoreLocalRootRequired,
		s3BucketRequired:  ErrPlanRuntimeArtifactStoreS3BucketRequired,
		s3RegionRequired:  ErrPlanRuntimeArtifactStoreS3RegionRequired,
		s3AccessRequired:  ErrPlanRuntimeArtifactStoreS3AccessKeyRequired,
		s3SecretRequired:  ErrPlanRuntimeArtifactStoreS3SecretKeyRequired,
	})
}

func newRuntimePostgres(cfg RuntimeConfig) (*postgres.Postgres, error) {
	if cfg.PostgresPoolMax > 0 {
		return postgres.New(cfg.PostgresURL, postgres.MaxPoolSize(cfg.PostgresPoolMax))
	}

	return postgres.New(cfg.PostgresURL)
}
