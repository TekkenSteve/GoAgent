package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	goredis "github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
)

type planRuntime struct {
	temporalClient client.Client
	closeTemporal  bool
	redis          *goredis.Redis
	subscriber     *repostream.RedisSubscriber
	postgres       *postgres.Postgres
	planEvents     agentosplan.PlanEventStore
	planIndex      agentosplan.PlanIndex
	auditStore     agentosplan.AuditStore
	taskQueue      string
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

func newPlanRuntimeWithClient(ctx context.Context, cfg RuntimeConfig, c client.Client, closeTemporal bool) (*planRuntime, error) {
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
		rt.subscriber = repostream.NewRedisSubscriber(rdb.Hub())
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
	}

	return rt, nil
}

func (r *planRuntime) StartPlan(ctx context.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	if r.planIndex == nil {
		return agentos.RunPlanStatus{}, errors.New("agentos temporal plan runtime: plan index is not configured")
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
	}, PlanWorkflowName, spec)
	if err != nil && !sdktemporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return agentos.RunPlanStatus{}, fmt.Errorf("agentos temporal plan runtime - start plan workflow: %w", err)
	}

	status.LifecycleState = agentos.PlanLifecycleRunning
	status.UpdatedAt = time.Now().UTC()

	return status, nil
}

func (r *planRuntime) StatusPlan(ctx context.Context, planID string) (agentos.RunPlanStatus, error) {
	if planID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	value, err := r.temporalClient.QueryWorkflow(ctx, planWorkflowID(planID), "", PlanStatusQueryName)
	if err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("agentos temporal plan runtime - query plan workflow: %w", err)
	}

	var status agentos.RunPlanStatus
	if err := value.Get(&status); err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("agentos temporal plan runtime - decode status: %w", err)
	}

	return status, nil
}

func (r *planRuntime) SignalPlan(ctx context.Context, planID string, signal agentos.Signal) error {
	if planID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentos.ErrInvalidSignal)
	}
	if signal.IdempotencyKey == "" {
		return fmt.Errorf("%w: signal idempotency key is required", agentos.ErrInvalidSignal)
	}
	created, err := r.recordPlanAudit(ctx, agentosplan.AuditRecord{
		PlanID:         planID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: signal.IdempotencyKey,
		Payload: map[string]any{
			"type": signal.Type,
		},
	})
	if err != nil {
		return err
	}
	if !created {
		return nil
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(planID), "", PlanSignalName, signal); err != nil {
		return fmt.Errorf("agentos temporal plan runtime - signal plan workflow: %w", err)
	}

	return nil
}

func (r *planRuntime) ControlPlan(ctx context.Context, planID string, control agentos.ControlRequest) error {
	if planID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}
	if control.IdempotencyKey == "" {
		return fmt.Errorf("%w: control idempotency key is required", agentos.ErrInvalidControlOperation)
	}
	created, err := r.recordPlanAudit(ctx, agentosplan.AuditRecord{
		PlanID:         planID,
		ActorID:        control.ActorID,
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: control.IdempotencyKey,
		Payload: map[string]any{
			"operation": control.Operation,
			"metadata":  control.Metadata,
		},
	})
	if err != nil {
		return err
	}
	if !created {
		return nil
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(planID), "", PlanControlSignalName, control); err != nil {
		return fmt.Errorf("agentos temporal plan runtime - control plan workflow: %w", err)
	}

	return nil
}

func (r *planRuntime) SubscribePlan(ctx context.Context, scope agentos.PlanStreamScope) (agentos.Subscription, error) {
	if r.planEvents == nil {
		return nil, errors.New("agentos temporal plan runtime: plan event store is not configured")
	}
	if scope.PlanID == "" {
		return nil, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidStreamScope)
	}

	events, err := r.planEvents.ListPlanEvents(ctx, scope, 0)
	if err != nil {
		return nil, err
	}

	return newPlanReplaySubscription(events), nil
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
		return false, errors.New("agentos temporal plan runtime: audit store is not configured")
	}
	_, created, err := r.auditStore.RecordAudit(ctx, record)

	return created, err
}

func planWorkflowID(planID string) string {
	return "agentos-plan-" + planID
}

func newRuntimePostgres(cfg RuntimeConfig) (*postgres.Postgres, error) {
	if cfg.PostgresPoolMax > 0 {
		return postgres.New(cfg.PostgresURL, postgres.MaxPoolSize(cfg.PostgresPoolMax))
	}

	return postgres.New(cfg.PostgresURL)
}
