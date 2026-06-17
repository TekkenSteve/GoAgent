package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	goredis "github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	"go.temporal.io/sdk/client"
)

type planRuntime struct {
	temporalClient client.Client
	closeTemporal  bool
	redis          *goredis.Redis
	subscriber     *repostream.RedisSubscriber
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

	return rt, nil
}

func (r *planRuntime) StartPlan(ctx context.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	_, err := r.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        planWorkflowID(spec.PlanID),
		TaskQueue: r.taskQueue,
	}, PlanWorkflowName, spec)
	if err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("agentos temporal plan runtime - start plan workflow: %w", err)
	}

	return agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
	}, nil
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

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(planID), "", PlanSignalName, signal); err != nil {
		return fmt.Errorf("agentos temporal plan runtime - signal plan workflow: %w", err)
	}

	return nil
}

func (r *planRuntime) ControlPlan(ctx context.Context, planID string, op agentos.ControlOperation) error {
	if planID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	switch op {
	case agentos.ControlPause, agentos.ControlResume, agentos.ControlCancel:
	default:
		return fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
	}
	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(planID), "", PlanControlSignalName, op); err != nil {
		return fmt.Errorf("agentos temporal plan runtime - control plan workflow: %w", err)
	}

	return nil
}

func (r *planRuntime) SubscribePlan(ctx context.Context, scope agentos.PlanStreamScope) (agentos.Subscription, error) {
	if r.subscriber == nil {
		return nil, errors.New("agentos temporal plan runtime: redis subscriber is not configured")
	}
	if scope.PlanID == "" {
		return nil, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidStreamScope)
	}

	sub, err := r.subscriber.Subscribe(ctx, scope.PlanID, scope.AfterSequence)
	if err != nil {
		return nil, err
	}

	return newSubscription(sub), nil
}

func (r *planRuntime) Close() error {
	var errs []error
	if r.closeTemporal && r.temporalClient != nil {
		r.temporalClient.Close()
	}
	if r.redis != nil {
		errs = append(errs, r.redis.Close())
	}

	return errors.Join(errs...)
}

func planWorkflowID(planID string) string {
	return "agentos-plan-" + planID
}
