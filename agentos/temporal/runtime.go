package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwruntime "github.com/TekkenSteve/GoAgent/internal/agentfw/runtime"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	goredis "github.com/TekkenSteve/GoAgent/pkg/redis"
	"go.temporal.io/sdk/client"
)

type runtime struct {
	temporalRuntime *agentfwruntime.TemporalRuntime
	executor        *temporalrepo.ExecutorTemporal
	subscriber      *repostream.RedisSubscriber
	redis           *goredis.Redis
}

// NewRuntime creates the default Temporal/Redis implementation of agentos.Runtime.
func NewRuntime(ctx context.Context, cfg RuntimeConfig) (agentos.Runtime, error) {
	fwTemporal := temporalConfig(cfg)

	rt, err := agentfwruntime.NewTemporalRuntime(fwTemporal)
	if err != nil {
		return nil, fmt.Errorf("agentos temporal runtime: %w", err)
	}

	r := &runtime{
		temporalRuntime: rt,
		executor:        temporalrepo.NewExecutorTemporal(rt.Client, fwTemporal),
	}

	if cfg.RedisURL != "" {
		rdb, err := goredis.New(ctx, cfg.RedisURL)
		if err != nil {
			rt.Close()

			return nil, fmt.Errorf("agentos temporal runtime redis: %w", err)
		}
		r.redis = rdb
		r.subscriber = repostream.NewRedisSubscriber(rdb.Hub())
	}

	return r, nil
}

// NewRuntimeWithClient adapts an existing Temporal client to agentos.Runtime.
// Hosts that already own worker/client lifecycle can use this without opening
// another Temporal connection.
func NewRuntimeWithClient(ctx context.Context, cfg RuntimeConfig, c client.Client) (agentos.Runtime, error) {
	fwTemporal := temporalConfig(cfg)
	r := &runtime{
		executor: temporalrepo.NewExecutorTemporal(c, fwTemporal),
	}

	if cfg.RedisURL != "" {
		rdb, err := goredis.New(ctx, cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("agentos temporal runtime redis: %w", err)
		}
		r.redis = rdb
		r.subscriber = repostream.NewRedisSubscriber(rdb.Hub())
	}

	return r, nil
}

func (r *runtime) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	req, err := executionRequestFromRunSpec(spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := r.executor.StartExecution(ctx, req)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return runStatusFromEntity(status), nil
}

func (r *runtime) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	status, err := r.executor.GetStatus(ctx, runID)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return runStatusFromEntity(status), nil
}

func (r *runtime) Control(ctx context.Context, runID string, op agentos.ControlOperation) error {
	internalOp, err := controlOperationToEntity(op)
	if err != nil {
		return err
	}

	switch internalOp {
	case entity.ControlPause:
		return r.executor.Pause(ctx, runID)
	case entity.ControlResume:
		return r.executor.Resume(ctx, runID)
	case entity.ControlCancel:
		return r.executor.Cancel(ctx, runID)
	default:
		return fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
	}
}

func (r *runtime) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	if r.subscriber == nil {
		return nil, errors.New("agentos temporal runtime: redis subscriber is not configured")
	}

	sessionID := scope.ThreadID
	if sessionID == "" {
		sessionID = scope.RunID
	}
	if sessionID == "" {
		return nil, fmt.Errorf("%w: run id or thread id is required", agentos.ErrInvalidStreamScope)
	}

	sub, err := r.subscriber.Subscribe(ctx, sessionID, scope.AfterSequence)
	if err != nil {
		return nil, err
	}

	return newSubscription(sub), nil
}

func (r *runtime) Close() error {
	var errs []error
	if r.temporalRuntime != nil {
		r.temporalRuntime.Close()
	}
	if r.redis != nil {
		errs = append(errs, r.redis.Close())
	}

	return errors.Join(errs...)
}
