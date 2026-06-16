package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwbackend "github.com/TekkenSteve/GoAgent/internal/agentfw/backend"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/backend/temporalexternal"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	goredis "github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	"go.temporal.io/sdk/client"
)

type runtime struct {
	temporalClient client.Client
	closeTemporal  bool
	router         *agentfwbackend.Router
	redis          *goredis.Redis
}

// NewRuntime creates the default Temporal/Redis implementation of agentos.Runtime.
func NewRuntime(ctx context.Context, cfg RuntimeConfig) (agentos.Runtime, error) {
	fwTemporal := temporalConfig(cfg)

	c, err := client.Dial(client.Options{
		HostPort:  fwTemporal.Address,
		Namespace: fwTemporal.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("agentos temporal runtime client: %w", err)
	}

	r := &runtime{
		temporalClient: c,
		closeTemporal:  true,
	}

	var subscriber *repostream.RedisSubscriber
	if cfg.RedisURL != "" {
		rdb, err := goredis.New(ctx, cfg.RedisURL)
		if err != nil {
			c.Close()

			return nil, fmt.Errorf("agentos temporal runtime redis: %w", err)
		}
		r.redis = rdb
		subscriber = repostream.NewRedisSubscriber(rdb.Hub())
	}

	if err := r.configureRouter(c, cfg, temporalrepo.NewExecutorTemporal(c, fwTemporal), subscriber); err != nil {
		c.Close()
		if r.redis != nil {
			_ = r.redis.Close()
		}

		return nil, err
	}

	return r, nil
}

// NewRuntimeWithClient adapts an existing Temporal client to agentos.Runtime.
// Hosts that already own worker/client lifecycle can use this without opening
// another Temporal connection.
func NewRuntimeWithClient(ctx context.Context, cfg RuntimeConfig, c client.Client) (agentos.Runtime, error) {
	if c == nil {
		return nil, errors.New("agentos temporal runtime: nil temporal client")
	}

	fwTemporal := temporalConfig(cfg)
	r := &runtime{
		temporalClient: c,
	}

	var subscriber *repostream.RedisSubscriber
	if cfg.RedisURL != "" {
		rdb, err := goredis.New(ctx, cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("agentos temporal runtime redis: %w", err)
		}
		r.redis = rdb
		subscriber = repostream.NewRedisSubscriber(rdb.Hub())
	}

	if err := r.configureRouter(c, cfg, temporalrepo.NewExecutorTemporal(c, fwTemporal), subscriber); err != nil {
		if r.redis != nil {
			_ = r.redis.Close()
		}

		return nil, err
	}

	return r, nil
}

func (r *runtime) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	return r.router.Start(ctx, spec)
}

func (r *runtime) Signal(ctx context.Context, runID string, signal agentos.Signal) error {
	return r.router.Signal(ctx, runID, signal)
}

func (r *runtime) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	return r.router.Status(ctx, runID)
}

func (r *runtime) Control(ctx context.Context, runID string, op agentos.ControlOperation) error {
	return r.router.Control(ctx, runID, op)
}

func (r *runtime) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	return r.router.Subscribe(ctx, scope)
}

func (r *runtime) configureRouter(temporalClient client.Client, cfg RuntimeConfig, executor *temporalrepo.ExecutorTemporal, subscriber *repostream.RedisSubscriber) error {
	registry := agentfwbackend.NewRegistry()
	agentosSubscriber := newAgentOSSubscriber(subscriber)
	native := newTemporalNativeBackend(executor, agentosSubscriber)
	if err := registry.Register(agentos.BackendRef{
		Kind: agentos.BackendKindTemporalNative,
		Name: agentos.BackendNameGoAgentNative,
	}, native); err != nil {
		return err
	}
	if err := registry.Register(agentos.BackendRef{
		Kind: agentos.BackendKindNative,
		Name: agentos.BackendNameGoAgentNative,
	}, native); err != nil {
		return err
	}
	for _, backendConfig := range cfg.TemporalExternalBackends {
		internalConfig := temporalExternalConfig(backendConfig)
		external, err := temporalexternal.NewBackend(temporalClient, agentosSubscriber, internalConfig)
		if err != nil {
			return err
		}
		if err := registry.Register(internalConfig.Ref(), external); err != nil {
			return err
		}
	}

	router, err := agentfwbackend.NewRouter(registry, agentfwbackend.NewMemoryRunBackendIndex())
	if err != nil {
		return err
	}
	r.router = router

	return nil
}

func temporalExternalConfig(cfg ExternalBackendConfig) temporalexternal.Config {
	return temporalexternal.Config{
		Name:         cfg.Name,
		TaskQueue:    cfg.TaskQueue,
		WorkflowType: cfg.WorkflowType,
		QueryType:    cfg.QueryType,
		Signals: temporalexternal.SignalNames{
			Pause:    cfg.Signals.Pause,
			Resume:   cfg.Signals.Resume,
			Cancel:   cfg.Signals.Cancel,
			Defaults: cfg.Signals.Defaults,
		},
	}
}

func (r *runtime) Close() error {
	var errs []error
	if r.closeTemporal && r.temporalClient != nil {
		r.temporalClient.Close()
	}
	if r.redis != nil {
		errs = append(errs, r.redis.Close())
	}

	return errors.Join(errs...)
}

type temporalNativeBackend struct {
	executor   *temporalrepo.ExecutorTemporal
	subscriber agentfwbackend.EventSubscriber
}

func newTemporalNativeBackend(executor *temporalrepo.ExecutorTemporal, subscriber agentfwbackend.EventSubscriber) *temporalNativeBackend {
	return &temporalNativeBackend{
		executor:   executor,
		subscriber: subscriber,
	}
}

func (b *temporalNativeBackend) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	req, err := executionRequestFromRunSpec(spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := b.executor.StartExecution(ctx, req)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return runStatusFromEntity(status), nil
}

func (b *temporalNativeBackend) Signal(ctx context.Context, runID string, signal agentos.Signal) error {
	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentos.ErrInvalidSignal)
	}

	switch signal.Type {
	case agentos.SignalControlPause:
		return b.Control(ctx, runID, agentos.ControlPause)
	case agentos.SignalControlResume:
		return b.Control(ctx, runID, agentos.ControlResume)
	case agentos.SignalControlCancel:
		return b.Control(ctx, runID, agentos.ControlCancel)
	default:
		return fmt.Errorf("%w: unsupported by temporal native backend: %s", agentos.ErrInvalidSignal, signal.Type)
	}
}

func (b *temporalNativeBackend) Control(ctx context.Context, runID string, op agentos.ControlOperation) error {
	internalOp, err := controlOperationToEntity(op)
	if err != nil {
		return err
	}

	switch internalOp {
	case entity.ControlPause:
		return b.executor.Pause(ctx, runID)
	case entity.ControlResume:
		return b.executor.Resume(ctx, runID)
	case entity.ControlCancel:
		return b.executor.Cancel(ctx, runID)
	default:
		return fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
	}
}

func (b *temporalNativeBackend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	status, err := b.executor.GetStatus(ctx, runID)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return runStatusFromEntity(status), nil
}

func (b *temporalNativeBackend) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	if b.subscriber == nil {
		return nil, errors.New("agentos temporal native backend: redis subscriber is not configured")
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

func (b *temporalNativeBackend) Capabilities() agentfwbackend.BackendCapabilities {
	return agentfwbackend.BackendCapabilities{
		SupportsSignal:    true,
		SupportsPause:     true,
		SupportsResume:    true,
		SupportsCancel:    true,
		SupportsStreaming: true,
	}
}
