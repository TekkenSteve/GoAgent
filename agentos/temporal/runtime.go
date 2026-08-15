package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/grpcbackend"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/httpbackend"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/temporalexternal"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"go.temporal.io/sdk/client"
)

type runtime struct {
	temporalClient client.Client
	closeTemporal  bool
	router         *agentosruntime.Router
	closers        []func() error
}

type runtimeRunBackendIndexFactory func(cfg *RuntimeConfig) (RunBackendIndex, func() error, error)

var (
	// ErrRuntimePostgresURLRequired reports a missing Postgres URL in the runtime config.
	ErrRuntimePostgresURLRequired = errors.New("agentos temporal runtime: postgres url is required")

	errRuntimeConfigRequired            = errors.New("agentos temporal runtime: config is required")
	errRuntimeNilTemporalClient         = errors.New("agentos temporal runtime: nil temporal client")
	errRuntimeNotConfigured             = errors.New("agentos temporal runtime: runtime is not configured")
	errRuntimeRunBackendIndexRequired   = errors.New("agentos temporal runtime: run backend index is required")
	errRuntimeNativeBackendNoSubscriber = errors.New("agentos temporal native backend: stream subscriber is not configured")
)

func runtimeRunBackendIndexFromPostgres(cfg *RuntimeConfig) (backend RunBackendIndex, cleanup func() error, err error) {
	if cfg.PostgresURL == "" {
		return nil, nil, ErrRuntimePostgresURLRequired
	}

	pg, err := newRuntimePostgres(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("agentos temporal runtime postgres: %w", err)
	}

	return temporalrepo.NewRunBackendIndexRepo(pg), func() error {
		pg.Close()

		return nil
	}, nil
}

// NewRuntime creates the default Temporal implementation of agentos.Runtime.
// The ctx argument is retained for API compatibility; runtime setup dials
// Temporal without a caller context.
func NewRuntime(_ context.Context, cfg *RuntimeConfig, options ...RuntimeOption) (agentos.Runtime, error) {
	if cfg == nil {
		return nil, errRuntimeConfigRequired
	}

	fwTemporal := temporalConfig(cfg)
	if err := fwTemporal.TaskQueues.Validate(); err != nil {
		return nil, err
	}

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

	subscriber := cfg.Subscriber

	runtimeOpts, err := r.runtimeOptionsWithDefaultRunBackendIndex(cfg, buildRuntimeOptions(options))
	if err != nil {
		return nil, errors.Join(err, r.Close())
	}

	executor, err := temporalrepo.NewExecutorTemporal(c, &fwTemporal)
	if err != nil {
		return nil, errors.Join(err, r.Close())
	}

	if err := r.configureRouter(c, cfg, runtimeOpts, executor, subscriber); err != nil {
		return nil, errors.Join(err, r.Close())
	}

	return r, nil
}

// NewRuntimeWithClient adapts an existing Temporal client to agentos.Runtime.
// Hosts that already own worker/client lifecycle can use this without opening
// another Temporal connection.
func NewRuntimeWithClient(_ context.Context, cfg *RuntimeConfig, c client.Client, options ...RuntimeOption) (agentos.Runtime, error) {
	if cfg == nil {
		return nil, errRuntimeConfigRequired
	}

	if c == nil {
		return nil, errRuntimeNilTemporalClient
	}

	fwTemporal := temporalConfig(cfg)
	if err := fwTemporal.TaskQueues.Validate(); err != nil {
		return nil, err
	}

	r := &runtime{
		temporalClient: c,
	}

	subscriber := cfg.Subscriber

	runtimeOpts, err := r.runtimeOptionsWithDefaultRunBackendIndex(cfg, buildRuntimeOptions(options))
	if err != nil {
		return nil, errors.Join(err, r.Close())
	}

	executor, err := temporalrepo.NewExecutorTemporal(c, &fwTemporal)
	if err != nil {
		return nil, errors.Join(err, r.Close())
	}

	if err := r.configureRouter(c, cfg, runtimeOpts, executor, subscriber); err != nil {
		return nil, errors.Join(err, r.Close())
	}

	return r, nil
}

func (r *runtime) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	if r == nil || r.router == nil {
		return agentos.RunStatus{}, errRuntimeNotConfigured
	}

	return r.router.Start(ctx, spec)
}

func (r *runtime) StartPlanNode(ctx context.Context, planID, nodeID string, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	if r == nil || r.router == nil {
		return agentos.RunStatus{}, errRuntimeNotConfigured
	}

	return r.router.StartPlanNode(ctx, planID, nodeID, spec)
}

func (r *runtime) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error {
	if r == nil || r.router == nil {
		return errRuntimeNotConfigured
	}

	return r.router.Signal(ctx, runID, signal)
}

func (r *runtime) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	if r == nil || r.router == nil {
		return agentos.RunStatus{}, errRuntimeNotConfigured
	}

	return r.router.Status(ctx, runID)
}

func (r *runtime) Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) error {
	if r == nil || r.router == nil {
		return errRuntimeNotConfigured
	}

	return r.router.Control(ctx, runID, control)
}

func (r *runtime) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	if r == nil || r.router == nil {
		return nil, errRuntimeNotConfigured
	}

	return r.router.Subscribe(ctx, scope)
}

func (r *runtime) configureRouter(temporalClient client.Client, cfg *RuntimeConfig, opts runtimeOptions, executor *temporalrepo.ExecutorTemporal, subscriber agentosstream.Subscriber) error {
	registry := agentosruntime.NewRegistry()
	agentosSubscriber := newAgentOSSubscriber(subscriber)

	lifecycle := streamadapter.NewRunLifecycle(cfg.Publisher, cfg.Logger)
	if cfg.ProjectionController != nil {
		lifecycle = lifecycle.WithEnsure(cfg.ProjectionController.EnsureSubscribed)
	}

	native := newTemporalNativeBackend(executor, agentosSubscriber)
	if err := registry.Register(agentos.BackendRef{
		Kind: agentos.BackendKindNative,
		Name: agentos.BackendNameGoAgentNative,
	}, native); err != nil {
		return err
	}

	closers, err := registerExternalBackends(registry, temporalClient, agentosSubscriber, lifecycle, cfg)
	if err != nil {
		return err
	}

	r.closers = append(r.closers, closers...)

	if opts.runBackendIndex == nil {
		return errRuntimeRunBackendIndexRequired
	}

	router, err := agentosruntime.NewRouter(registry, opts.runBackendIndex)
	if err != nil {
		return err
	}

	if opts.backendSelector != nil {
		router.WithBackendSelector(opts.backendSelector)
	}

	r.router = router

	return nil
}

func registerExternalBackends(registry *agentosruntime.Registry, temporalClient client.Client, agentosSubscriber *agentOSSubscriber, lifecycle agentosruntime.LifecyclePublisher, cfg *RuntimeConfig) ([]func() error, error) {
	var closers []func() error

	for i := range cfg.TemporalExternalBackends {
		internalConfig := temporalExternalConfig(&cfg.TemporalExternalBackends[i])

		external, err := temporalexternal.NewBackend(temporalexternal.NewTemporalClient(temporalClient), agentosSubscriber, lifecycle, &internalConfig)
		if err != nil {
			return nil, err
		}

		if err := registry.Register(internalConfig.Ref(), external); err != nil {
			return nil, err
		}
	}

	for i := range cfg.HTTPBackends {
		internalConfig := httpBackendConfig(&cfg.HTTPBackends[i])

		httpBackend, err := httpbackend.NewBackend(nil, agentosSubscriber, lifecycle, internalConfig)
		if err != nil {
			return nil, err
		}

		if err := registry.Register(internalConfig.Ref(), httpBackend); err != nil {
			return nil, err
		}
	}

	for i := range cfg.GRPCBackends {
		internalConfig := grpcBackendConfig(&cfg.GRPCBackends[i])

		grpcBackend, err := grpcbackend.NewBackend(agentosSubscriber, lifecycle, &internalConfig)
		if err != nil {
			return nil, err
		}

		if err := registry.Register(internalConfig.Ref(), grpcBackend); err != nil {
			return nil, errors.Join(err, grpcBackend.Close())
		}

		closers = append(closers, grpcBackend.Close)
	}

	return closers, nil
}

func (r *runtime) runtimeOptionsWithDefaultRunBackendIndex(cfg *RuntimeConfig, opts runtimeOptions) (runtimeOptions, error) {
	if opts.runBackendIndex != nil {
		return opts, nil
	}

	index, closeFn, err := opts.runBackendIndexFactory(cfg)
	if err != nil {
		return runtimeOptions{}, err
	}

	if index == nil {
		return opts, nil
	}

	opts.runBackendIndex = index

	if closeFn != nil {
		r.closers = append(r.closers, closeFn)
	}

	return opts, nil
}

func buildRuntimeOptions(options []RuntimeOption) runtimeOptions {
	opts := runtimeOptions{
		runBackendIndexFactory: runtimeRunBackendIndexFromPostgres,
	}

	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}

	return opts
}

func httpBackendConfig(cfg *HTTPBackendConfig) httpbackend.Config {
	return httpbackend.Config{
		Name:     cfg.Name,
		Endpoint: cfg.Endpoint,
		Headers:  cfg.Headers,
	}
}

func grpcBackendConfig(cfg *GRPCBackendConfig) grpcbackend.Config {
	return grpcbackend.Config{
		Name:      cfg.Name,
		Target:    cfg.Target,
		Authority: cfg.Authority,
		Insecure:  cfg.Insecure,
		Service:   cfg.Service,
		Methods: grpcbackend.MethodNames{
			Start:   cfg.Methods.Start,
			Signal:  cfg.Methods.Signal,
			Control: cfg.Methods.Control,
			Status:  cfg.Methods.Status,
		},
	}
}

func temporalExternalConfig(cfg *ExternalBackendConfig) temporalexternal.Config {
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

	for _, closeFn := range r.closers {
		errs = append(errs, closeFn())
	}

	return errors.Join(errs...)
}

type temporalNativeBackend struct {
	executor   nativeExecutor
	subscriber agentosruntime.EventSubscriber
}

type nativeExecutor interface {
	StartExecution(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error)
	GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
	Pause(ctx context.Context, runID string) error
	Resume(ctx context.Context, runID string) error
	Cancel(ctx context.Context, runID string) error
	SignalUserMessage(ctx context.Context, runID string, message *orchestration.UserMessageSignal) error
}

func newTemporalNativeBackend(executor nativeExecutor, subscriber agentosruntime.EventSubscriber) *temporalNativeBackend {
	return &temporalNativeBackend{
		executor:   executor,
		subscriber: subscriber,
	}
}

func (b *temporalNativeBackend) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	req, err := executionRequestFromRunSpec(spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := b.executor.StartExecution(ctx, req)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return runStatusFromEntity(&status), nil
}

func (b *temporalNativeBackend) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error {
	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentoscore.ErrInvalidSignal)
	}

	switch signal.Type {
	case agentoscore.SignalControlPause:
		control := controlRequestFromSignal(agentoscore.ControlPause, signal)

		return b.Control(ctx, runID, &control)
	case agentoscore.SignalControlResume:
		control := controlRequestFromSignal(agentoscore.ControlResume, signal)

		return b.Control(ctx, runID, &control)
	case agentoscore.SignalControlCancel:
		control := controlRequestFromSignal(agentoscore.ControlCancel, signal)

		return b.Control(ctx, runID, &control)
	case agentoscore.SignalUserMessage:
		message, err := userMessageSignalToNative(signal)
		if err != nil {
			return err
		}

		return b.executor.SignalUserMessage(ctx, runID, &message)
	case agentoscore.SignalPlanNodeRetry, agentoscore.SignalPlanApprove, agentoscore.SignalPlanReject,
		agentoscore.SignalUserApproval, agentoscore.SignalUserReject,
		agentoscore.SignalToolResult, agentoscore.SignalHumanFeedback, agentoscore.SignalConfigPatch,
		agentoscore.SignalMemoryPatch:
		return fmt.Errorf("%w: unsupported by temporal native backend: %s", agentoscore.ErrInvalidSignal, signal.Type)
	default:
		return fmt.Errorf("%w: unsupported by temporal native backend: %s", agentoscore.ErrInvalidSignal, signal.Type)
	}
}

func (b *temporalNativeBackend) Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) error {
	if err := agentoscore.ValidateControlRequest(control); err != nil {
		return err
	}

	internalOp, err := controlOperationToEntity(control.Operation)
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
		return fmt.Errorf("%w: %s", agentoscore.ErrInvalidControlOperation, control.Operation)
	}
}

func (b *temporalNativeBackend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	status, err := b.executor.GetStatus(ctx, runID)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return runStatusFromEntity(&status), nil
}

func (b *temporalNativeBackend) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	if b.subscriber == nil {
		return nil, errRuntimeNativeBackendNoSubscriber
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

func (b *temporalNativeBackend) Capabilities() agentosruntime.BackendCapabilities {
	return agentosruntime.BackendCapabilities{
		SupportsSignal:            true,
		SupportsSignalUserMessage: true,
		SupportsPause:             true,
		SupportsResume:            true,
		SupportsCancel:            true,
		SupportsStreaming:         true,
	}
}

func controlRequestFromSignal(operation agentoscore.ControlOperation, signal *agentoscore.Signal) agentoscore.ControlRequest {
	return agentoscore.ControlRequest{
		Operation:      operation,
		IdempotencyKey: signal.IdempotencyKey,
		RequestedAt:    signal.SentAt,
	}
}

func userMessageSignalToNative(signal *agentoscore.Signal) (orchestration.UserMessageSignal, error) {
	content, ok := signal.Payload["content"].(string)
	if !ok || content == "" {
		return orchestration.UserMessageSignal{}, fmt.Errorf("%w: payload.content is required", agentoscore.ErrInvalidSignal)
	}

	sentAt := signal.SentAt
	if sentAt.IsZero() {
		sentAt = time.Now().UTC()
	}

	messageID, ok := signal.Payload["message_id"].(string)
	if !ok {
		messageID = ""
	}

	return orchestration.UserMessageSignal{
		MessageID:      messageID,
		IdempotencyKey: signal.IdempotencyKey,
		Content:        content,
		Attachments:    payloadMapSlice(signal.Payload, "attachments"),
		Context:        payloadMap(signal.Payload, "context"),
		ReceivedAtUnix: sentAt.Unix(),
	}, nil
}

func payloadMap(payload map[string]any, key string) map[string]any {
	value, ok := payload[key].(map[string]any)
	if !ok {
		return nil
	}

	return value
}

func payloadMapSlice(payload map[string]any, key string) []map[string]any {
	rawItems, ok := payload[key].([]any)
	if !ok {
		return nil
	}

	items := make([]map[string]any, 0, len(rawItems))
	for _, rawItem := range rawItems {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return nil
		}

		items = append(items, item)
	}

	return items
}
