package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/dshbackend"
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
	// backends is the supervised view the router resolves runs through;
	// rawBackends keeps the undecorated backends the run supervisor activities
	// use, so applying a control cannot route back into the supervisor that
	// issued it.
	backends    *agentosruntime.Registry
	rawBackends *agentosruntime.Registry
	closers     []func() error
}

// BackendResolver resolves a backend ref to its undecorated implementation.
// Run supervisor activities use it to reach the backend directly.
type BackendResolver interface {
	ResolveBackend(ref agentos.BackendRef) (agentosruntime.AgentBackend, error)
}

// ResolveBackend returns the undecorated backend registered for ref.
func (r *runtime) ResolveBackend(ref agentos.BackendRef) (agentosruntime.AgentBackend, error) {
	if r == nil || r.rawBackends == nil {
		return nil, errRuntimeNotConfigured
	}

	return r.rawBackends.Get(ref)
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

func (r *runtime) Signal(ctx context.Context, ref agentos.RunRef, signal *agentoscore.Signal) error {
	if r == nil || r.router == nil {
		return errRuntimeNotConfigured
	}

	return r.router.Signal(ctx, ref, signal)
}

func (r *runtime) Status(ctx context.Context, ref agentos.RunRef) (agentos.RunStatus, error) {
	if r == nil || r.router == nil {
		return agentos.RunStatus{}, errRuntimeNotConfigured
	}

	return r.router.Status(ctx, ref)
}

func (r *runtime) Control(ctx context.Context, ref agentos.RunRef, control *agentoscore.ControlRequest) error {
	if r == nil || r.router == nil {
		return errRuntimeNotConfigured
	}

	return r.router.Control(ctx, ref, control)
}

func (r *runtime) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	if r == nil || r.router == nil {
		return nil, errRuntimeNotConfigured
	}

	return r.router.Subscribe(ctx, scope)
}

func (r *runtime) configureRouter(temporalClient client.Client, cfg *RuntimeConfig, opts runtimeOptions, executor *temporalrepo.ExecutorTemporal, subscriber agentosstream.Subscriber) error {
	registry := agentosruntime.NewRegistry()
	rawRegistry := agentosruntime.NewRegistry()
	r.backends = registry
	r.rawBackends = rawRegistry

	supervisor, err := NewSupervisorClient(temporalClient, temporalConfig(cfg).TaskQueues.ProcessControl)
	if err != nil {
		return err
	}

	backends := supervisedRegistrar{raw: rawRegistry, supervised: registry, supervisor: supervisor}
	agentosSubscriber := newAgentOSSubscriber(subscriber)

	lifecycle := streamadapter.NewRunLifecycle(cfg.Publisher, cfg.Logger)
	if cfg.Facts != nil {
		lifecycle = lifecycle.WithFacts(cfg.Facts)
	}

	native := newTemporalNativeBackend(executor, agentosSubscriber, lifecycle)
	if err := (supervisedRegistrar{raw: rawRegistry, supervised: registry}).registerPlain(agentos.BackendRef{
		Kind: agentos.BackendKindNative,
		Name: agentos.BackendNameGoAgentNative,
	}, native); err != nil {
		return err
	}

	closers, err := registerExternalBackends(backends, temporalClient, agentosSubscriber, lifecycle, cfg)
	if err != nil {
		return err
	}

	r.closers = append(r.closers, closers...)

	dshClosers, err := registerDSHBackends(backends, agentosSubscriber, cfg)
	if err != nil {
		return err
	}

	r.closers = append(r.closers, dshClosers...)

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

// supervisedRegistrar registers each backend twice: undecorated for the run
// supervisor activities, and wrapped for the router. Backends whose runs are
// already Temporal workflows (native, temporal_external) keep the same value in
// both views — their control path is Temporal primitives already.
type supervisedRegistrar struct {
	raw        *agentosruntime.Registry
	supervised *agentosruntime.Registry
	supervisor RunSupervisor
}

// registerPlain registers one backend in both views unchanged: its control path
// is already served by Temporal primitives.
func (r supervisedRegistrar) registerPlain(ref agentos.BackendRef, backend agentosruntime.AgentBackend) error {
	if err := r.raw.Register(ref, backend); err != nil {
		return err
	}

	return r.supervised.Register(ref, backend)
}

func (r supervisedRegistrar) register(ref agentos.BackendRef, backend agentosruntime.AgentBackend) error {
	if err := r.raw.Register(ref, backend); err != nil {
		return err
	}

	if r.supervisor == nil {
		return r.supervised.Register(ref, backend)
	}

	wrapped, err := NewSupervisedBackend(ref, backend, r.supervisor)
	if err != nil {
		return err
	}

	return r.supervised.Register(ref, wrapped)
}

func registerExternalBackends(backends supervisedRegistrar, temporalClient client.Client, agentosSubscriber *agentOSSubscriber, lifecycle agentosruntime.LifecyclePublisher, cfg *RuntimeConfig) ([]func() error, error) {
	var closers []func() error

	for i := range cfg.TemporalExternalBackends {
		internalConfig := temporalExternalConfig(&cfg.TemporalExternalBackends[i])

		external, err := temporalexternal.NewBackend(temporalexternal.NewTemporalClient(temporalClient), agentosSubscriber, lifecycle, &internalConfig)
		if err != nil {
			return nil, err
		}

		if err := backends.registerPlain(internalConfig.Ref(), external); err != nil {
			return nil, err
		}
	}

	for i := range cfg.HTTPBackends {
		internalConfig := httpBackendConfig(&cfg.HTTPBackends[i])

		httpBackend, err := httpbackend.NewBackend(nil, agentosSubscriber, lifecycle, internalConfig)
		if err != nil {
			return nil, err
		}

		if err := backends.register(internalConfig.Ref(), httpBackend); err != nil {
			return nil, err
		}
	}

	for i := range cfg.GRPCBackends {
		internalConfig := grpcBackendConfig(&cfg.GRPCBackends[i])

		grpcBackend, err := grpcbackend.NewBackend(agentosSubscriber, lifecycle, &internalConfig)
		if err != nil {
			return nil, err
		}

		if err := backends.register(internalConfig.Ref(), grpcBackend); err != nil {
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

// registerDSHBackends wires configured DeepSeek Harness SDK backends into the
// registry. Unlike the external pollers, each dsh backend publishes its run
// stream directly onto the data-plane bus (Publisher/Facts), so its
// registration closes the shared SDK subprocess on runtime shutdown.
func registerDSHBackends(backends supervisedRegistrar, agentosSubscriber *agentOSSubscriber, cfg *RuntimeConfig) ([]func() error, error) {
	var closers []func() error

	for i := range cfg.DSHBackends {
		internalConfig := dshBackendConfig(&cfg.DSHBackends[i])

		dshBackend, err := dshbackend.NewBackend(agentosSubscriber, cfg.Publisher, cfg.Facts, cfg.Logger, &internalConfig)
		if err != nil {
			return nil, err
		}

		if err := backends.register(internalConfig.Ref(), dshBackend); err != nil {
			return nil, errors.Join(err, dshBackend.Close())
		}

		closers = append(closers, dshBackend.Close)
	}

	return closers, nil
}

func dshBackendConfig(cfg *DSHBackendConfig) dshbackend.Config {
	return dshbackend.Config{
		Name:       cfg.Name,
		Command:    cfg.Command,
		Profile:    cfg.Profile,
		Args:       cfg.Args,
		Env:        cfg.Env,
		WorkingDir: cfg.WorkingDir,
	}
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
	lifecycle  agentosruntime.LifecyclePublisher
}

type nativeExecutor interface {
	StartExecution(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error)
	GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
	Pause(ctx context.Context, runID string) error
	Resume(ctx context.Context, runID string) error
	Cancel(ctx context.Context, runID string) error
	SignalUserMessage(ctx context.Context, runID string, message *orchestration.UserMessageSignal) error
	// The step-queue mode is driven by these two: a queue that can be changed
	// while it runs, and a step that waits for an outside event. Without them
	// the mode the control plane can start would be one it cannot talk to.
	SignalStepModify(ctx context.Context, runID string, mutation *entity.StepMutation) error
	SignalExternalEvent(ctx context.Context, runID string, event map[string]any) error
}

func newTemporalNativeBackend(
	executor nativeExecutor,
	subscriber agentosruntime.EventSubscriber,
	lifecycle agentosruntime.LifecyclePublisher,
) *temporalNativeBackend {
	return &temporalNativeBackend{
		executor:   executor,
		subscriber: subscriber,
		lifecycle:  lifecycle,
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

	runStatus := runStatusFromEntity(&status)

	// RUN_STARTED is a fact like on every other backend: a persist failure
	// fails the start so the platform retries it — SignalWithStart is
	// idempotent on the workflow id, so the retry is safe.
	if b.lifecycle != nil {
		if err := b.lifecycle.PublishStarted(ctx, spec, &runStatus); err != nil {
			return agentos.RunStatus{}, err
		}
	}

	return runStatus, nil
}

func (b *temporalNativeBackend) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error {
	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentoscore.ErrInvalidSignal)
	}

	if operation, isControl := nativeControlOperation(signal.Type); isControl {
		control := controlRequestFromSignal(operation, signal)

		return b.Control(ctx, runID, &control)
	}

	switch signal.Type {
	case agentoscore.SignalControlPause, agentoscore.SignalControlResume, agentoscore.SignalControlCancel:
		// Handled above, by the control operation the signal carries.
		return nil
	case agentoscore.SignalUserMessage:
		message, err := userMessageSignalToNative(signal)
		if err != nil {
			return err
		}

		return b.executor.SignalUserMessage(ctx, runID, &message)
	case agentoscore.SignalStepModify:
		mutation, err := stepMutationFromSignal(signal)
		if err != nil {
			return err
		}

		return b.executor.SignalStepModify(ctx, runID, mutation)
	case agentoscore.SignalExternalEvent:
		return b.executor.SignalExternalEvent(ctx, runID, signal.Payload)
	case agentoscore.SignalPlanNodeRetry, agentoscore.SignalPlanApprove, agentoscore.SignalPlanReject,
		agentoscore.SignalUserApproval, agentoscore.SignalUserReject, agentoscore.SignalToolResult,
		agentoscore.SignalHumanFeedback, agentoscore.SignalConfigPatch, agentoscore.SignalMemoryPatch:
		return fmt.Errorf("%w: unsupported by temporal native backend: %s", agentoscore.ErrInvalidSignal, signal.Type)
	}

	return fmt.Errorf("%w: unsupported by temporal native backend: %s", agentoscore.ErrInvalidSignal, signal.Type)
}

// nativeControlOperation maps a control-plane signal to the control operation
// it carries, and reports whether it carries one at all.
// nativeControlOperation maps a control-plane signal to the control operation
// it carries, and reports whether it carries one at all.
func nativeControlOperation(signalType agentoscore.SignalType) (agentoscore.ControlOperation, bool) {
	switch signalType {
	case agentoscore.SignalControlPause:
		return agentoscore.ControlPause, true
	case agentoscore.SignalControlResume:
		return agentoscore.ControlResume, true
	case agentoscore.SignalControlCancel:
		return agentoscore.ControlCancel, true
	case agentoscore.SignalPlanNodeRetry, agentoscore.SignalPlanApprove, agentoscore.SignalPlanReject,
		agentoscore.SignalUserMessage, agentoscore.SignalUserApproval, agentoscore.SignalUserReject,
		agentoscore.SignalToolResult, agentoscore.SignalHumanFeedback, agentoscore.SignalConfigPatch,
		agentoscore.SignalStepModify, agentoscore.SignalExternalEvent, agentoscore.SignalMemoryPatch:
		return "", false
	}

	return "", false
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

	runStatus := runStatusFromEntity(&status)

	// Terminal milestones ride the status poll, exactly like the external
	// backends: the lifecycle latch makes them at-most-once, and a persist
	// failure fails the poll so the next one retries the milestone.
	if b.lifecycle != nil {
		if err := b.lifecycle.PublishStatus(ctx, runID, &runStatus); err != nil {
			return agentos.RunStatus{}, err
		}
	}

	return runStatus, nil
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

// stepMutationFromSignal reads a queue mutation from a control-plane signal.
//
// The mutation travels as a payload, so it is decoded through the same
// encoder-decoder every other request body goes through: a signal that does not
// name a mutation is refused rather than applied as a no-op.
func stepMutationFromSignal(signal *agentoscore.Signal) (*entity.StepMutation, error) {
	encoded, err := json.Marshal(signal.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: step mutation cannot be read: %w", agentoscore.ErrInvalidSignal, err)
	}

	var mutation entity.StepMutation
	if err := json.Unmarshal(encoded, &mutation); err != nil {
		return nil, fmt.Errorf("%w: step mutation cannot be read: %w", agentoscore.ErrInvalidSignal, err)
	}

	if mutation.IsEmpty() {
		return nil, fmt.Errorf("%w: step mutation must change something", agentoscore.ErrInvalidSignal)
	}

	return &mutation, nil
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
