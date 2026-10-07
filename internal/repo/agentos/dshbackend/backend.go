package dshbackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/google/uuid"
)

// Run lifecycle states surfaced by Status, in the same spelling the one-shot
// backends use on the wire.
const (
	stateRunning   = "running"
	stateCompleted = "completed"
	stateFailed    = "failed"
	stateCanceled  = "canceled"
)

// Backend error sentinels.
var (
	// errDSHRunNotFound reports a Control/Status call for an unknown run.
	errDSHRunNotFound = errors.New("dsh backend: run not found")
	// errDSHNotificationDropped reports a run whose event stream lost a
	// notification: its timeline can no longer be complete.
	errDSHNotificationDropped = errors.New("dsh backend: notification dropped for session")

	// errDSHSubscriberNotConfigured reports a Subscribe call without a data-plane
	// subscriber wired.
	errDSHSubscriberNotConfigured = errors.New("dsh backend: subscriber not configured")
)

// Backend adapts one DeepSeek Harness SDK subprocess into an AgentOS streaming
// backend. Runs share the subprocess as separate sessions; each run's dsh event
// stream is mapped to entity events and published to the message bus via the
// same PublishWriter path a native streaming backend uses. The dsh stream never
// reaches a frontend directly.
type Backend struct {
	subscriber agentosruntime.EventSubscriber
	publisher  agentosstream.Publisher
	facts      *streamadapter.MilestoneRecorder
	logger     logger.Interface
	config     Config

	mu     sync.Mutex
	client *sdkClient
	runs   map[string]*runState
}

// runState is one in-flight run's session view on the bus.
type runState struct {
	sessionID   string
	threadID    string
	handle      *agentosstream.Handle
	writer      *streamadapter.PublishWriter
	listener    <-chan sdkNotification
	unsubscribe func()
	cancel      atomic.Bool

	mu     sync.Mutex
	status agentos.RunStatus
	done   bool
}

var _ agentosruntime.AgentBackend = (*Backend)(nil)

// NewBackend wires a dsh backend. The publisher is required: a dsh run's whole
// observable life is its bus stream. The subscriber and recorder are optional
// (nil subscriber makes Subscribe fail; nil recorder keeps the run a pure bus
// mirror with no durable facts).
func NewBackend(subscriber agentosruntime.EventSubscriber, publisher agentosstream.Publisher, facts *streamadapter.MilestoneRecorder, l logger.Interface, config *Config) (*Backend, error) {
	if publisher == nil {
		return nil, fmt.Errorf("%w: dsh backend publisher is required", agentoscore.ErrInvalidBackendRef)
	}

	config.withDefaults()

	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Backend{
		subscriber: subscriber,
		publisher:  publisher,
		facts:      facts,
		logger:     l,
		config:     *config,
		runs:       make(map[string]*runState),
	}, nil
}

// Start opens a run: spawns the shared SDK subprocess on first use, registers a
// session listener before the prompt so no event can slip past, publishes the
// RUN_STARTED milestone, and hands the prompt to dsh. The stream then flows from
// the consumer goroutine onto the bus.
func (b *Backend) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	if spec == nil {
		return agentos.RunStatus{}, fmt.Errorf("%w: run spec is required", agentoscore.ErrInvalidRunSpec)
	}

	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	if spec.Backend != b.config.Ref() {
		return agentos.RunStatus{}, fmt.Errorf("%w: backend %s/%s does not match %s/%s", agentoscore.ErrInvalidBackendRef, spec.Backend.Kind, spec.Backend.Name, b.config.Ref().Kind, b.config.Ref().Name)
	}

	client, err := b.ensureClient(ctx)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	state := b.newRunState(spec)

	listener, unsubscribe := client.subscribe(state.sessionID)
	state.listener = listener
	state.unsubscribe = unsubscribe

	b.mu.Lock()
	b.runs[spec.RunID] = state
	b.mu.Unlock()

	// The RUN_STARTED milestone lands before the prompt so it precedes every
	// stream event the consumer publishes, keeping the timeline ordered. A run
	// whose very first fact cannot be persisted never starts: reporting it
	// running would announce a timeline that does not exist.
	if err := b.writeEvent(ctx, state, &entity.AgentRunStartEvent{
		BaseEvent: entity.BaseEvent{
			Source:      entity.SourceAgent,
			Phase:       entity.PhaseStart,
			ContentType: entity.ContentStatus,
		},
		AgentName:    b.config.Name,
		InputSummary: spec.UserMessage,
	}); err != nil {
		unsubscribe()
		b.finishRun(ctx, state, stateFailed)

		return agentos.RunStatus{}, err
	}

	if _, err := client.prompt(ctx, state.sessionID, spec.UserMessage); err != nil {
		unsubscribe()
		b.publishTerminal(ctx, state, agentErrorEvent(stateFailed))

		return agentos.RunStatus{}, err
	}

	// The consumer is started only after dsh accepted the prompt; its context is
	// detached from Start's so an HTTP caller canceling does not stop the stream.
	go b.consume(context.WithoutCancel(ctx), state)

	// Return the start-time status directly rather than reading state.status:
	// the consumer may already have flipped it by the time this line runs.
	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: stateRunning}, nil
}

// newRunState builds the per-run session view: a fresh dsh session id, the run's
// channel handle, and a PublishWriter that makes milestones durable first.
func (b *Backend) newRunState(spec *agentos.RunSpec) *runState {
	handle := streamadapter.HandleForRun(spec.AccountID, spec.RunID)

	writer := streamadapter.NewPublishWriter(b.publisher, handle, spec.ThreadID, spec.RunID, b.logger)

	if b.facts != nil {
		writer = writer.WithFacts(b.facts)
	}

	return &runState{
		sessionID: uuid.New().String(),
		threadID:  spec.ThreadID,
		handle:    handle,
		writer:    writer,
		status:    agentos.RunStatus{RunID: spec.RunID, LifecycleState: stateRunning},
	}
}

// writeEvent makes one event durable and, on the streaming path, publishes it.
//
// A milestone that cannot be persisted must not be announced: the writer
// records the fact before it publishes, so a failed write means the event never
// reached the bus either. Unlike an activity, the DSH consume loop has no
// platform retry to hand the failure to — the recorder already retried once —
// so the loop fails the run instead of continuing with a hole in the timeline
// the control plane would report as a success.
//
// There is deliberately no local spool: "buffer first, persist later" is the
// WAL this architecture ruled out, and it would put a second, unreconciled copy
// of the timeline on the disk of the process running the subprocess.
func (b *Backend) writeEvent(ctx context.Context, state *runState, event entity.StreamEvent) error {
	if err := state.writer.WriteEvent(ctx, event); err != nil {
		return fmt.Errorf("dsh backend: fact %s: %w", event.EventType(), err)
	}

	return nil
}

// logError reports a failure the run cannot hand to anyone else.
func (b *Backend) logError(err error) {
	if b.logger != nil {
		b.logger.Error(err)
	}
}

// consume drains a session's notification stream onto the run's channel until
// the run closes or the subprocess dies.
func (b *Backend) consume(ctx context.Context, state *runState) {
	defer state.unsubscribe()

	names := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.client.done:
			b.failOnProcessExit(ctx, state)

			return
		case notification := <-state.listener:
			if b.failIfNotificationDropped(ctx, state) {
				return
			}

			if b.handleNotification(ctx, state, notification, names) {
				return
			}
		}
	}
}

// failOnProcessExit closes a run whose subprocess went away before the turn
// ended. A run that already finished keeps its terminal state.
func (b *Backend) failOnProcessExit(ctx context.Context, state *runState) {
	if state.isDone() {
		return
	}

	if err := b.writeEvent(ctx, state, agentErrorEvent("dsh sdk process exited before the turn ended")); err != nil {
		b.logError(err)
	}

	b.finishRun(ctx, state, stateFailed)
}

// failIfNotificationDropped ends the run when the transport dropped one of its
// notifications. The dropped notification is the only carrier of the events it
// held, so the run's timeline can no longer be complete — finishing it as if it
// were would report a hole as a success. The flag is sticky, and the subprocess
// dying fails the run through the other branch, so a drop is never missed
// merely because no further notification arrives.
func (b *Backend) failIfNotificationDropped(ctx context.Context, state *runState) bool {
	if !b.client.droppedNotification(state.sessionID) {
		return false
	}

	b.logError(fmt.Errorf("%w: %s", errDSHNotificationDropped, state.sessionID))
	b.finishRun(ctx, state, stateFailed)

	return true
}

// handleNotification processes one server notification, returning true when the
// run closed.
func (b *Backend) handleNotification(ctx context.Context, state *runState, notification sdkNotification, names map[string]string) bool {
	if notification.Method == methodSessionStatus {
		b.handleSessionStatus(state, notification)

		return false
	}

	if notification.Method != methodSessionEvent {
		return false
	}

	var envelope sessionEventNotification

	if err := json.Unmarshal(notification.Params, &envelope); err != nil {
		return false
	}

	collectToolNames(envelope.Event, names)

	events, done := mapEvent(envelope.Event, state.cancel.Load())
	stampToolNames(events, names)

	for _, event := range events {
		// A fact that cannot be persisted ends the run here: continuing would
		// stream a timeline the durable copy does not have, and the run would
		// finish as if nothing were missing.
		if err := b.writeEvent(ctx, state, event); err != nil {
			b.logError(err)
			b.finishRun(ctx, state, stateFailed)

			return true
		}
	}

	if !done {
		return false
	}

	b.finishRun(ctx, state, terminalStatusFor(events))

	return true
}

// handleSessionStatus records the session's running/idle status at debug level.
// The status notification carries no terminal signal — turn/end owns that role —
// so the adapter treats it as diagnostics only.
func (b *Backend) handleSessionStatus(state *runState, notification sdkNotification) {
	var envelope sessionStatusNotification

	if err := json.Unmarshal(notification.Params, &envelope); err != nil {
		return
	}

	if b.logger != nil {
		b.logger.Debug("dsh sdk: session %s status %s", state.sessionID, envelope.Status)
	}
}

// finishRun closes a run: it flushes open messages and records the terminal
// status. A flush that cannot be persisted fails the run instead of the
// requested terminal — the alternative is reporting success while the last
// message's closing fact never landed.
func (b *Backend) finishRun(ctx context.Context, state *runState, terminal string) {
	if err := state.writer.Flush(ctx); err != nil {
		b.logError(fmt.Errorf("dsh backend: flush before %s: %w", terminal, err))

		terminal = stateFailed
	}

	state.mu.Lock()
	state.done = true
	state.status = agentos.RunStatus{RunID: state.status.RunID, LifecycleState: terminal}
	state.mu.Unlock()
}

// publishTerminal writes a terminal milestone and closes the run. It is the
// failure path Start uses when dsh rejects the prompt, so the run ends failed
// whether or not the terminal fact itself could be written.
func (b *Backend) publishTerminal(ctx context.Context, state *runState, terminal entity.StreamEvent) {
	if err := b.writeEvent(ctx, state, terminal); err != nil {
		b.logError(err)
	}

	b.finishRun(ctx, state, stateFailed)
}

// Signal accepts and drops signals. The dsh SDK has no out-of-band signal
// channel; forwarding a signal as a follow-up prompt would publish a second
// turn after this run's terminal milestone, violating terminal closure.
// Capabilities declares SupportsSignal:false.
func (b *Backend) Signal(_ context.Context, runID string, signal *agentoscore.Signal) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidSignal)
	}

	if signal == nil {
		return fmt.Errorf("%w: signal is required", agentoscore.ErrInvalidSignal)
	}

	if signal.Type == "" {
		return fmt.Errorf("%w: signal type is required", agentoscore.ErrInvalidSignal)
	}

	if _, ok := b.run(runID); !ok {
		return fmt.Errorf("%w: run %s", errDSHRunNotFound, runID)
	}

	return nil
}

// Control applies a run control intent. Cancel is honored at the adapter level:
// it flags the run and the next turn/end boundary publishes RUN_CANCELED. The
// status is deliberately left "running" here so a caller polling after Control
// still observes the in-flight run, matching the conformance contract.
func (b *Backend) Control(_ context.Context, runID string, control *agentoscore.ControlRequest) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidControlOperation)
	}

	if err := agentoscore.ValidateControlRequest(control); err != nil {
		return err
	}

	state, ok := b.run(runID)
	if !ok {
		return fmt.Errorf("%w: run %s", errDSHRunNotFound, runID)
	}

	if control.Operation != agentoscore.ControlCancel {
		return fmt.Errorf("%w: unsupported by dsh backend: %s", agentoscore.ErrInvalidControlOperation, control.Operation)
	}

	state.cancel.Store(true)

	return nil
}

// Status returns the run's last known lifecycle view.
func (b *Backend) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	if runID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	state, ok := b.run(runID)
	if !ok {
		return agentos.RunStatus{}, fmt.Errorf("%w: run %s", errDSHRunNotFound, runID)
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	return state.status, nil
}

// Subscribe forwards to the data-plane subscriber.
func (b *Backend) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	if b.subscriber == nil {
		return nil, errDSHSubscriberNotConfigured
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

// Capabilities declares the streaming dsh backend. Cancel is adapter-level (see
// Control); nothing else is supported over the SDK protocol.
func (b *Backend) Capabilities() agentosruntime.BackendCapabilities {
	return agentosruntime.BackendCapabilities{
		SupportsStreaming: true,
		SupportsCancel:    true,
	}
}

// Close shuts the shared SDK subprocess down. In-flight runs are left to their
// consumer, which observes the process death and closes their timelines.
func (b *Backend) Close() error {
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()

	if client == nil {
		return nil
	}

	return client.close(context.Background())
}

// run looks up a run's session state.
func (b *Backend) run(runID string) (*runState, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	state, ok := b.runs[runID]

	return state, ok
}

// isDone reports whether the run already reached a terminal.
func (s *runState) isDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.done
}

// ensureClient lazily spawns the shared SDK subprocess. The subprocess is
// created under the registry lock so concurrent Start calls cannot double-spawn.
func (b *Backend) ensureClient(ctx context.Context) (*sdkClient, error) {
	b.mu.Lock()

	if b.client == nil {
		client := newSDKClient(&b.config, b.logger)

		if err := client.connect(); err != nil {
			b.mu.Unlock()

			return nil, err
		}

		b.client = client
	}

	client := b.client
	b.mu.Unlock()

	if err := client.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// collectToolNames records tool-call names from tool/call and tool-call-delta
// events, since dsh's tool/result carries no name and the tool execution arc
// needs one for display.
func collectToolNames(ev sessionEvent, names map[string]string) {
	switch ev.Type {
	case eventToolCall:
		var data toolCallData

		if json.Unmarshal(ev.Data, &data) == nil && data.CallID != "" && data.Name != "" {
			names[data.CallID] = data.Name
		}
	case eventAssistantChunk:
		var data assistantChunkData

		if json.Unmarshal(ev.Data, &data) == nil && data.Chunk.Type == chunkToolCallDelta && data.Chunk.ID != "" && data.Chunk.Name != "" {
			names[data.Chunk.ID] = data.Chunk.Name
		}
	}
}

// stampToolNames fills the tool name onto mapped completion events using names
// collected from the call arc.
func stampToolNames(events []entity.StreamEvent, names map[string]string) {
	for _, event := range events {
		switch e := event.(type) {
		case *entity.ToolCallFinishEvent:
			if e.ToolName == "" {
				e.ToolName = names[e.ToolCallID]
			}
		case *entity.ToolExecStartEvent:
			if e.ToolName == "" {
				e.ToolName = names[e.ToolCallID]
			}
		case *entity.ToolExecFinishEvent:
			if e.ToolName == "" {
				e.ToolName = names[e.ToolCallID]
			}
		}
	}
}

// terminalStatusFor derives the run lifecycle state from a turn/end's mapped
// terminal event.
func terminalStatusFor(events []entity.StreamEvent) string {
	if len(events) == 0 {
		return stateCompleted
	}

	switch events[len(events)-1].(type) {
	case *entity.AgentRunCanceledEvent:
		return stateCanceled
	case *entity.AgentErrorEvent:
		return stateFailed
	default:
		return stateCompleted
	}
}

// agentErrorEvent builds the terminal error milestone for failed starts and
// process deaths.
func agentErrorEvent(message string) entity.StreamEvent {
	return &entity.AgentErrorEvent{
		BaseEvent: entity.BaseEvent{
			Source:      entity.SourceSystem,
			Phase:       entity.PhaseError,
			ContentType: entity.ContentStatus,
		},
		ErrorMessage: message,
		ErrorCode:    "dsh.backend.failure",
	}
}
