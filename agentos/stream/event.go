// Package stream defines the engine-neutral data-plane contract for agent
// streaming: the AG-UI event vocabulary that every backend publishes and
// every frontend consumes, the Handle issued by the control plane, and the
// Publisher / Subscriber / Projector interfaces behind which the transport
// (Centrifugo in production, an in-memory bus in tests/dev) hides.
//
// The package carries zero Temporal, Redis, or HTTP dependencies. Its only
// import is agentos/core, used by the projection mapping (ProjectToCore) so
// the control plane can reduce the wire stream into its durable event model.
package stream

import "fmt"

// EventType identifies an AG-UI wire event. The vocabulary is the fixed
// agent↔UI protocol set; domain-specific semantics ride on CUSTOM with a
// namespaced name, never on new top-level types.
type EventType string

const (
	// Run lifecycle.
	EventRunStarted   EventType = "RUN_STARTED"
	EventRunFinished  EventType = "RUN_FINISHED"
	EventRunError     EventType = "RUN_ERROR"
	EventRunCancelled EventType = "RUN_CANCELLED"

	// Step lifecycle. STEP_* events may carry turn/step numbers in payload
	// (see FieldTurn / FieldStep) — the minimal extension that gives AG-UI
	// the turn/step nesting a rich session model like dsh's has.
	EventStepStarted  EventType = "STEP_STARTED"
	EventStepFinished EventType = "STEP_FINISHED"

	// Text messages.
	EventTextMessageStart   EventType = "TEXT_MESSAGE_START"
	EventTextMessageContent EventType = "TEXT_MESSAGE_CONTENT"
	EventTextMessageEnd     EventType = "TEXT_MESSAGE_END"

	// Reasoning (thinking / internal chain-of-thought).
	EventReasoningStart          EventType = "REASONING_START"
	EventReasoningMessageStart   EventType = "REASONING_MESSAGE_START"
	EventReasoningMessageContent EventType = "REASONING_MESSAGE_CONTENT"
	EventReasoningMessageEnd     EventType = "REASONING_MESSAGE_END"

	// Tool calls.
	EventToolCallStart  EventType = "TOOL_CALL_START"
	EventToolCallArgs   EventType = "TOOL_CALL_ARGS"
	EventToolCallResult EventType = "TOOL_CALL_RESULT"
	EventToolCallEnd    EventType = "TOOL_CALL_END"
	EventToolCallError  EventType = "TOOL_CALL_ERROR"

	// State.
	EventStateSnapshot    EventType = "STATE_SNAPSHOT"
	EventStateDelta       EventType = "STATE_DELTA"
	EventMessagesSnapshot EventType = "MESSAGES_SNAPSHOT"
	EventActivitySnapshot EventType = "ACTIVITY_SNAPSHOT"
	EventActivityDelta    EventType = "ACTIVITY_DELTA"

	// Raw passthrough.
	EventRaw EventType = "RAW"

	// Custom carries a domain event inside the same timeline. The payload
	// MUST set FieldName to a namespaced name ("agentos.approval.requested",
	// "<backend>.file.diff", ...). Frontends may safely ignore unknown custom
	// names; the control plane never re-projects them (their authoritative
	// copy is the control plane's own durable store).
	EventCustom EventType = "CUSTOM"
)

// Payload field names shared across AG-UI event types. Keys are camelCase to
// match the AG-UI wire format verbatim — the frontend SDKs read these as-is.
const (
	FieldDelta   = "delta"   // text/reasoning/args increment
	FieldResult  = "result"  // completed tool result
	FieldMeta    = "meta"    // tool-owned presentation payload (opaque JSON)
	FieldError   = "error"   // structured error
	FieldName    = "name"    // CUSTOM event name (namespaced)
	FieldCallID  = "callId"  // tool call correlation id
	FieldIsError = "isError" // tool result error flag (AG-UI)
	FieldUsage   = "usage"   // token accounting reported on RUN_FINISHED
	FieldTurn    = "turn"    // 1-based turn number on STEP_*
	FieldStep    = "step"    // 1-based step number within a turn on STEP_*
	FieldState   = "state"   // STATE_SNAPSHOT full state
	FieldPatch   = "patch"   // STATE_DELTA JSON Patch (RFC 6902)
)

// Event is the AG-UI wire envelope. Type discriminates the payload; every
// backend emits the same vocabulary so any frontend SDK recognizes the stream
// regardless of which backend produced it.
type Event struct {
	ID        string         `json:"id,omitempty"`
	Type      EventType      `json:"type"`
	ThreadID  string         `json:"threadId,omitempty"`
	RunID     string         `json:"runId,omitempty"`
	MessageID string         `json:"messageId,omitempty"`
	StepID    string         `json:"stepId,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// NewEvent creates an event with an empty payload map ready to fill.
func NewEvent(typ EventType) *Event {
	return &Event{Type: typ, Payload: map[string]any{}}
}

// Set is a convenience for filling payload fields.
func (e *Event) Set(key string, value any) *Event {
	if e.Payload == nil {
		e.Payload = map[string]any{}
	}

	e.Payload[key] = value

	return e
}

// Validate checks the wire envelope. Type is required; RAW and CUSTOM (with a
// namespaced name) intentionally accept any extension so the vocabulary can
// grow without a protocol bump.
func (e *Event) Validate() error {
	if e == nil {
		return fmt.Errorf("%w: event is required", ErrInvalidEvent)
	}

	if e.Type == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidEvent)
	}

	if e.Type == EventCustom {
		name, ok := e.Payload[FieldName].(string)
		if !ok || name == "" {
			return fmt.Errorf("%w: CUSTOM requires %q", ErrInvalidEvent, FieldName)
		}
	}

	return nil
}

// IsMilestone reports whether the control plane must persist this event to
// its durable projection. Byte deltas (text/reasoning/args content, state and
// activity increments) are transient — they live in the bus history window
// only. CUSTOM events are intentionally excluded: the control plane owns them
// and their authoritative copy is already its durable store.
func IsMilestone(typ EventType) bool {
	switch typ {
	case EventRunStarted, EventRunFinished, EventRunError, EventRunCancelled,
		EventStepStarted, EventStepFinished,
		EventTextMessageEnd,
		EventToolCallStart, EventToolCallResult, EventToolCallError:
		return true
	case EventTextMessageStart, EventTextMessageContent,
		EventReasoningStart, EventReasoningMessageStart, EventReasoningMessageContent, EventReasoningMessageEnd,
		EventToolCallArgs, EventToolCallEnd,
		EventStateSnapshot, EventStateDelta, EventMessagesSnapshot, EventActivitySnapshot, EventActivityDelta,
		EventRaw, EventCustom:
		return false
	}

	return false
}

// IsTransient is the complement of IsMilestone for the known vocabulary:
// events that exist only in the bus history window and are never re-projected.
func IsTransient(typ EventType) bool {
	return !IsMilestone(typ)
}

// KnownEventTypes lists the full AG-UI vocabulary this contract supports,
// in a stable order for docs and SDK generation.
func KnownEventTypes() []EventType {
	return []EventType{
		EventRunStarted, EventRunFinished, EventRunError, EventRunCancelled,
		EventStepStarted, EventStepFinished,
		EventTextMessageStart, EventTextMessageContent, EventTextMessageEnd,
		EventReasoningStart, EventReasoningMessageStart, EventReasoningMessageContent, EventReasoningMessageEnd,
		EventToolCallStart, EventToolCallArgs, EventToolCallResult, EventToolCallEnd, EventToolCallError,
		EventStateSnapshot, EventStateDelta, EventMessagesSnapshot, EventActivitySnapshot, EventActivityDelta,
		EventRaw, EventCustom,
	}
}

// Event constructors. These are the three-step integration contract made
// concrete: map a backend output to one of these, publish it, report usage on
// RUN_FINISHED.

// NewRunStarted opens a run's timeline.
func NewRunStarted(threadID, runID string) *Event {
	return NewEvent(EventRunStarted).SetThread(runID, threadID)
}

// NewRunFinished closes a run's timeline. Pass usage on the payload so the
// projector can bill from it without the byte stream ever reaching the
// control plane.
func NewRunFinished(threadID, runID string) *Event {
	return NewEvent(EventRunFinished).SetThread(runID, threadID)
}

// NewRunError closes a run's timeline with a failure.
func NewRunError(threadID, runID string, err error) *Event {
	return NewEvent(EventRunError).SetThread(runID, threadID).Set(FieldError, errorValue(err))
}

// NewRunCancelled closes a run's timeline as canceled. The projector treats it
// as a terminal milestone, so a canceled run never lingers as a live
// projection.
func NewRunCancelled(threadID, runID string) *Event {
	return NewEvent(EventRunCancelled).SetThread(runID, threadID)
}

// NewStepStarted opens one model call (turn/step numbering is the minimal
// extension that preserves turn-step nesting on the wire).
func NewStepStarted(threadID, runID string, turn, step int) *Event {
	return NewEvent(EventStepStarted).SetThread(runID, threadID).Set(FieldTurn, turn).Set(FieldStep, step)
}

// NewStepFinished closes one model call.
func NewStepFinished(threadID, runID string, turn, step int) *Event {
	return NewEvent(EventStepFinished).SetThread(runID, threadID).Set(FieldTurn, turn).Set(FieldStep, step)
}

// NewTextMessageContent emits a token delta for an in-progress text message.
func NewTextMessageContent(threadID, runID, messageID, delta string) *Event {
	return NewEvent(EventTextMessageContent).SetThread(runID, threadID).SetMessage(messageID).Set(FieldDelta, delta)
}

// NewTextMessageEnd marks a text message complete (a projection milestone).
func NewTextMessageEnd(threadID, runID, messageID string) *Event {
	return NewEvent(EventTextMessageEnd).SetThread(runID, threadID).SetMessage(messageID)
}

// NewReasoningMessageContent emits a reasoning (thinking) delta.
func NewReasoningMessageContent(threadID, runID, messageID, delta string) *Event {
	return NewEvent(EventReasoningMessageContent).SetThread(runID, threadID).SetMessage(messageID).Set(FieldDelta, delta)
}

// NewToolCallStart opens a tool invocation requested by the model.
func NewToolCallStart(threadID, runID, callID, name string) *Event {
	return NewEvent(EventToolCallStart).SetThread(runID, threadID).Set(FieldCallID, callID).Set(FieldName, name)
}

// NewToolCallArgs emits a partial tool arguments delta.
func NewToolCallArgs(threadID, runID, callID, delta string) *Event {
	return NewEvent(EventToolCallArgs).SetThread(runID, threadID).Set(FieldCallID, callID).Set(FieldDelta, delta)
}

// NewToolCallResult completes a tool call. Meta carries tool-owned
// presentation JSON (opaque to the bus and control plane).
func NewToolCallResult(threadID, runID, callID string, result any) *Event {
	return NewEvent(EventToolCallResult).SetThread(runID, threadID).Set(FieldCallID, callID).Set(FieldResult, result)
}

// NewToolCallError completes a tool call with a failure.
func NewToolCallError(threadID, runID, callID string, err error) *Event {
	return NewEvent(EventToolCallError).SetThread(runID, threadID).Set(FieldCallID, callID).Set(FieldError, errorValue(err))
}

// NewCustom publishes a domain event on the same timeline. The name must be
// namespaced ("agentos.*", "<backend>.*").
func NewCustom(threadID, runID, name string) *Event {
	return NewEvent(EventCustom).SetThread(runID, threadID).Set(FieldName, name)
}

// SetThread stamps the event's run/thread scope.
func (e *Event) SetThread(runID, threadID string) *Event {
	e.RunID, e.ThreadID = runID, threadID

	return e
}

// SetMessage stamps the event's message id.
func (e *Event) SetMessage(messageID string) *Event {
	e.MessageID = messageID

	return e
}

func errorValue(err error) map[string]any {
	if err == nil {
		return nil
	}

	return map[string]any{"message": err.Error()}
}
