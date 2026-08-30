package streamadapter

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// Domain custom event names. The control plane owns the agentos.* namespace:
// these events ride the same timeline the frontend sees but their authoritative
// copy is the control plane's own durable store, so they are never re-projected
// from the bus.
const (
	customToolExecPrefix = "agentos.tool.execution."
	customSystemPrefix   = "agentos.system."
	customLLMPrefix      = "agentos.llm."
	customUserPrefix     = "agentos.user."

	// toolCallIDKey is the correlation id carried by agentos-owned CUSTOM tool
	// execution events (kept distinct from the AG-UI callId vocabulary).
	toolCallIDKey = "toolCallId"

	// Runtime event kind names (entity.EventType() values) as named constants:
	// the dispatch switches reference each kind twice (domain gate + concrete
	// mapper), so string literals would trip goconst.
	evTextDelta        = "llm.text.delta"
	evReasoningDelta   = "llm.reasoning.delta"
	evToolCallStart    = "llm.tool_call.start"
	evToolCallDelta    = "llm.tool_call.delta"
	evToolCallFinish   = "llm.tool_call.finish"
	evUsageFinish      = "llm.usage.finish"
	evToolExecStart    = "tool.execution.start"
	evToolExecStdout   = "tool.execution.stdout"
	evToolExecStderr   = "tool.execution.stderr"
	evToolExecFinish   = "tool.execution.finish"
	evAgentRunStart    = "agent.run.start"
	evAgentRunFinish   = "agent.run.finish"
	evAgentRunCanceled = "agent.run.canceled"
	evPrepStage        = "system.prep_stage.delta"
	evContextUsage     = "system.context_usage.delta"
	evStateDelta       = "system.state.delta"
	evInterrupt        = "system.interrupt"
	evAgentError       = "system.error"
	evUserCommand      = "user.command"
	evUserFeedback     = "user.feedback"
)

// MapEvent translates one runtime stream event into the AG-UI wire event the
// data plane carries. The mapping is a pure function — the stateful message
// open/close synthesis lives in PublishWriter, not here. Milestone choices
// align with agentos/stream.ProjectToCore: anything transient here is excluded
// from the durable projection by construction.
//
// Dispatch is two-level: this switch groups the 20 runtime kinds into five
// domains, and each domain dispatcher picks the concrete mapper. Splitting the
// one big switch this way keeps cyclomatic complexity bounded (gocyclo) without
// a package-level table (gochecknoglobals).
func MapEvent(ev entity.StreamEvent) (*stream.Event, error) {
	switch ev.EventType() {
	case evTextDelta, evReasoningDelta, evToolCallStart, evToolCallDelta, evToolCallFinish, evUsageFinish:
		return mapAssistantEvent(ev)
	case evToolExecStart, evToolExecStdout, evToolExecStderr, evToolExecFinish:
		return mapExecutionEvent(ev)
	case evAgentRunStart, evAgentRunFinish, evAgentRunCanceled:
		return mapLifecycleEvent(ev)
	case evPrepStage, evContextUsage, evStateDelta, evInterrupt, evAgentError:
		return mapSystemEvent(ev)
	case evUserCommand, evUserFeedback:
		return mapUserEvent(ev)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedStreamEvent, ev.EventType())
	}
}

func mapAssistantEvent(ev entity.StreamEvent) (*stream.Event, error) {
	switch ev.EventType() {
	case evTextDelta:
		return mapTextDelta(ev)
	case evReasoningDelta:
		return mapReasoningDelta(ev)
	case evToolCallStart:
		return mapToolCallStart(ev)
	case evToolCallDelta:
		return mapToolCallDelta(ev)
	case evToolCallFinish:
		return mapToolCallFinish(ev)
	case evUsageFinish:
		return mapUsageFinish(ev)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedStreamEvent, ev.EventType())
	}
}

func mapExecutionEvent(ev entity.StreamEvent) (*stream.Event, error) {
	switch ev.EventType() {
	case evToolExecStart:
		return mapToolExecStart(ev)
	case evToolExecStdout:
		return mapToolExecStdout(ev)
	case evToolExecStderr:
		return mapToolExecStderr(ev)
	case evToolExecFinish:
		return mapToolExecFinish(ev)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedStreamEvent, ev.EventType())
	}
}

func mapLifecycleEvent(ev entity.StreamEvent) (*stream.Event, error) {
	switch ev.EventType() {
	case evAgentRunStart:
		return mapAgentRunStart(ev)
	case evAgentRunFinish:
		return mapAgentRunFinish(ev)
	case evAgentRunCanceled:
		return mapAgentRunCanceled(ev)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedStreamEvent, ev.EventType())
	}
}

func mapSystemEvent(ev entity.StreamEvent) (*stream.Event, error) {
	switch ev.EventType() {
	case evPrepStage:
		return mapPrepStage(ev)
	case evContextUsage:
		return mapContextUsage(ev)
	case evStateDelta:
		return mapStateDelta(ev)
	case evInterrupt:
		return mapInterrupt(ev)
	case evAgentError:
		return mapAgentError(ev)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedStreamEvent, ev.EventType())
	}
}

func mapUserEvent(ev entity.StreamEvent) (*stream.Event, error) {
	switch ev.EventType() {
	case evUserCommand:
		return mapUserCommand(ev)
	case evUserFeedback:
		return mapUserFeedback(ev)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedStreamEvent, ev.EventType())
	}
}

// scope derives the AG-UI thread/run identity from the entity's base metadata.
// The runtime session is the frontend thread.
func scope(ev entity.StreamEvent) (threadID, runID string) {
	base := ev.Base()

	return base.SessionID, base.RunID
}

func mapTextDelta(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.TextDeltaEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	// The writer synthesizes TEXT_MESSAGE_START and stamps the message id.
	return stream.NewTextMessageContent(threadID, runID, "", e.Content), nil
}

func mapReasoningDelta(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ReasoningDeltaEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewReasoningMessageContent(threadID, runID, "", e.Reasoning), nil
}

func mapToolCallStart(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolCallStartEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewToolCallStart(threadID, runID, e.ToolCallID, e.ToolName), nil
}

func mapToolCallDelta(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolCallDeltaEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewToolCallArgs(threadID, runID, e.ToolCallID, e.ArgumentsDelta), nil
}

func mapToolCallFinish(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolCallFinishEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	// TOOL_CALL_END carries the assembled arguments for presentation. The
	// milestone for tool completion is tool.execution.finish below, so this
	// stays transient — no duplicate durable tool-completed event.
	return stream.NewEvent(stream.EventToolCallEnd).SetThread(runID, threadID).
		Set(stream.FieldCallID, e.ToolCallID).
		Set(stream.FieldDelta, e.Arguments), nil
}

func mapUsageFinish(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.UsageFinishEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewCustom(threadID, runID, customLLMPrefix+"usage").
		Set(stream.FieldUsage, usageValue(e.Usage)).
		Set("model", e.Model), nil
}

func mapToolExecStart(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolExecStartEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewCustom(threadID, runID, customToolExecPrefix+"start").
		Set(toolCallIDKey, e.ToolCallID).
		Set("toolName", e.ToolName), nil
}

func mapToolExecStdout(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolExecStdoutEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewCustom(threadID, runID, customToolExecPrefix+"stdout").
		Set(toolCallIDKey, e.ToolCallID).
		Set(stream.FieldDelta, e.StdoutDelta), nil
}

func mapToolExecStderr(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolExecStderrEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewCustom(threadID, runID, customToolExecPrefix+"stderr").
		Set(toolCallIDKey, e.ToolCallID).
		Set(stream.FieldDelta, e.StderrDelta), nil
}

func mapToolExecFinish(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ToolExecFinishEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	meta := map[string]any{"exit_code": e.ExitCode, "duration_ms": e.DurationMs}
	if e.IsError {
		return stream.NewEvent(stream.EventToolCallError).SetThread(runID, threadID).
			Set(stream.FieldCallID, e.ToolCallID).
			Set(stream.FieldError, map[string]any{"message": e.Output}).
			Set(stream.FieldMeta, meta), nil
	}

	return stream.NewEvent(stream.EventToolCallResult).SetThread(runID, threadID).
		Set(stream.FieldCallID, e.ToolCallID).
		Set(stream.FieldResult, e.Output).
		Set(stream.FieldMeta, meta), nil
}

func mapAgentRunStart(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.AgentRunStartEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewRunStarted(threadID, runID).Set("agentName", e.AgentName), nil
}

func mapAgentRunFinish(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.AgentRunFinishEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	agg := stream.NewRunFinished(threadID, runID).Set("finishReason", e.FinishReason)
	if e.Usage != nil {
		agg.Set(stream.FieldUsage, usageValue(*e.Usage))
	}

	return agg, nil
}

func mapAgentRunCanceled(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.AgentRunCanceledEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewRunCanceled(threadID, runID), nil
}

func mapPrepStage(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.PrepStageEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	return stream.NewCustom(threadID, runID, customSystemPrefix+"prep_stage").
		Set("stage", e.Stage).
		Set("progress", e.Progress), nil
}

func mapContextUsage(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.ContextUsageEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	custom := stream.NewCustom(threadID, runID, customSystemPrefix+"context_usage").
		Set("messageCount", e.MessageCount)
	if e.MessagesBefore != nil {
		custom.Set("messagesBefore", *e.MessagesBefore)
	}

	if e.MessagesAfter != nil {
		custom.Set("messagesAfter", *e.MessagesAfter)
	}

	if e.Compressed != nil {
		custom.Set("compressed", *e.Compressed)
	}

	return custom, nil
}

func mapStateDelta(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.StateDeltaEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	// Keyed update — not mapped to AG-UI STATE_DELTA, which is RFC 6902 JSON
	// Patch semantics the runtime does not emit.
	return stream.NewCustom(threadID, runID, customSystemPrefix+"state_delta").
		Set("key", e.Key).
		Set("value", e.Value), nil
}

func mapInterrupt(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.InterruptEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	// A runtime interrupt is not a terminal cancellation: it stays a transient
	// custom event rather than closing the timeline. A deliberate cancel is
	// expressed separately via the agent.run.canceled milestone, which the
	// projector treats as a terminal one.
	return stream.NewCustom(threadID, runID, customSystemPrefix+"interrupt").
		Set("reason", e.Reason), nil
}

func mapAgentError(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.AgentErrorEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	if !e.Recoverable {
		return stream.NewRunError(threadID, runID, fmt.Errorf("%w: %s: %s", ErrRunFatal, e.ErrorCode, e.ErrorMessage)), nil
	}

	return stream.NewCustom(threadID, runID, customSystemPrefix+"error").
		Set(stream.FieldError, map[string]any{"message": e.ErrorMessage, "code": e.ErrorCode}), nil
}

func mapUserCommand(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.UserCommandEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	custom := stream.NewCustom(threadID, runID, customUserPrefix+"command").Set("command", e.Command)
	if len(e.Payload) > 0 {
		custom.Set("payload", e.Payload)
	}

	return custom, nil
}

func mapUserFeedback(ev entity.StreamEvent) (*stream.Event, error) {
	e, ok := ev.(*entity.UserFeedbackEvent)
	if !ok {
		return nil, unsupportedType(ev)
	}

	threadID, runID := scope(e)

	custom := stream.NewCustom(threadID, runID, customUserPrefix+"feedback").
		Set("targetEventId", e.TargetEventID).
		Set("feedbackType", e.FeedbackType)
	if e.Payload != nil {
		custom.Set("payload", e.Payload)
	}

	return custom, nil
}

func unsupportedType(ev entity.StreamEvent) error {
	return fmt.Errorf("%w: %s is %T", ErrUnsupportedStreamEvent, ev.EventType(), ev)
}

// usageValue serializes an entity Usage for the RUN_FINISHED payload. The
// projector bills from this without the byte stream ever reaching the control
// plane.
func usageValue(u entity.Usage) map[string]any {
	return map[string]any{
		"prompt_tokens":     u.PromptTokens,
		"completion_tokens": u.CompletionTokens,
		"total_tokens":      u.TotalTokens,
		"cost":              u.Cost,
	}
}
