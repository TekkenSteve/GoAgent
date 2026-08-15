package entity

import (
	"fmt"
	"time"
)

// NewBase create BaseEvent, automatically populate EventID, Timestamp, and EventType.
// sessionID / runID will be injected at the Event Store layer (Phase 2), leave blank here.
func NewBase(source EventSource, phase EventPhase, contentType EventContentType, eventType string) BaseEvent {
	return BaseEvent{
		EventType:   eventType,
		Source:      source,
		Phase:       phase,
		ContentType: contentType,
		Timestamp:   time.Now().UTC(),
	}
}

// NewTextDeltaEvent create a text increment event.
func NewTextDeltaEvent(content string, index int) *TextDeltaEvent {
	return &TextDeltaEvent{
		BaseEvent: NewBase(SourceLLM, PhaseDelta, ContentText, "llm.text.delta"),
		Content:   content,
		Index:     index,
	}
}

// NewReasoningDeltaEvent create an incremental event for a thought chain.
func NewReasoningDeltaEvent(reasoning string) *ReasoningDeltaEvent {
	return &ReasoningDeltaEvent{
		BaseEvent: NewBase(SourceLLM, PhaseDelta, ContentReasoning, "llm.reasoning.delta"),
		Reasoning: reasoning,
	}
}

// NewToolCallStartEvent create a tool call start event.
func NewToolCallStartEvent(toolCallID, toolName string, index int) *ToolCallStartEvent {
	return &ToolCallStartEvent{
		BaseEvent:     NewBase(SourceLLM, PhaseStart, ContentToolCall, "llm.tool_call.start"),
		ToolCallID:    toolCallID,
		ToolCallIndex: index,
		ToolName:      toolName,
	}
}

// NewToolCallDeltaEvent create a tool to invoke incremental parameter events.
func NewToolCallDeltaEvent(toolCallID, argsDelta string) *ToolCallDeltaEvent {
	return &ToolCallDeltaEvent{
		BaseEvent:      NewBase(SourceLLM, PhaseDelta, ContentToolCall, "llm.tool_call.delta"),
		ToolCallID:     toolCallID,
		ArgumentsDelta: argsDelta,
	}
}

// NewToolCallFinishEvent create a tool invocation completion event.
func NewToolCallFinishEvent(toolCallID, toolName, arguments string) *ToolCallFinishEvent {
	return &ToolCallFinishEvent{
		BaseEvent:  NewBase(SourceLLM, PhaseFinish, ContentToolCall, "llm.tool_call.finish"),
		ToolCallID: toolCallID,
		ToolName:   toolName,
		Arguments:  arguments,
	}
}

// NewToolExecStartEvent create a tool to execute the start event.
func NewToolExecStartEvent(toolCallID, toolName string) *ToolExecStartEvent {
	return &ToolExecStartEvent{
		BaseEvent:  NewBase(SourceTool, PhaseStart, ContentToolResult, "tool.execution.start"),
		ToolCallID: toolCallID,
		ToolName:   toolName,
	}
}

// NewToolExecFinishEvent create a tool to execute the completion event.
func NewToolExecFinishEvent(toolCallID, toolName, output string, exitCode int, isError bool, durationMs int64) *ToolExecFinishEvent {
	return &ToolExecFinishEvent{
		BaseEvent:  NewBase(SourceTool, PhaseFinish, ContentToolResult, "tool.execution.finish"),
		ToolCallID: toolCallID,
		ToolName:   toolName,
		Output:     output,
		ExitCode:   exitCode,
		IsError:    isError,
		DurationMs: durationMs,
	}
}

// NewAgentRunStartEvent create an Agent to run the start event.
func NewAgentRunStartEvent(inputSummary string) *AgentRunStartEvent {
	return &AgentRunStartEvent{
		BaseEvent:    NewBase(SourceSystem, PhaseStart, ContentStatus, "agent.run.start"),
		InputSummary: inputSummary,
	}
}

// NewAgentRunFinishEvent create an Agent run completion event.
func NewAgentRunFinishEvent(finishReason string, usage *Usage) *AgentRunFinishEvent {
	return &AgentRunFinishEvent{
		BaseEvent:    NewBase(SourceSystem, PhaseFinish, ContentStatus, "agent.run.finish"),
		FinishReason: finishReason,
		Usage:        usage,
	}
}

// NewAgentRunCancelledEvent create an Agent run cancellation event. It is a
// finish-phase lifecycle event, so the data plane projects it as a terminal
// milestone that closes the run's timeline.
func NewAgentRunCancelledEvent() *AgentRunCancelledEvent {
	return &AgentRunCancelledEvent{
		BaseEvent: NewBase(SourceSystem, PhaseFinish, ContentStatus, "agent.run.cancelled"),
	}
}

// NewPrepStageEvent create an initialization phase progress event.
func NewPrepStageEvent(stage string, progress int) *PrepStageEvent {
	return &PrepStageEvent{
		BaseEvent: NewBase(SourceSystem, PhaseDelta, ContentStatus, "system.prep_stage.delta"),
		Stage:     stage,
		Progress:  progress,
	}
}

// NewContextUsageEvent create a context usage event.
func NewContextUsageEvent(msgCount int, before, after *int, compressed *bool) *ContextUsageEvent {
	return &ContextUsageEvent{
		BaseEvent:      NewBase(SourceSystem, PhaseDelta, ContentStatus, "system.context_usage.delta"),
		MessageCount:   msgCount,
		MessagesBefore: before,
		MessagesAfter:  after,
		Compressed:     compressed,
	}
}

// NewAgentErrorEvent create a system error event.
func NewAgentErrorEvent(code, message string) *AgentErrorEvent {
	return &AgentErrorEvent{
		BaseEvent:    NewBase(SourceSystem, PhaseError, ContentStatus, "system.error"),
		ErrorMessage: message,
		ErrorCode:    code,
	}
}

// NewUsageFinishEvent creates a usage/finish event for LLM call completion.
func NewUsageFinishEvent(usage Usage, model string) *UsageFinishEvent {
	return &UsageFinishEvent{
		BaseEvent: NewBase(SourceSystem, PhaseFinish, ContentStatus, "llm.usage.finish"),
		Usage:     usage,
		Model:     model,
	}
}

// EventTypeFromParts generate a standard event_type string based on the three-dimensional enumeration.
func EventTypeFromParts(source EventSource, contentType EventContentType, phase EventPhase) string {
	return fmt.Sprintf("%s.%s.%s", source, contentType, phase)
}
