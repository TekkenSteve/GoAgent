package entity

import "slices"

// AgentOSEventType identifies the canonical event kinds emitted by AgentOS backends.
type AgentOSEventType string

const (
	// AgentOSEventRunStarted is emitted when a run starts.
	AgentOSEventRunStarted AgentOSEventType = "run.started"
	// AgentOSEventRunCompleted is emitted when a run completes.
	AgentOSEventRunCompleted AgentOSEventType = "run.completed"
	// AgentOSEventRunFailed is emitted when a run fails.
	AgentOSEventRunFailed AgentOSEventType = "run.failed"
	// AgentOSEventRunCancelled is emitted when a run is canceled.
	AgentOSEventRunCancelled AgentOSEventType = "run.cancelled"
	// AgentOSEventRunPaused is emitted when a run is paused.
	AgentOSEventRunPaused AgentOSEventType = "run.paused"
	// AgentOSEventRunResumed is emitted when a run resumes.
	AgentOSEventRunResumed AgentOSEventType = "run.resumed"
	// AgentOSEventAgentStepStarted is emitted when an agent step starts.
	AgentOSEventAgentStepStarted AgentOSEventType = "agent.step.started"
	// AgentOSEventAgentStepCompleted is emitted when an agent step completes.
	AgentOSEventAgentStepCompleted AgentOSEventType = "agent.step.completed"
	// AgentOSEventAgentStepFailed is emitted when an agent step fails.
	AgentOSEventAgentStepFailed AgentOSEventType = "agent.step.failed"
	// AgentOSEventAgentMessageDelta is emitted when an agent message is updated incrementally.
	AgentOSEventAgentMessageDelta AgentOSEventType = "agent.message.delta"
	// AgentOSEventAgentMessageCompleted is emitted when an agent message is complete.
	AgentOSEventAgentMessageCompleted AgentOSEventType = "agent.message.completed"
	// AgentOSEventToolCallStarted is emitted when a tool call starts.
	AgentOSEventToolCallStarted AgentOSEventType = "tool.call.started"
	// AgentOSEventToolCallDelta is emitted when a tool call's arguments are updated incrementally.
	AgentOSEventToolCallDelta AgentOSEventType = "tool.call.delta"
	// AgentOSEventToolCallCompleted is emitted when a tool call completes.
	AgentOSEventToolCallCompleted AgentOSEventType = "tool.call.completed"
	// AgentOSEventToolCallFailed is emitted when a tool call fails.
	AgentOSEventToolCallFailed AgentOSEventType = "tool.call.failed"
	// AgentOSEventApprovalRequested is emitted when user approval is requested.
	AgentOSEventApprovalRequested AgentOSEventType = "approval.requested"
	// AgentOSEventApprovalResolved is emitted when an approval request is resolved.
	AgentOSEventApprovalResolved AgentOSEventType = "approval.resolved"
	// AgentOSEventUsageReported reports token usage for a run.
	AgentOSEventUsageReported AgentOSEventType = "usage.reported"
	// AgentOSEventCheckpointCreated is emitted when a checkpoint is created.
	AgentOSEventCheckpointCreated AgentOSEventType = "checkpoint.created"
)

// IsAgentOSStandardEventType reports whether the event type is one of the standard AgentOS event types.
func IsAgentOSStandardEventType(eventType string) bool {
	return slices.Contains(AgentOSStandardEventTypes(), AgentOSEventType(eventType))
}

// AgentOSStandardEventTypes returns the standard AgentOS event types.
func AgentOSStandardEventTypes() []AgentOSEventType {
	return []AgentOSEventType{
		AgentOSEventRunStarted,
		AgentOSEventRunCompleted,
		AgentOSEventRunFailed,
		AgentOSEventRunCancelled,
		AgentOSEventRunPaused,
		AgentOSEventRunResumed,
		AgentOSEventAgentStepStarted,
		AgentOSEventAgentStepCompleted,
		AgentOSEventAgentStepFailed,
		AgentOSEventAgentMessageDelta,
		AgentOSEventAgentMessageCompleted,
		AgentOSEventToolCallStarted,
		AgentOSEventToolCallDelta,
		AgentOSEventToolCallCompleted,
		AgentOSEventToolCallFailed,
		AgentOSEventApprovalRequested,
		AgentOSEventApprovalResolved,
		AgentOSEventUsageReported,
		AgentOSEventCheckpointCreated,
	}
}

// ——— LLM Event ———

// TextDeltaEvent is emitted for each incremental chunk of LLM text output.
type TextDeltaEvent struct {
	BaseEvent
	Content string `json:"content"`
	Index   int    `json:"index"` // content block serial number, used when multiple blocks are interleaved
}

// Base returns the metadata embedded in this event.
func (e *TextDeltaEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *TextDeltaEvent) EventType() string { return "llm.text.delta" }

// ReasoningDeltaEvent is emitted for each incremental chunk of LLM reasoning output.
type ReasoningDeltaEvent struct {
	BaseEvent
	Reasoning string `json:"reasoning"`
}

// Base returns the metadata embedded in this event.
func (e *ReasoningDeltaEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ReasoningDeltaEvent) EventType() string { return "llm.reasoning.delta" }

// ToolCallStartEvent is emitted when the LLM starts a tool call.
type ToolCallStartEvent struct {
	BaseEvent
	ToolCallID    string `json:"tool_call_id"`
	ToolCallIndex int    `json:"tool_call_index"`
	ToolName      string `json:"tool_name"`
}

// Base returns the metadata embedded in this event.
func (e *ToolCallStartEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolCallStartEvent) EventType() string { return "llm.tool_call.start" }

// ToolCallDeltaEvent carries an incremental fragment of a tool call's arguments.
type ToolCallDeltaEvent struct {
	BaseEvent
	ToolCallID     string `json:"tool_call_id"`
	ArgumentsDelta string `json:"arguments_delta"`
}

// Base returns the metadata embedded in this event.
func (e *ToolCallDeltaEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolCallDeltaEvent) EventType() string { return "llm.tool_call.delta" }

// ToolCallFinishEvent is emitted when the LLM finishes a tool call with its complete arguments.
type ToolCallFinishEvent struct {
	BaseEvent
	ToolCallID   string `json:"tool_call_id"`
	ToolName     string `json:"tool_name"`
	Arguments    string `json:"arguments"`
	RawArguments string `json:"raw_arguments,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *ToolCallFinishEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolCallFinishEvent) EventType() string { return "llm.tool_call.finish" }

// UsageFinishEvent reports the token usage of an LLM response.
type UsageFinishEvent struct {
	BaseEvent
	Usage Usage  `json:"usage"`
	Model string `json:"model"`
}

// Base returns the metadata embedded in this event.
func (e *UsageFinishEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *UsageFinishEvent) EventType() string { return "llm.usage.finish" }

// ——— Tool execution event ———

// ToolExecStartEvent is emitted when tool execution begins.
type ToolExecStartEvent struct {
	BaseEvent
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"tool_name"`
}

// Base returns the metadata embedded in this event.
func (e *ToolExecStartEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolExecStartEvent) EventType() string { return "tool.execution.start" }

// ToolExecStdoutEvent carries incremental stdout output from a running tool.
type ToolExecStdoutEvent struct {
	BaseEvent
	ToolCallID  string `json:"tool_call_id"`
	StdoutDelta string `json:"stdout_delta"`
}

// Base returns the metadata embedded in this event.
func (e *ToolExecStdoutEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolExecStdoutEvent) EventType() string { return "tool.execution.stdout" }

// ToolExecStderrEvent carries incremental stderr output from a running tool.
type ToolExecStderrEvent struct {
	BaseEvent
	ToolCallID  string `json:"tool_call_id"`
	StderrDelta string `json:"stderr_delta"`
}

// Base returns the metadata embedded in this event.
func (e *ToolExecStderrEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolExecStderrEvent) EventType() string { return "tool.execution.stderr" }

// ToolExecFinishEvent is emitted when tool execution finishes with its final output.
type ToolExecFinishEvent struct {
	BaseEvent
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"tool_name"`
	Output     string `json:"output,omitempty"`
	ExitCode   int    `json:"exit_code"`
	IsError    bool   `json:"is_error"`
	DurationMs int64  `json:"duration_ms,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *ToolExecFinishEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ToolExecFinishEvent) EventType() string { return "tool.execution.finish" }

// ——— Agent Event ———

// AgentRunStartEvent is emitted when an agent run starts.
type AgentRunStartEvent struct {
	BaseEvent
	AgentName    string `json:"agent_name"`
	InputSummary string `json:"input_summary,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *AgentRunStartEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *AgentRunStartEvent) EventType() string { return "agent.run.start" }

// AgentRunFinishEvent is emitted when an agent run finishes.
type AgentRunFinishEvent struct {
	BaseEvent
	FinishReason string `json:"finish_reason"`
	Usage        *Usage `json:"usage,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *AgentRunFinishEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *AgentRunFinishEvent) EventType() string { return "agent.run.finish" }

// AgentRunCancelledEvent is emitted when an agent run is canceled. It closes
// the run's timeline as a terminal milestone on the data plane.
type AgentRunCancelledEvent struct {
	BaseEvent
}

// Base returns the metadata embedded in this event.
func (e *AgentRunCancelledEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *AgentRunCancelledEvent) EventType() string { return "agent.run.cancelled" }

// ——— System event ———

// PrepStageEvent reports the progress of the run preparation pipeline.
type PrepStageEvent struct {
	BaseEvent
	Stage    string `json:"stage"`    // "initializing" | "ready" | "summarizing"
	Progress int    `json:"progress"` // 0-100
}

// Base returns the metadata embedded in this event.
func (e *PrepStageEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *PrepStageEvent) EventType() string { return "system.prep_stage.delta" }

// ContextUsageEvent reports changes to context usage, such as compaction.
type ContextUsageEvent struct {
	BaseEvent
	MessageCount   int   `json:"message_count"`
	MessagesBefore *int  `json:"messages_before,omitempty"`
	MessagesAfter  *int  `json:"messages_after,omitempty"`
	Compressed     *bool `json:"compressed,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *ContextUsageEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *ContextUsageEvent) EventType() string { return "system.context_usage.delta" }

// StateDeltaEvent carries a keyed update to the run's state.
type StateDeltaEvent struct {
	BaseEvent
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// Base returns the metadata embedded in this event.
func (e *StateDeltaEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *StateDeltaEvent) EventType() string { return "system.state.delta" }

// InterruptEvent is emitted when a run is interrupted, such as by cancellation or timeout.
type InterruptEvent struct {
	BaseEvent
	Reason             string `json:"reason"` // "user_canceled" | "timeout" | "max_depth"
	InterruptedEventID string `json:"interrupted_event_id,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *InterruptEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *InterruptEvent) EventType() string { return "system.interrupt" }

// AgentErrorEvent is emitted when a run encounters a recoverable or fatal error.
type AgentErrorEvent struct {
	BaseEvent
	ErrorMessage string `json:"error_message"`
	ErrorCode    string `json:"error_code,omitempty"`
	Recoverable  bool   `json:"recoverable"`
}

// Base returns the metadata embedded in this event.
func (e *AgentErrorEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *AgentErrorEvent) EventType() string { return "system.error" }

// ——— User Event (Reserved for Two-Way Communication) ———

// UserCommandEvent carries a user control command, such as pause, resume, or cancel.
type UserCommandEvent struct {
	BaseEvent
	Command string         `json:"command"` // "pause" | "resume" | "cancel"
	Payload map[string]any `json:"payload,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *UserCommandEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *UserCommandEvent) EventType() string { return "user.command" }

// UserFeedbackEvent carries user feedback, such as approval or rejection of a target event.
type UserFeedbackEvent struct {
	BaseEvent
	TargetEventID string `json:"target_event_id"`
	FeedbackType  string `json:"feedback_type"` // "approve" | "reject" | "modify"
	Payload       any    `json:"payload,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *UserFeedbackEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type discriminator for this event.
func (e *UserFeedbackEvent) EventType() string { return "user.feedback" }

// AgentOSEvent is the canonical event envelope accepted from external AgentOS backends.
type AgentOSEvent struct {
	BaseEvent
	Payload map[string]any `json:"payload,omitempty"`
}

// Base returns the metadata embedded in this event.
func (e *AgentOSEvent) Base() BaseEvent { return e.BaseEvent }

// EventType returns the event type carried by the embedded BaseEvent.
func (e *AgentOSEvent) EventType() string { return e.BaseEvent.EventType }
