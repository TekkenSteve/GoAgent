package entity

// ——— LLM Event ———

type TextDeltaEvent struct {
	BaseEvent
	Content string `json:"content"`
	Index   int    `json:"index"` // content block serial number, used when multiple blocks are interleaved
}

func (e TextDeltaEvent) Base() BaseEvent   { return e.BaseEvent }
func (e TextDeltaEvent) EventType() string { return "llm.text.delta" }

type ReasoningDeltaEvent struct {
	BaseEvent
	Reasoning string `json:"reasoning"`
}

func (e ReasoningDeltaEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ReasoningDeltaEvent) EventType() string { return "llm.reasoning.delta" }

type ToolCallStartEvent struct {
	BaseEvent
	ToolCallID    string `json:"tool_call_id"`
	ToolCallIndex int    `json:"tool_call_index"`
	ToolName      string `json:"tool_name"`
}

func (e ToolCallStartEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolCallStartEvent) EventType() string { return "llm.tool_call.start" }

type ToolCallDeltaEvent struct {
	BaseEvent
	ToolCallID     string `json:"tool_call_id"`
	ArgumentsDelta string `json:"arguments_delta"`
}

func (e ToolCallDeltaEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolCallDeltaEvent) EventType() string { return "llm.tool_call.delta" }

type ToolCallFinishEvent struct {
	BaseEvent
	ToolCallID   string `json:"tool_call_id"`
	ToolName     string `json:"tool_name"`
	Arguments    string `json:"arguments"`
	RawArguments string `json:"raw_arguments,omitempty"`
}

func (e ToolCallFinishEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolCallFinishEvent) EventType() string { return "llm.tool_call.finish" }

type UsageFinishEvent struct {
	BaseEvent
	Usage Usage  `json:"usage"`
	Model string `json:"model"`
}

func (e UsageFinishEvent) Base() BaseEvent   { return e.BaseEvent }
func (e UsageFinishEvent) EventType() string { return "llm.usage.finish" }

// ——— Tool execution event ———

type ToolExecStartEvent struct {
	BaseEvent
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"tool_name"`
}

func (e ToolExecStartEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolExecStartEvent) EventType() string { return "tool.execution.start" }

type ToolExecStdoutEvent struct {
	BaseEvent
	ToolCallID  string `json:"tool_call_id"`
	StdoutDelta string `json:"stdout_delta"`
}

func (e ToolExecStdoutEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolExecStdoutEvent) EventType() string { return "tool.execution.stdout" }

type ToolExecStderrEvent struct {
	BaseEvent
	ToolCallID  string `json:"tool_call_id"`
	StderrDelta string `json:"stderr_delta"`
}

func (e ToolExecStderrEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolExecStderrEvent) EventType() string { return "tool.execution.stderr" }

type ToolExecFinishEvent struct {
	BaseEvent
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"tool_name"`
	Output     string `json:"output,omitempty"`
	ExitCode   int    `json:"exit_code"`
	IsError    bool   `json:"is_error"`
	DurationMs int64  `json:"duration_ms,omitempty"`
}

func (e ToolExecFinishEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ToolExecFinishEvent) EventType() string { return "tool.execution.finish" }

// ——— Agent Event ———

type AgentRunStartEvent struct {
	BaseEvent
	AgentName    string `json:"agent_name"`
	InputSummary string `json:"input_summary,omitempty"`
}

func (e AgentRunStartEvent) Base() BaseEvent   { return e.BaseEvent }
func (e AgentRunStartEvent) EventType() string { return "agent.run.start" }

type AgentRunFinishEvent struct {
	BaseEvent
	FinishReason string `json:"finish_reason"`
	Usage        *Usage `json:"usage,omitempty"`
}

func (e AgentRunFinishEvent) Base() BaseEvent   { return e.BaseEvent }
func (e AgentRunFinishEvent) EventType() string { return "agent.run.finish" }

// ——— System event ———

type PrepStageEvent struct {
	BaseEvent
	Stage    string `json:"stage"`    // "initializing" | "ready" | "summarizing"
	Progress int    `json:"progress"` // 0-100
}

func (e PrepStageEvent) Base() BaseEvent   { return e.BaseEvent }
func (e PrepStageEvent) EventType() string { return "system.prep_stage.delta" }

type ContextUsageEvent struct {
	BaseEvent
	MessageCount   int   `json:"message_count"`
	MessagesBefore *int  `json:"messages_before,omitempty"`
	MessagesAfter  *int  `json:"messages_after,omitempty"`
	Compressed     *bool `json:"compressed,omitempty"`
}

func (e ContextUsageEvent) Base() BaseEvent   { return e.BaseEvent }
func (e ContextUsageEvent) EventType() string { return "system.context_usage.delta" }

type StateDeltaEvent struct {
	BaseEvent
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func (e StateDeltaEvent) Base() BaseEvent   { return e.BaseEvent }
func (e StateDeltaEvent) EventType() string { return "system.state.delta" }

type InterruptEvent struct {
	BaseEvent
	Reason             string `json:"reason"` // "user_cancelled" | "timeout" | "max_depth"
	InterruptedEventID string `json:"interrupted_event_id,omitempty"`
}

func (e InterruptEvent) Base() BaseEvent   { return e.BaseEvent }
func (e InterruptEvent) EventType() string { return "system.interrupt" }

type AgentErrorEvent struct {
	BaseEvent
	ErrorMessage string `json:"error_message"`
	ErrorCode    string `json:"error_code,omitempty"`
	Recoverable  bool   `json:"recoverable"`
}

func (e AgentErrorEvent) Base() BaseEvent   { return e.BaseEvent }
func (e AgentErrorEvent) EventType() string { return "system.error" }

// ——— User Event (Reserved for Two-Way Communication) ———

type UserCommandEvent struct {
	BaseEvent
	Command string         `json:"command"` // "pause" | "resume" | "cancel"
	Payload map[string]any `json:"payload,omitempty"`
}

func (e UserCommandEvent) Base() BaseEvent   { return e.BaseEvent }
func (e UserCommandEvent) EventType() string { return "user.command" }

type UserFeedbackEvent struct {
	BaseEvent
	TargetEventID string `json:"target_event_id"`
	FeedbackType  string `json:"feedback_type"` // "approve" | "reject" | "modify"
	Payload       any    `json:"payload,omitempty"`
}

func (e UserFeedbackEvent) Base() BaseEvent   { return e.BaseEvent }
func (e UserFeedbackEvent) EventType() string { return "user.feedback" }
