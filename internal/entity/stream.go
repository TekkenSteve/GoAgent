package entity

import "time"

// ——— Three-dimensional orthogonal enumeration ———

// EventSource identifies the subsystem that produced an event.
type EventSource string

const (
	// SourceLLM marks an event emitted by the LLM layer.
	SourceLLM EventSource = "llm"
	// SourceTool marks an event emitted by tool execution.
	SourceTool EventSource = "tool"
	// SourceAgent marks an event emitted by agent orchestration.
	SourceAgent EventSource = "agent"
	// SourceSystem marks an event emitted by the system itself.
	SourceSystem EventSource = "system"
	// SourceUser marks an event originating from the user.
	SourceUser EventSource = "user"
)

// EventPhase identifies the lifecycle phase of an event.
type EventPhase string

const (
	// PhaseRequest marks the request phase of an event lifecycle.
	PhaseRequest EventPhase = "request"
	// PhaseStart marks the start phase of an event lifecycle.
	PhaseStart EventPhase = "start"
	// PhaseDelta marks the incremental update phase of an event lifecycle.
	PhaseDelta EventPhase = "delta"
	// PhaseFinish marks the completion phase of an event lifecycle.
	PhaseFinish EventPhase = "finish"
	// PhaseError marks the failure phase of an event lifecycle.
	PhaseError EventPhase = "error"
	// PhaseInterrupt marks the interruption phase of an event lifecycle.
	PhaseInterrupt EventPhase = "interrupt"
)

// EventContentType identifies the kind of payload an event carries.
type EventContentType string

const (
	// ContentText is the content type for text output.
	ContentText EventContentType = "text"
	// ContentReasoning is the content type for reasoning output.
	ContentReasoning EventContentType = "reasoning"
	// ContentToolCall is the content type for tool call data.
	ContentToolCall EventContentType = "tool_call"
	// ContentToolResult is the content type for tool execution results.
	ContentToolResult EventContentType = "tool_result"
	// ContentState is the content type for state updates.
	ContentState EventContentType = "state"
	// ContentStatus is the content type for status updates.
	ContentStatus EventContentType = "status"
	// ContentCommand is the content type for user commands.
	ContentCommand EventContentType = "command"
	// ContentFeedback is the content type for user feedback.
	ContentFeedback EventContentType = "feedback"
)

// BaseEvent is the common metadata for all events.
type BaseEvent struct {
	EventType   string            `json:"event_type"`
	Source      EventSource       `json:"source"`
	Phase       EventPhase        `json:"phase"`
	ContentType EventContentType  `json:"content_type"`
	SessionID   string            `json:"session_id,omitempty"`
	RunID       string            `json:"run_id,omitempty"`
	EventID     string            `json:"event_id,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
	TraceID     string            `json:"trace_id,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// StreamEvent is the core interface of the event system.
// Open for implementation outside the package (for test mocks, plugin extensions),
// but deserialization security is guaranteed by EventRegistry — unregistered types cannot be restored from JSON.
type StreamEvent interface {
	Base() BaseEvent
	EventType() string
}

// ToolCallDelta is an incremental fragment of a single tool call's parameters.
// It belongs to the internal type at the repo layer and is defined in the entity due to embedding LLMStreamChunk.
type ToolCallDelta struct {
	Index      int    `json:"index"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	Name       string `json:"name,omitempty"`
	ArgsDelta  string `json:"args_delta,omitempty"`
}

// LLMStreamChunk is a single chunk in an LLM streaming response.
type LLMStreamChunk struct {
	Content        string
	Reasoning      string
	ToolCalls      []ToolCall
	ToolCallDeltas []ToolCallDelta
	FinishReason   FinishReason
	Usage          Usage
}

// StreamRequest is the request parameters executed by the streaming Agent.
type StreamRequest struct {
	RunID            string
	AccountID        string
	SystemPrompt     string
	Message          string
	History          []Message
	Tools            []ToolDef
	Config           LLMConfig
	MCPServerConfigs []MCPServerConfig
}
