package entity

import "time"

// ——— Three-dimensional orthogonal enumeration ———

type EventSource string

const (
	SourceLLM    EventSource = "llm"
	SourceTool   EventSource = "tool"
	SourceAgent  EventSource = "agent"
	SourceSystem EventSource = "system"
	SourceUser   EventSource = "user"
)

type EventPhase string

const (
	PhaseRequest   EventPhase = "request"
	PhaseStart     EventPhase = "start"
	PhaseDelta     EventPhase = "delta"
	PhaseFinish    EventPhase = "finish"
	PhaseError     EventPhase = "error"
	PhaseInterrupt EventPhase = "interrupt"
)

type EventContentType string

const (
	ContentText       EventContentType = "text"
	ContentReasoning  EventContentType = "reasoning"
	ContentToolCall   EventContentType = "tool_call"
	ContentToolResult EventContentType = "tool_result"
	ContentState      EventContentType = "state"
	ContentStatus     EventContentType = "status"
	ContentCommand    EventContentType = "command"
	ContentFeedback   EventContentType = "feedback"
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
// TODO(phase2): 迁移到 repo/webapi/types.go
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
	RunID        string
	SystemPrompt string
	Message      string
	History      []Message
	Tools        []ToolDef
	Config       LLMConfig
}
