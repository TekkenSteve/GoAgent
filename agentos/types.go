package agentos

import (
	"encoding/json"
	"time"
)

// RunSpec describes a generic agent run without binding callers to GoAgent internals.
type RunSpec struct {
	RunID          string
	ThreadID       string
	AccountID      string
	ProjectID      string
	AgentID        string
	ModelRef       string
	SystemPrompt   string
	UserMessage    string
	IdempotencyKey string
	RequestedAt    time.Time
	Metadata       map[string]string
}

// RunStatus is the public lifecycle view for a run.
type RunStatus struct {
	RunID          string
	LifecycleState string
	Step           int32
	Reason         string
	UpdatedAt      time.Time
}

// Event is the public stream event envelope.
type Event struct {
	EventID   string
	EventType string
	RunID     string
	ThreadID  string
	Sequence  int64
	Timestamp time.Time
	Payload   map[string]any
}

// Message is a public conversation message.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

// ToolCall describes a model-requested tool invocation.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction contains function-call details.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDef describes a function/tool available to a model.
type ToolDef struct {
	Type     string      `json:"type"`
	Function ToolFuncDef `json:"function"`
}

// ToolFuncDef is the function definition inside a tool.
type ToolFuncDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// StreamScope selects events for a run/thread.
type StreamScope struct {
	RunID         string
	ThreadID      string
	AfterSequence int64
}
