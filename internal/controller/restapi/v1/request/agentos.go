package request

import (
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// AgentOSStart starts a generic AgentOS run.
type AgentOSStart struct {
	RunID          string             `json:"run_id" validate:"required"`
	ThreadID       string             `json:"thread_id,omitempty"`
	AccountID      string             `json:"account_id" validate:"required"`
	ProjectID      string             `json:"project_id,omitempty"`
	AgentID        string             `json:"agent_id,omitempty"`
	ModelRef       string             `json:"model_ref,omitempty"`
	SystemPrompt   string             `json:"system_prompt,omitempty"`
	UserMessage    string             `json:"user_message,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time          `json:"requested_at,omitempty"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Backend        agentos.BackendRef `json:"backend" validate:"required"`
	Input          map[string]any     `json:"input,omitempty"`
}

// AgentOSSignal sends business input to a generic AgentOS run.
type AgentOSSignal struct {
	Type           agentos.SignalType `json:"type" validate:"required"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	Payload        map[string]any     `json:"payload,omitempty"`
	SentAt         time.Time          `json:"sent_at,omitempty"`
}

// AgentOSControl sends a lifecycle control operation to a generic AgentOS run.
type AgentOSControl struct {
	Operation agentos.ControlOperation `json:"operation" validate:"required"`
}
