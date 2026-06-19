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
	ActorID        string             `json:"actor_id,omitempty"`
	Payload        map[string]any     `json:"payload,omitempty"`
	SentAt         time.Time          `json:"sent_at,omitempty"`
}

// AgentOSControl sends a lifecycle control operation to a generic AgentOS run.
type AgentOSControl struct {
	Operation      agentos.ControlOperation `json:"operation" validate:"required"`
	IdempotencyKey string                   `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                `json:"requested_at,omitempty"`
	ActorID        string                   `json:"actor_id,omitempty"`
	Metadata       map[string]string        `json:"metadata,omitempty"`
}

// AgentOSPlanSignal sends business input to a RunPlan inside tenant scope.
type AgentOSPlanSignal struct {
	Type           agentos.SignalType `json:"type" validate:"required"`
	AccountID      string             `json:"account_id" validate:"required"`
	ProjectID      string             `json:"project_id,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	ActorID        string             `json:"actor_id,omitempty"`
	Payload        map[string]any     `json:"payload,omitempty"`
	SentAt         time.Time          `json:"sent_at,omitempty"`
}

// AgentOSPlanControl sends a lifecycle control operation to a RunPlan inside tenant scope.
type AgentOSPlanControl struct {
	Operation      agentos.ControlOperation `json:"operation" validate:"required"`
	AccountID      string                   `json:"account_id" validate:"required"`
	ProjectID      string                   `json:"project_id,omitempty"`
	IdempotencyKey string                   `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                `json:"requested_at,omitempty"`
	ActorID        string                   `json:"actor_id,omitempty"`
	Metadata       map[string]string        `json:"metadata,omitempty"`
}

// AgentOSPlanStreamScope selects plan events for REST streaming.
type AgentOSPlanStreamScope struct {
	AccountID     string `query:"account_id" validate:"required"`
	ProjectID     string `query:"project_id"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
}

// AgentOSPlanScope selects one plan inside a tenant boundary.
type AgentOSPlanScope struct {
	AccountID string `query:"account_id" validate:"required"`
	ProjectID string `query:"project_id"`
}

// AgentOSEvent is the public REST envelope for external backend event ingest.
type AgentOSEvent struct {
	EventID   string            `json:"event_id" validate:"required"`
	RunID     string            `json:"run_id,omitempty"`
	ThreadID  string            `json:"thread_id,omitempty"`
	Sequence  int64             `json:"sequence,omitempty"`
	EventType agentos.EventType `json:"event_type" validate:"required"`
	Source    string            `json:"source" validate:"required"`
	Timestamp time.Time         `json:"timestamp,omitempty"`
	TraceID   string            `json:"trace_id,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Payload   map[string]any    `json:"payload,omitempty"`
}
