package temporalexternal

import (
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// StartInput is the stable cross-language workflow input sent to external Temporal backends.
type StartInput struct {
	RunID          string             `json:"run_id"`
	ThreadID       string             `json:"thread_id,omitempty"`
	AccountID      string             `json:"account_id,omitempty"`
	ProjectID      string             `json:"project_id,omitempty"`
	AgentID        string             `json:"agent_id,omitempty"`
	ModelRef       string             `json:"model_ref,omitempty"`
	SystemPrompt   string             `json:"system_prompt,omitempty"`
	UserMessage    string             `json:"user_message,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time          `json:"requested_at"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Backend        agentos.BackendRef `json:"backend"`
	Input          map[string]any     `json:"input,omitempty"`
}

// SignalInput is the stable signal payload sent to external Temporal backends.
type SignalInput struct {
	Type           agentos.SignalType `json:"type"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	Payload        map[string]any     `json:"payload,omitempty"`
	SentAt         time.Time          `json:"sent_at"`
}

func startInputFromSpec(spec agentos.RunSpec) StartInput {
	requestedAt := spec.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}

	return StartInput{
		RunID:          spec.RunID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		AgentID:        spec.AgentID,
		ModelRef:       spec.ModelRef,
		SystemPrompt:   spec.SystemPrompt,
		UserMessage:    spec.UserMessage,
		IdempotencyKey: spec.IdempotencyKey,
		RequestedAt:    requestedAt,
		Metadata:       spec.Metadata,
		Backend:        spec.Backend,
		Input:          spec.Input,
	}
}

func signalInputFromSignal(signal agentos.Signal) SignalInput {
	sentAt := signal.SentAt
	if sentAt.IsZero() {
		sentAt = time.Now().UTC()
	}

	return SignalInput{
		Type:           signal.Type,
		IdempotencyKey: signal.IdempotencyKey,
		Payload:        signal.Payload,
		SentAt:         sentAt,
	}
}
