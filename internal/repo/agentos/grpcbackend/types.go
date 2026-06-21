package grpcbackend

import (
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

type startRequest struct {
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

type signalRequest struct {
	RunID          string             `json:"run_id"`
	Type           agentos.SignalType `json:"type"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	Payload        map[string]any     `json:"payload,omitempty"`
	SentAt         time.Time          `json:"sent_at"`
}

type controlRequest struct {
	RunID          string                   `json:"run_id"`
	Operation      agentos.ControlOperation `json:"operation"`
	IdempotencyKey string                   `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                `json:"requested_at"`
}

type statusRequest struct {
	RunID string `json:"run_id"`
}

type emptyResponse struct{}

func startRequestFromSpec(spec agentos.RunSpec) startRequest {
	requestedAt := spec.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}

	return startRequest{
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

func signalRequestFromSignal(runID string, signal agentos.Signal) signalRequest {
	sentAt := signal.SentAt
	if sentAt.IsZero() {
		sentAt = time.Now().UTC()
	}

	return signalRequest{
		RunID:          runID,
		Type:           signal.Type,
		IdempotencyKey: signal.IdempotencyKey,
		Payload:        signal.Payload,
		SentAt:         sentAt,
	}
}

func controlRequestFromControl(runID string, control agentos.ControlRequest) controlRequest {
	requestedAt := control.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}

	return controlRequest{
		RunID:          runID,
		Operation:      control.Operation,
		IdempotencyKey: control.IdempotencyKey,
		RequestedAt:    requestedAt,
	}
}
