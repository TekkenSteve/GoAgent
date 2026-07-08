package control

import (
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// RunSpec describes a generic agent run without binding callers to GoAgent internals.
type RunSpec struct {
	RunID          string            `json:"run_id"`
	ThreadID       string            `json:"thread_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	ProjectID      string            `json:"project_id,omitempty"`
	AgentID        string            `json:"agent_id,omitempty"`
	ModelRef       string            `json:"model_ref,omitempty"`
	SystemPrompt   string            `json:"system_prompt,omitempty"`
	UserMessage    string            `json:"user_message,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitzero" schema:"optional"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Backend        BackendRef        `json:"backend"`
	Input          map[string]any    `json:"input,omitempty"`
}

// BackendKind identifies the execution substrate used by an agent backend.
type BackendKind string

const (
	BackendKindNative           BackendKind = "native"
	BackendKindTemporalExternal BackendKind = "temporal_external"
	BackendKindHTTP             BackendKind = "http"
	BackendKindGRPC             BackendKind = "grpc"
)

const (
	// BackendNameGoAgentNative is the built-in GoAgent native backend.
	BackendNameGoAgentNative = "goagent-native"
)

const (
	// CapabilityRun is the standard capability for starting one backend-owned run.
	CapabilityRun = "run"
	// CapabilityRunBatch is the standard capability for starting one backend-owned
	// run that internally owns a bounded batch.
	CapabilityRunBatch = "run.batch"
)

// BackendRef selects the backend that owns a run.
type BackendRef struct {
	Kind BackendKind `json:"kind"`
	Name string      `json:"name"`
}

// RunStatus is the public lifecycle view for a run.
type RunStatus struct {
	RunID          string             `json:"run_id"`
	LifecycleState string             `json:"lifecycle_state"`
	Progress       *core.RunProgress  `json:"progress,omitempty"`
	Artifacts      []core.ArtifactRef `json:"artifacts,omitempty"`
	BudgetUsage    PlanBudgetUsage    `json:"budget_usage,omitzero" schema:"optional"`
	Reason         string             `json:"reason,omitempty"`
	UpdatedAt      time.Time          `json:"updated_at,omitzero" schema:"optional"`
}

// RunBackendOwnership is the durable routing record for a backend-owned run.
type RunBackendOwnership struct {
	RunID          string     `json:"run_id"`
	PlanID         string     `json:"plan_id,omitempty"`
	NodeID         string     `json:"node_id,omitempty"`
	ThreadID       string     `json:"thread_id,omitempty"`
	AccountID      string     `json:"account_id,omitempty"`
	ProjectID      string     `json:"project_id,omitempty"`
	Backend        BackendRef `json:"backend"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	LifecycleState string     `json:"lifecycle_state,omitempty"`
	CreatedAt      time.Time  `json:"created_at,omitzero" schema:"optional"`
	UpdatedAt      time.Time  `json:"updated_at,omitzero" schema:"optional"`
}
