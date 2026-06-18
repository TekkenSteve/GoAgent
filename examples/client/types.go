package client

// ─── Request types ───────────────────────────────────────────────────────────────

// AgentOSRunRequest is the payload for POST /v1/agentos/runs.
type AgentOSRunRequest struct {
	RunID          string            `json:"run_id"`
	ThreadID       string            `json:"thread_id,omitempty"`
	AccountID      string            `json:"account_id"`
	ProjectID      string            `json:"project_id,omitempty"`
	AgentID        string            `json:"agent_id,omitempty"`
	ModelRef       string            `json:"model_ref,omitempty"`
	SystemPrompt   string            `json:"system_prompt,omitempty"`
	UserMessage    string            `json:"user_message,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Backend        BackendRef        `json:"backend"`
	Input          map[string]any    `json:"input,omitempty"`
}

// BackendRef selects the backend that owns an AgentOS run.
type BackendRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// AgentOSSignalRequest is the payload for POST /v1/agentos/runs/{run_id}/signals.
type AgentOSSignalRequest struct {
	Type           string         `json:"type"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
}

// AgentOSControlRequest is the payload for POST /v1/agentos/runs/{run_id}/control.
type AgentOSControlRequest struct {
	Operation      string            `json:"operation"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	ActorID        string            `json:"actor_id,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// OrchestrationRequest is the payload for POST /v1/orchestration/execute.
// Use TeamSpec or Steps (not both).
type OrchestrationRequest struct {
	RunID     string `json:"run_id"`
	AccountID string `json:"account_id"`
	TeamSpec  any    `json:"team_spec,omitempty"` // map[string]any describing entity.TeamSpec
	Steps     any    `json:"steps,omitempty"`     // []any describing []entity.Step
}

// ─── Response types ──────────────────────────────────────────────────────────────

// RunStatus is the standard response for execution and status endpoints.
type RunStatus struct {
	RunID          string `json:"run_id"`
	LifecycleState string `json:"lifecycle_state"`
	Step           int    `json:"step,omitempty"`
	Reason         string `json:"reason,omitempty"`
	UpdatedAt      int64  `json:"updated_at,omitempty"`
}
