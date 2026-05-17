package client

import "encoding/json"

// ─── Request types ───────────────────────────────────────────────────────────────

// ExecuteRequest is the payload for POST /v1/agent/execute.
// Only RunID, AccountID, and UserMessage are required; AgentID is optional.
type ExecuteRequest struct {
	RunID       string `json:"run_id"`
	AccountID   string `json:"account_id"`
	AgentID     string `json:"agent_id,omitempty"`
	UserMessage string `json:"user_message"`
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

// MessagesResponse wraps a paginated list of message records.
type MessagesResponse struct {
	Data   []json.RawMessage `json:"data"`
	Limit  uint64            `json:"limit"`
	Offset uint64            `json:"offset"`
}
