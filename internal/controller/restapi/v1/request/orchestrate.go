package request

import "encoding/json"

// Orchestrate -.
type Orchestrate struct {
	RunID          string          `json:"run_id"            validate:"required" example:"run-550e8400-e29b-41d4-a716-446655440000"`
	AccountID      string          `json:"account_id"        validate:"required" example:"acct-001"`
	TeamSpec       json.RawMessage `json:"team_spec,omitempty" swaggertype:"object"`
	Steps          json.RawMessage `json:"steps,omitempty" swaggertype:"array,object"`
	SystemPrompt   string          `json:"system_prompt,omitempty"`
	Message        string          `json:"message,omitempty"`
	MaxDepth       int             `json:"max_depth,omitempty"`
	ContinuePolicy json.RawMessage `json:"continue_policy,omitempty" swaggertype:"object"`
}
