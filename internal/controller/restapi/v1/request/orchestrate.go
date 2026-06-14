package request

import "github.com/TekkenSteve/GoAgent/internal/entity"

// Orchestrate -.
type Orchestrate struct {
	RunID          string                `json:"run_id"            validate:"required" example:"run-550e8400-e29b-41d4-a716-446655440000"`
	AccountID      string                `json:"account_id"        validate:"required" example:"acct-001"`
	TeamSpec       *entity.TeamSpec      `json:"team_spec,omitempty"`
	Steps          []entity.Step         `json:"steps,omitempty"`
	SystemPrompt   string                `json:"system_prompt,omitempty"`
	Message        string                `json:"message,omitempty"`
	MaxDepth       int                   `json:"max_depth,omitempty"`
	ContinuePolicy entity.ContinuePolicy `json:"continue_policy"`
}
