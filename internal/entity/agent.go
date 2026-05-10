package entity

import "time"

// AgentRecord is the persistent model for an agent definition in the database.
type AgentRecord struct {
	AgentID        string    `json:"agent_id"`
	AccountID      string    `json:"account_id"`
	Name           string    `json:"name"`
	Description    string    `json:"description,omitempty"`
	SystemPrompt   string    `json:"system_prompt"`
	ModelRef       string    `json:"model_ref"`
	Config         LLMConfig `json:"config"`
	CurrentVersion string    `json:"current_version,omitempty"`
	IsDefault      bool      `json:"is_default"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// AgentVersionRecord is the persistent model for an agent version snapshot.
type AgentVersionRecord struct {
	VersionID        string         `json:"version_id"`
	AgentID          string         `json:"agent_id"`
	VersionName      string         `json:"version_name"`
	SystemPrompt     string         `json:"system_prompt"`
	ModelRef         string         `json:"model_ref"`
	Config           LLMConfig      `json:"config"`
	ToolBindings     []ToolBinding  `json:"tool_bindings,omitempty"`
	ChangeDescription string        `json:"change_description,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

// CreateAgentRequest is the input for creating a new agent.
type CreateAgentRequest struct {
	AccountID    string        `json:"account_id"`
	Name         string        `json:"name"`
	Description  string        `json:"description,omitempty"`
	SystemPrompt string        `json:"system_prompt"`
	ModelRef     string        `json:"model_ref"`
	Config       LLMConfig     `json:"config"`
	ToolBindings []ToolBinding `json:"tool_bindings,omitempty"`
}

// UpdateAgentRequest is the input for updating an existing agent.
type UpdateAgentRequest struct {
	Name           *string       `json:"name,omitempty"`
	Description    *string       `json:"description,omitempty"`
	SystemPrompt   *string       `json:"system_prompt,omitempty"`
	ModelRef       *string       `json:"model_ref,omitempty"`
	Config         *LLMConfig    `json:"config,omitempty"`
	IsDefault      *bool         `json:"is_default,omitempty"`
	CurrentVersion *string       `json:"current_version,omitempty"`
}
