package entity

import "time"

// WorkflowTemplate is a persisted workflow definition that can be loaded
// and executed by OrchestrationWorkflow.
type WorkflowTemplate struct {
	ID           string    `json:"id"`
	AccountID    string    `json:"account_id"`
	Name         string    `json:"name"`
	Description  string    `json:"description,omitempty"`
	TeamSpec     TeamSpec  `json:"team_spec"`
	SystemPrompt string    `json:"system_prompt,omitempty"`
	DefaultModel string    `json:"default_model,omitempty"`
	Tags         []string  `json:"tags,omitempty"`
	IsEnabled    bool      `json:"is_enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// CreateWorkflowTemplateRequest is the input for creating a new template.
type CreateWorkflowTemplateRequest struct {
	AccountID    string   `json:"account_id"`
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	TeamSpec     TeamSpec `json:"team_spec"`
	SystemPrompt string   `json:"system_prompt,omitempty"`
	DefaultModel string   `json:"default_model,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	IsEnabled    bool     `json:"is_enabled"`
}

// UpdateWorkflowTemplateRequest is the input for updating a template.
type UpdateWorkflowTemplateRequest struct {
	Name         *string   `json:"name,omitempty"`
	Description  *string   `json:"description,omitempty"`
	TeamSpec     *TeamSpec `json:"team_spec,omitempty"`
	SystemPrompt *string   `json:"system_prompt,omitempty"`
	DefaultModel *string   `json:"default_model,omitempty"`
	Tags         *[]string `json:"tags,omitempty"`
	IsEnabled    *bool     `json:"is_enabled,omitempty"`
}
