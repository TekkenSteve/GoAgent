package entity

import (
	"regexp"
	"strings"
	"time"
)

// templateVarPattern matches {{variableName}} in prompt text.
var templateVarPattern = regexp.MustCompile(`\{\{(\w+)\}\}`)

// TriggerType enumerates the supported trigger sources.
type TriggerType string

const (
	TriggerSchedule TriggerType = "schedule"
	TriggerEvent    TriggerType = "event"
)

// TriggerSpec persists a trigger that schedules or event-fires a workflow.
type TriggerSpec struct {
	ID               string            `json:"id"`
	TemplateID       string            `json:"template_id"`
	AccountID        string            `json:"account_id"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	TriggerType      TriggerType       `json:"trigger_type"`
	CronExpression   string            `json:"cron_expression,omitempty"`
	EventSlug        string            `json:"event_slug,omitempty"`
	AgentPrompt      string            `json:"agent_prompt"`
	Config           map[string]any    `json:"config,omitempty"`
	TemplateVars     []string          `json:"template_vars,omitempty"`
	TemplateVarsVals map[string]string `json:"template_vars_vals,omitempty"`
	IsActive         bool              `json:"is_active"`
	LastFiredAt      *time.Time        `json:"last_fired_at,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// ExtractTemplateVars extracts unique {{variable}} names from a text string.
// Returns the variable names (without braces) in order of first occurrence.
func ExtractTemplateVars(text string) []string {
	matches := templateVarPattern.FindAllStringSubmatch(text, -1)
	seen := make(map[string]struct{}, len(matches))

	vars := make([]string, 0, len(matches))
	for _, m := range matches {
		name := m[1]
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			vars = append(vars, name)
		}
	}

	return vars
}

// ResolvePrompt returns the agent prompt with {{variable}} placeholders replaced
// by their values in TemplateVarsVals. Placeholders without a matching value are
// left unchanged.
func (s *TriggerSpec) ResolvePrompt() string {
	prompt := s.AgentPrompt
	for k, v := range s.TemplateVarsVals {
		prompt = strings.ReplaceAll(prompt, "{{"+k+"}}", v)
	}

	return prompt
}

// TriggerEventLog records a trigger fire for audit purposes.
type TriggerEventLog struct {
	ID            string            `json:"id"`
	TriggerID     string            `json:"trigger_id"`
	TemplateID    string            `json:"template_id"`
	TriggerType   TriggerType       `json:"trigger_type"`
	Success       bool              `json:"success"`
	Message       string            `json:"message"`
	EventData     string            `json:"event_data,omitempty"`
	AgentPrompt   string            `json:"agent_prompt,omitempty"`
	ExecVariables map[string]string `json:"exec_variables,omitempty"`
	FiredAt       time.Time         `json:"fired_at"`
	CreatedAt     time.Time         `json:"created_at"`
}

// CreateTriggerRequest is the input for creating a new trigger.
type CreateTriggerRequest struct {
	TemplateID       string            `json:"template_id"`
	AccountID        string            `json:"account_id"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	TriggerType      TriggerType       `json:"trigger_type"`
	CronExpression   string            `json:"cron_expression,omitempty"`
	EventSlug        string            `json:"event_slug,omitempty"`
	AgentPrompt      string            `json:"agent_prompt"`
	Config           map[string]any    `json:"config,omitempty"`
	TemplateVars     []string          `json:"template_vars,omitempty"`
	TemplateVarsVals map[string]string `json:"template_vars_vals,omitempty"`
	IsActive         bool              `json:"is_active"`
}

// TriggerFireResult is the resolved data returned when an event trigger fires.
type TriggerFireResult struct {
	TriggerID    string    `json:"trigger_id"`
	RunID        string    `json:"run_id"`
	Name         string    `json:"name"`
	SystemPrompt string    `json:"-"`
	Message      string    `json:"-"`
	ModelRef     string    `json:"-"`
	FiredAt      time.Time `json:"fired_at"`
}

// UpdateTriggerRequest is the input for updating a trigger.
type UpdateTriggerRequest struct {
	Name             *string            `json:"name,omitempty"`
	Description      *string            `json:"description,omitempty"`
	CronExpression   *string            `json:"cron_expression,omitempty"`
	EventSlug        *string            `json:"event_slug,omitempty"`
	AgentPrompt      *string            `json:"agent_prompt,omitempty"`
	Config           *map[string]any    `json:"config,omitempty"`
	TemplateVars     *[]string          `json:"template_vars,omitempty"`
	TemplateVarsVals *map[string]string `json:"template_vars_vals,omitempty"`
	IsActive         *bool              `json:"is_active,omitempty"`
}
