package toolkit

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

var (
	ErrTriggerToolNameRequired        = errors.New("trigger_tool: name is required")
	ErrTriggerToolCronRequired        = errors.New("trigger_tool: cron_expression is required")
	ErrTriggerToolTemplateIDRequired  = errors.New("trigger_tool: template_id is required")
	ErrListTriggersTemplateIDRequired = errors.New("list_triggers_tool: template_id is required")
	ErrToggleTriggerIDRequired        = errors.New("toggle_trigger_tool: trigger_id is required")
	ErrDeleteTriggerIDRequired        = errors.New("delete_trigger_tool: trigger_id is required")
)

// ——— Callback types (wired by app.go) ———

// TriggerCreatorFn creates a trigger record and returns the trigger ID.
type TriggerCreatorFn func(ctx context.Context, templateID, name, cronExpression, agentPrompt string, templateVars []string, templateVarsVals map[string]string) (string, error)

// TriggerScheduleFn schedules a trigger with Temporal after creation.
type TriggerScheduleFn func(ctx context.Context, triggerID, cronExpression string) error

// TriggerListerFn returns triggers for a given template.
type TriggerListerFn func(ctx context.Context, templateID string) ([]entity.TriggerSpec, error)

// TriggerToggleFn enables or disables a trigger.
type TriggerToggleFn func(ctx context.Context, triggerID string, isActive bool) (entity.TriggerSpec, error)

// TriggerDeleterFn removes a trigger.
type TriggerDeleterFn func(ctx context.Context, triggerID string) error

// ——— create_trigger tool ———

// TriggerTool allows an agent to create scheduled triggers.
// Kept as the original name for backwards compatibility.
type TriggerTool struct {
	meta      ToolMeta
	creator   TriggerCreatorFn
	scheduler TriggerScheduleFn
}

// NewTriggerTool creates a tool for agents to create scheduled triggers.
func NewTriggerTool(creator TriggerCreatorFn, scheduler TriggerScheduleFn) *TriggerTool {
	return &TriggerTool{
		meta: ToolMeta{
			Name:        "create_trigger",
			Description: "Create a scheduled trigger for recurring agent tasks. The agent will run automatically at the specified cron schedule. Use {{variable_name}} syntax in the prompt to create reusable template variables.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type":        "string",
						"description": "Name for the scheduled trigger",
					},
					"cron_expression": map[string]any{
						"type":        "string",
						"description": "Cron expression defining the schedule (e.g., '0 9 * * *' for daily at 9am, '*/30 * * * *' for every 30 minutes)",
					},
					"agent_prompt": map[string]any{
						"type":        "string",
						"description": "Prompt to send to the agent when the trigger fires. Use {{variable_name}} syntax for reusable template variables.",
					},
					"template_id": map[string]any{
						"type":        "string",
						"description": "ID of the workflow template to execute",
					},
					"variable_values": map[string]any{
						"type":        "object",
						"description": "Optional values for {{variable_name}} placeholders in the prompt. Keys are variable names, values are what they will be replaced with at fire time.",
						"additionalProperties": map[string]any{
							"type": "string",
						},
					},
				},
				"required": []string{"name", "cron_expression", "agent_prompt", "template_id"},
			},
		},
		creator:   creator,
		scheduler: scheduler,
	}
}

// Meta returns the tool metadata for LLM function calling.
func (t *TriggerTool) Meta() ToolMeta {
	return t.meta
}

// Execute creates a scheduled trigger.
func (t *TriggerTool) Execute(ctx context.Context, args map[string]any) (any, error) {
	name := getStringArg(args, "name")
	cronExpr := getStringArg(args, "cron_expression")
	agentPrompt := getStringArg(args, "agent_prompt")
	templateID := getStringArg(args, "template_id")

	if name == "" {
		return nil, ErrTriggerToolNameRequired
	}

	if cronExpr == "" {
		return nil, ErrTriggerToolCronRequired
	}

	if templateID == "" {
		return nil, ErrTriggerToolTemplateIDRequired
	}

	// Extract {{variable}} patterns from the prompt
	templateVars := entity.ExtractTemplateVars(agentPrompt)

	templateVarsVals := extractTemplateVarValues(args)

	triggerID, err := t.creator(ctx, templateID, name, cronExpr, agentPrompt, templateVars, templateVarsVals)
	if err != nil {
		return nil, fmt.Errorf("trigger_tool - create: %w", err)
	}

	// Schedule with Temporal
	if err := t.scheduler(ctx, triggerID, cronExpr); err != nil {
		return nil, fmt.Errorf("trigger_tool - schedule: %w", err)
	}

	result := map[string]any{
		"trigger_id": triggerID,
		"name":       name,
		"schedule":   cronExpr,
		"status":     "scheduled",
	}
	if len(templateVars) > 0 {
		result["template_variables"] = templateVars
	}

	if len(templateVarsVals) > 0 {
		result["variable_values"] = templateVarsVals
	}

	return result, nil
}

// extractTemplateVarValues extracts variable_values from args into a string map.
func extractTemplateVarValues(args map[string]any) map[string]string {
	result := make(map[string]string)

	if rawVals, ok := args["variable_values"].(map[string]any); ok {
		for k, v := range rawVals {
			if strVal, ok := v.(string); ok {
				result[k] = strVal
			}
		}
	}

	return result
}

// ——— list_triggers tool ———

// ListTriggersTool lists triggers for a given template.
type ListTriggersTool struct {
	meta   ToolMeta
	lister TriggerListerFn
}

// NewListTriggersTool creates a tool that lists triggers by template.
func NewListTriggersTool(lister TriggerListerFn) *ListTriggersTool {
	return &ListTriggersTool{
		meta: ToolMeta{
			Name:        "list_triggers",
			Description: "List all scheduled triggers for a workflow template. Use this to see existing triggers, their status, and schedule.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"template_id": map[string]any{
						"type":        "string",
						"description": "ID of the workflow template to list triggers for",
					},
				},
				"required": []string{"template_id"},
			},
		},
		lister: lister,
	}
}

// Meta returns the tool metadata.
func (t *ListTriggersTool) Meta() ToolMeta {
	return t.meta
}

// Execute lists triggers for the given template.
func (t *ListTriggersTool) Execute(ctx context.Context, args map[string]any) (any, error) {
	templateID, ok := args["template_id"].(string)
	if !ok {
		templateID = ""
	}

	if templateID == "" {
		return nil, ErrListTriggersTemplateIDRequired
	}

	triggers, err := t.lister(ctx, templateID)
	if err != nil {
		return nil, fmt.Errorf("list_triggers_tool - list: %w", err)
	}

	type triggerView struct {
		ID           string   `json:"id"`
		Name         string   `json:"name"`
		Schedule     string   `json:"schedule"`
		Prompt       string   `json:"prompt"`
		IsActive     bool     `json:"is_active"`
		LastFiredAt  string   `json:"last_fired_at,omitempty"`
		TemplateVars []string `json:"template_variables,omitempty"`
	}

	items := make([]triggerView, 0, len(triggers))
	for i := range triggers {
		tr := triggers[i]

		view := triggerView{
			ID:           tr.ID,
			Name:         tr.Name,
			Schedule:     tr.CronExpression,
			Prompt:       tr.AgentPrompt,
			IsActive:     tr.IsActive,
			TemplateVars: tr.TemplateVars,
		}
		if tr.LastFiredAt != nil {
			view.LastFiredAt = tr.LastFiredAt.Format(timeRfc3339)
		}

		items = append(items, view)
	}

	return map[string]any{
		"triggers": items,
		"total":    len(items),
	}, nil
}

// ——— toggle_trigger tool ———

// ToggleTriggerTool enables or disables a trigger.
type ToggleTriggerTool struct {
	meta   ToolMeta
	toggle TriggerToggleFn
}

// NewToggleTriggerTool creates a tool for enabling/disabling triggers.
func NewToggleTriggerTool(toggle TriggerToggleFn) *ToggleTriggerTool {
	return &ToggleTriggerTool{
		meta: ToolMeta{
			Name:        "toggle_trigger",
			Description: "Enable or disable a scheduled trigger. Disabled triggers won't run until re-enabled.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"trigger_id": map[string]any{
						"type":        "string",
						"description": "ID of the trigger to enable or disable",
					},
					"is_active": map[string]any{
						"type":        "boolean",
						"description": "true to enable the trigger, false to disable it",
					},
				},
				"required": []string{"trigger_id", "is_active"},
			},
		},
		toggle: toggle,
	}
}

// Meta returns the tool metadata.
func (t *ToggleTriggerTool) Meta() ToolMeta {
	return t.meta
}

// Execute toggles the trigger's active state.
func (t *ToggleTriggerTool) Execute(ctx context.Context, args map[string]any) (any, error) {
	triggerID, ok := args["trigger_id"].(string)
	if !ok {
		return nil, ErrToggleTriggerIDRequired
	}

	isActive, ok := args["is_active"].(bool)
	if !ok {
		isActive = false
	}

	if triggerID == "" {
		return nil, ErrToggleTriggerIDRequired
	}

	updated, err := t.toggle(ctx, triggerID, isActive)
	if err != nil {
		return nil, fmt.Errorf("toggle_trigger_tool - toggle: %w", err)
	}

	status := "enabled"
	if !isActive {
		status = "disabled"
	}

	return map[string]any{
		"trigger_id": updated.ID,
		"name":       updated.Name,
		"status":     status,
		"is_active":  updated.IsActive,
	}, nil
}

// ——— delete_trigger tool ———

// DeleteTriggerTool removes a trigger.
type DeleteTriggerTool struct {
	meta    ToolMeta
	deleter TriggerDeleterFn
}

// NewDeleteTriggerTool creates a tool for deleting triggers.
func NewDeleteTriggerTool(deleter TriggerDeleterFn) *DeleteTriggerTool {
	return &DeleteTriggerTool{
		meta: ToolMeta{
			Name:        "delete_trigger",
			Description: "Delete a scheduled trigger. The agent will no longer run automatically on the schedule. This cannot be undone.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"trigger_id": map[string]any{
						"type":        "string",
						"description": "ID of the trigger to delete",
					},
				},
				"required": []string{"trigger_id"},
			},
		},
		deleter: deleter,
	}
}

// Meta returns the tool metadata.
func (t *DeleteTriggerTool) Meta() ToolMeta {
	return t.meta
}

// Execute deletes the trigger.
func (t *DeleteTriggerTool) Execute(ctx context.Context, args map[string]any) (any, error) {
	triggerID, ok := args["trigger_id"].(string)
	if !ok || triggerID == "" {
		return nil, ErrDeleteTriggerIDRequired
	}

	if err := t.deleter(ctx, triggerID); err != nil {
		return nil, fmt.Errorf("delete_trigger_tool - delete: %w", err)
	}

	return map[string]any{
		"trigger_id": triggerID,
		"status":     "deleted",
	}, nil
}

// timeRfc3339 is a convenience constant.
const timeRfc3339 = "2006-01-02T15:04:05Z07:00"

// getStringArg safely extracts a string from a map or returns empty string.
func getStringArg(args map[string]any, key string) string {
	v, ok := args[key].(string)
	if !ok {
		return ""
	}

	return v
}
