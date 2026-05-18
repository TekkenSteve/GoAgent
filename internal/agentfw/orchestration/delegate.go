package orchestration

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/workflow"
)

const delegateToolName = "delegate_to_agent"

// DelegateToolDef returns the ToolDef for the delegate_to_agent tool.
// It is injected into every agent's tool list so the LLM can delegate sub-tasks.
func DelegateToolDef() entity.ToolDef {
	return entity.ToolDef{
		Type: "function",
		Function: entity.ToolFuncDef{
			Name:        delegateToolName,
			Description: "Delegate a sub-task to a temporary sub-agent with custom instructions. The sub-agent runs in an isolated context and returns its result. Use this when a task requires a different focus, specialized expertise, or a separate context to avoid polluting the current conversation.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"system_prompt": map[string]any{
						"type":        "string",
						"description": "System prompt defining the sub-agent's role, expertise, and behavioral guidelines",
					},
					"task": map[string]any{
						"type":        "string",
						"description": "The specific task for the sub-agent to accomplish. Be detailed and include all necessary context.",
					},
					"model": map[string]any{
						"type":        "string",
						"description": "Optional: model to use for the sub-agent (e.g., gpt-4o-mini, claude-3-haiku). Defaults to the parent agent's model. Use a cheaper/faster model for simple sub-tasks.",
					},
					"tools": map[string]any{
						"type":        "array",
						"items":       map[string]any{"type": "string"},
						"description": "Optional: list of tool names the sub-agent can use (e.g., web_search, code_executor)",
					},
				},
				"required": []string{"system_prompt", "task"},
			},
		},
	}
}

// isDelegateToolCall checks whether a tool call targets the delegate tool.
func isDelegateToolCall(tc entity.ToolCall) bool {
	return tc.Function.Name == delegateToolName
}

// executeDelegateTool spawns a child AgentWorkflow for the delegated task
// and returns the final assistant output.
func executeDelegateTool(ctx workflow.Context, tc entity.ToolCall, parentCfg entity.LLMConfig, accountID string) (string, error) {
	args := parseArgsJSON(tc.Function.Arguments)

	systemPrompt := getStringArg(args, "system_prompt")
	task := getStringArg(args, "task")
	model := getStringArg(args, "model")

	if model == "" {
		model = parentCfg.Model
	}

	// Build a default config; delegate always uses zero temperature
	// for deterministic sub-agent output.
	childConfig := entity.DefaultLLMConfig()
	childConfig.Model = model

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("delegate-%s", tc.ID),
	})

	var result WorkflowResult

	err := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, &AgentWorkflowInput{
		AccountID:    accountID,
		RunID:        tc.ID,
		SystemPrompt: systemPrompt,
		Message:      task,
		Config:       childConfig,
	}).Get(ctx, &result)
	if err != nil {
		return "", err
	}

	return result.Output, nil
}

// getStringArg safely extracts a string from a map or returns empty string.
func getStringArg(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok {
		return ""
	}

	if s, ok := v.(string); ok {
		return s
	}

	return ""
}
