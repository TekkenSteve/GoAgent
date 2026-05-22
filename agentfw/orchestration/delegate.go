package orchestration

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"go.temporal.io/sdk/workflow"
)

const (
	delegateToolName = "delegate_to_agent"

	// delegateChildWorkflowTimeout limits how long a single delegated
	// sub-agent is allowed to run before the parent gets a timeout error.
	delegateChildWorkflowTimeout = 10 * time.Minute
)

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
						"description": "Optional: list of tool names the sub-agent can use (e.g., web_search, code_executor). When omitted the sub-agent runs as a pure LLM call with no tool access.",
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
func executeDelegateTool(
	ctx workflow.Context,
	tc entity.ToolCall,
	parentCfg entity.LLMConfig,
	accountID string,
	parentTools []entity.ToolDef,
	parentMCPServerConfigs []entity.MCPServerConfig,
) (string, error) {
	delegateInput, err := entity.ParseDelegateArgs(tc.Function.Arguments)
	if err != nil {
		return "", fmt.Errorf("failed to parse delegate_to_agent args: %w", err)
	}

	systemPrompt := delegateInput.SystemPrompt
	task := delegateInput.Task
	model := delegateInput.ModelRef

	if model == "" {
		model = parentCfg.Model
	}

	// Inherit full parent config, override model if caller specified
	// one (e.g. cheaper/faster model for simple sub-tasks), and force
	// zero temperature for deterministic sub-agent output.
	childConfig := parentCfg
	childConfig.Model = model
	childConfig.Temperature = 0

	// Filter parent tools by the names the caller explicitly requested.
	// When no tools argument is provided the sub-agent runs as a pure
	// LLM call (no tool access), keeping delegation lightweight.
	var childTools []entity.ToolDef

	if requested := delegateInput.Tools; len(requested) > 0 {
		nameSet := make(map[string]struct{}, len(requested))
		for _, name := range requested {
			nameSet[name] = struct{}{}
		}

		for _, t := range parentTools {
			if _, ok := nameSet[t.Function.Name]; ok {
				childTools = append(childTools, t)
			}
		}
	}

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:               fmt.Sprintf("delegate-%s", tc.ID),
		WorkflowExecutionTimeout: delegateChildWorkflowTimeout,
	})

	var result WorkflowResult

	err = workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, &AgentWorkflowInput{
		AccountID:        accountID,
		RunID:            tc.ID,
		SystemPrompt:     systemPrompt,
		Message:          task,
		Config:           childConfig,
		Tools:            childTools,
		MCPServerConfigs: parentMCPServerConfigs,
	}).Get(ctx, &result)
	if err != nil {
		return "", err
	}

	return result.Output, nil
}
