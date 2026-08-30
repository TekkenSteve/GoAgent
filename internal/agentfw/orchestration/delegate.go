package orchestration

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

const (
	delegateToolName = "delegate_to_agent"

	// delegateChildWorkflowTimeout limits how long a single delegated
	// sub-agent is allowed to run before the parent gets a timeout error.
	delegateChildWorkflowTimeout = 10 * time.Minute

	// _delegateKeyType is the JSON-Schema "type" key in the tool's parameters.
	_delegateKeyType = "type"
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
				_delegateKeyType: "object",
				"properties": map[string]any{
					"system_prompt": map[string]any{
						_delegateKeyType: "string",
						"description":    "System prompt defining the sub-agent's role, expertise, and behavioral guidelines",
					},
					"task": map[string]any{
						_delegateKeyType: "string",
						"description":    "The specific task for the sub-agent to accomplish. Be detailed and include all necessary context.",
					},
					"model": map[string]any{
						_delegateKeyType: "string",
						"description":    "Optional: model to use for the sub-agent (e.g., gpt-4o-mini, claude-3-haiku). Defaults to the parent agent's model. Use a cheaper/faster model for simple sub-tasks.",
					},
					"tools": map[string]any{
						_delegateKeyType: "array",
						"items":          map[string]any{_delegateKeyType: "string"},
						"description":    "Optional: list of tool names the sub-agent can use (e.g., web_search, code_executor). When omitted the sub-agent runs as a pure LLM call with no tool access.",
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
	taskQueues *WorkflowTaskQueues,
) (string, error) {
	if err := taskQueues.ValidateNativeAgent(); err != nil {
		return "", err
	}

	delegateInput, err := entity.ParseDelegateArgs(tc.Function.Arguments)
	if err != nil {
		return "", fmt.Errorf("failed to parse delegate_to_agent args: %w", err)
	}

	childOptions := workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("delegate-%s", tc.ID),
		// Explicit policies, never implicit SDK defaults:
		//   - REQUEST_CANCEL: the delegate child is canceled gracefully when
		//     the parent run terminates — the child's own cancel handling stops
		//     in-flight LLM/tool work instead of the parent being hard-killed
		//     mid-inference (TERMINATE, the SDK default).
		//   - ALLOW_DUPLICATE_FAILED_ONLY: a retried child launch with the same
		//     WorkflowID may only bind to a previously failed execution, never
		//     to a running one.
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		WorkflowExecutionTimeout: delegateChildWorkflowTimeout,
		TaskQueue:                taskQueues.NativeControl,
	}

	childCtx := workflow.WithChildOptions(ctx, childOptions)

	var result WorkflowResult

	childInput := delegateChildWorkflowInput(tc.ID, delegateInput, parentCfg, accountID, parentTools, parentMCPServerConfigs, taskQueues)

	err = workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, childInput).Get(ctx, &result)
	if err != nil {
		return "", err
	}

	return result.Output, nil
}

func delegateChildWorkflowInput(
	runID string,
	delegateInput *entity.DelegateTaskInput,
	parentCfg entity.LLMConfig,
	accountID string,
	parentTools []entity.ToolDef,
	parentMCPServerConfigs []entity.MCPServerConfig,
	taskQueues *WorkflowTaskQueues,
) *AgentWorkflowInput {
	return &AgentWorkflowInput{
		AccountID:        accountID,
		RunID:            runID,
		SystemPrompt:     delegateInput.SystemPrompt,
		Message:          delegateInput.Task,
		Config:           delegateChildConfig(parentCfg, delegateInput.ModelRef),
		Tools:            delegateChildTools(parentTools, delegateInput.Tools),
		MCPServerConfigs: parentMCPServerConfigs,
		TaskQueues:       *taskQueues,
	}
}

func delegateChildConfig(parentCfg entity.LLMConfig, modelRef string) entity.LLMConfig {
	childConfig := parentCfg
	if modelRef != "" {
		childConfig.Model = modelRef
	}

	childConfig.Temperature = 0

	return childConfig
}

func delegateChildTools(parentTools []entity.ToolDef, requested []string) []entity.ToolDef {
	if len(requested) == 0 {
		return nil
	}

	nameSet := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		nameSet[name] = struct{}{}
	}

	childTools := make([]entity.ToolDef, 0, len(requested))

	for _, tool := range parentTools {
		if _, ok := nameSet[tool.Function.Name]; ok {
			childTools = append(childTools, tool)
		}
	}

	return childTools
}
