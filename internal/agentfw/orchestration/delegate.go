package orchestration

import (
	"errors"
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

	// defaultMaxDelegateDepth bounds a delegation tree when the run states
	// no limit. A tree, not a chain: what matters is that the recursion
	// terminates, because every level re-injects the delegate tool and a
	// prompt that talks an agent into delegating to itself would otherwise
	// never stop.
	defaultMaxDelegateDepth = 3

	// _delegateKeyType is the JSON-Schema "type" key in the tool's parameters.
	_delegateKeyType = "type"

	// Reasons a delegation is refused. They are stable tokens: a run's
	// events and the model's tool message both carry them, so an operator
	// reading a run can tell a refusal from a failure.
	delegateReasonDepthExhausted  = "delegate_depth_exhausted"
	delegateReasonBudgetExhausted = "delegate_token_budget_exhausted"
)

var (
	// ErrDelegateDepthExhausted is returned when a delegation would exceed
	// the tree's depth limit.
	ErrDelegateDepthExhausted = errors.New(delegateReasonDepthExhausted)
	// ErrDelegateBudgetExhausted is returned when the delegation tree has
	// spent its token budget.
	ErrDelegateBudgetExhausted = errors.New(delegateReasonBudgetExhausted)
)

// DepthLimit is the tree's depth limit, defaulting on the root.
func (b *DelegateBudget) DepthLimit() int {
	if b.MaxDepth <= 0 {
		return defaultMaxDelegateDepth
	}

	return b.MaxDepth
}

// Allowed reports whether this workflow may delegate at all. A workflow at the
// limit is not offered the tool: a model cannot call what it cannot see, and
// the guard in executeDelegateTool catches a hallucinated call anyway.
func (b *DelegateBudget) Allowed() bool {
	return b.Depth < b.DepthLimit()
}

// Exhausted reports whether the tree has spent its token budget.
func (b *DelegateBudget) Exhausted() bool {
	return b.TokenBudget > 0 && b.TokensSpent() >= b.TokenBudget
}

// TokensSpent is what the tree has spent in total, baseline included: the
// number a delegation decision is measured against.
func (b *DelegateBudget) TokensSpent() int64 {
	return b.BaselineSpent + b.OwnTokensSpent
}

// Check reports why the tree may not delegate further, or nil when it may.
// It is a pure decision, so the policy is testable without a workflow.
func (b *DelegateBudget) Check() error {
	if !b.Allowed() {
		return fmt.Errorf("%w: depth %d of %d", ErrDelegateDepthExhausted, b.Depth, b.DepthLimit())
	}

	if b.Exhausted() {
		return fmt.Errorf("%w: spent %d of %d tokens", ErrDelegateBudgetExhausted, b.TokensSpent(), b.TokenBudget)
	}

	return nil
}

// Child returns the budget a delegated child starts under: one level deeper,
// under the same limit and budget, carrying what the tree has spent so far.
func (b *DelegateBudget) Child() DelegateBudget {
	return DelegateBudget{
		Depth:         b.Depth + 1,
		MaxDepth:      b.DepthLimit(),
		TokenBudget:   b.TokenBudget,
		BaselineSpent: b.TokensSpent(),
	}
}

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

// toolsWithDelegate exposes the domain tools plus the delegate tool while the
// delegation tree has room left. Both agent loops assemble their tool list this
// way, so a workflow at the limit — either loop — never sees the tool, and the
// model cannot call what it cannot see.
func toolsWithDelegate(budget *DelegateBudget, domainTools []entity.ToolDef) []entity.ToolDef {
	allTools := make([]entity.ToolDef, 0, len(domainTools)+1)
	allTools = append(allTools, domainTools...)

	if budget.Allowed() {
		allTools = append(allTools, DelegateToolDef())
	}

	return allTools
}

// isDelegateToolCall checks whether a tool call targets the delegate tool.
func isDelegateToolCall(tc entity.ToolCall) bool {
	return tc.Function.Name == delegateToolName
}

// handleDelegateToolCall runs one delegate_to_agent call for an agent loop:
// it enforces the delegation budget, spawns the child, accounts what the child
// spent, and returns the tool message the model reads. Both agent loops call
// it, so neither can drift from the other's budget.
func handleDelegateToolCall(
	ctx workflow.Context,
	tc entity.ToolCall,
	parent *delegateParent,
) entity.Message {
	content, spent, err := executeDelegateTool(ctx, tc, parent)
	if err != nil {
		// A refusal is an answer, not a crash: the reason reaches the model
		// in the tool message, where it can change what the agent does next.
		content = fmt.Sprintf("Error delegating task: %v", err)
	}

	parent.budget.OwnTokensSpent += spent

	return entity.Message{
		Role:       entity.RoleTool,
		ToolCallID: tc.ID,
		Content:    content,
	}
}

// delegateParent is everything a delegation needs from its parent. It is a
// value, so the budget it carries is the parent's own: the accounting below
// updates the caller's total through the pointer it holds.
type delegateParent struct {
	budget      *DelegateBudget
	accountID   string
	projectID   string
	config      entity.LLMConfig
	domainTools []entity.ToolDef
	mcpConfigs  []entity.MCPServerConfig
	taskQueues  WorkflowTaskQueues
}

// executeDelegateTool spawns a child AgentWorkflow for the delegated task and
// returns the final assistant output along with what the child's subtree spent.
func executeDelegateTool(
	ctx workflow.Context,
	tc entity.ToolCall,
	parent *delegateParent,
) (output string, tokensSpent int64, err error) {
	if err := parent.taskQueues.ValidateNativeAgent(); err != nil {
		return "", 0, err
	}

	// Refuse a delegation the tree cannot afford before spending anything on
	// it. A model that keeps asking gets told why, in stable words.
	if err := parent.budget.Check(); err != nil {
		return "", 0, err
	}

	delegateInput, err := entity.ParseDelegateArgs(tc.Function.Arguments)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse delegate_to_agent args: %w", err)
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
		TaskQueue:                parent.taskQueues.NativeControl,
	}

	childCtx := workflow.WithChildOptions(ctx, childOptions)

	var result WorkflowResult

	childInput := delegateChildWorkflowInput(tc.ID, delegateInput, parent)

	err = workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, childInput).Get(ctx, &result)
	if err != nil {
		return "", 0, err
	}

	return result.Output, result.TokensSpent, nil
}

func delegateChildWorkflowInput(
	runID string,
	delegateInput *entity.DelegateTaskInput,
	parent *delegateParent,
) *AgentWorkflowInput {
	return &AgentWorkflowInput{
		AccountID:        parent.accountID,
		ProjectID:        parent.projectID,
		RunID:            runID,
		SystemPrompt:     delegateInput.SystemPrompt,
		Message:          delegateInput.Task,
		Config:           delegateChildConfig(parent.config, delegateInput.ModelRef),
		Tools:            delegateChildTools(parent.domainTools, delegateInput.Tools),
		MCPServerConfigs: parent.mcpConfigs,
		TaskQueues:       parent.taskQueues,
		Delegate:         parent.budget.Child(),
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
