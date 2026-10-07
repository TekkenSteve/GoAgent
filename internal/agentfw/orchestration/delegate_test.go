package orchestration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
)

// A delegation tree has two bounds: how deep it may go, and what it may
// spend. Every level re-injects the delegate tool, so without them a prompt
// that talks an agent into delegating to itself builds an unbounded tree of
// paid LLM calls.

func TestDelegateBudgetDefaultsAndChild(t *testing.T) {
	t.Parallel()

	root := DelegateBudget{}

	require.Equal(t, defaultMaxDelegateDepth, root.DepthLimit())
	require.True(t, root.Allowed())
	require.False(t, root.Exhausted(), "no token budget means no token bound")

	child := root.Child()
	require.Equal(t, 1, child.Depth)
	require.Equal(t, defaultMaxDelegateDepth, child.MaxDepth, "children inherit the resolved limit")
	require.Zero(t, child.TokensSpent(), "a child starts with nothing of its own")
}

func TestDelegateBudgetRefusesAtTheLimit(t *testing.T) {
	t.Parallel()

	budget := DelegateBudget{MaxDepth: 2}

	for range 2 {
		require.True(t, budget.Allowed())
		budget = budget.Child()
	}

	require.Equal(t, 2, budget.Depth)
	require.False(t, budget.Allowed(), "a workflow at the limit may not delegate")

	below := budget.Child()
	require.False(t, below.Allowed(), "and neither may anything below it")
}

func TestDelegateBudgetExhaustsTokens(t *testing.T) {
	t.Parallel()

	budget := DelegateBudget{TokenBudget: 100, BaselineSpent: 60, OwnTokensSpent: 39}
	require.False(t, budget.Exhausted())

	budget.OwnTokensSpent = 40
	require.True(t, budget.Exhausted(), "spending the budget exactly exhausts it")

	child := budget.Child()
	require.Equal(t, int64(100), child.BaselineSpent, "the committed cost travels down as a baseline")
	require.Zero(t, child.OwnTokensSpent, "and is never counted as the child's own")
	require.Equal(t, int64(100), child.TokenBudget)
}

func TestDelegateBudgetCheckRefusesAtTheLimit(t *testing.T) {
	t.Parallel()

	err := (&DelegateBudget{MaxDepth: 1, Depth: 1}).Check()

	require.ErrorIs(t, err, ErrDelegateDepthExhausted)
	require.Contains(t, err.Error(), "depth 1 of 1")
}

func TestDelegateBudgetCheckRefusesPastTheTokenBudget(t *testing.T) {
	t.Parallel()

	err := (&DelegateBudget{MaxDepth: 5, TokenBudget: 100, OwnTokensSpent: 100}).Check()

	require.ErrorIs(t, err, ErrDelegateBudgetExhausted)
	require.Contains(t, err.Error(), "spent 100 of 100 tokens")
}

func TestDelegateBudgetCheckAllowsWithinBounds(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&DelegateBudget{MaxDepth: 2, Depth: 1, TokenBudget: 100, OwnTokensSpent: 50}).Check())
}

// selfDelegatingLLM always asks to delegate to itself, records what each run
// sees (the tool messages the model would read), and stops asking once it has
// been told why — the behavior the depth limit exists to produce.
type selfDelegatingLLM struct {
	mu        sync.Mutex
	toolMsgs  []string
	runIDs    map[string]struct{}
	childOf   map[string]string // delegated run -> the run that delegated it
	nextID    atomic.Int32
	refusals  int
	llmCalled int
}

func newSelfDelegatingLLM() *selfDelegatingLLM {
	return &selfDelegatingLLM{
		runIDs:  map[string]struct{}{},
		childOf: map[string]string{},
	}
}

func (m *selfDelegatingLLM) call(_ context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.llmCalled++
	m.runIDs[input.RunID] = struct{}{}

	sawRefusal := false

	for _, msg := range input.Messages {
		if msg.Role != entity.RoleTool {
			continue
		}

		m.toolMsgs = append(m.toolMsgs, msg.Content)

		if strings.Contains(msg.Content, delegateReasonDepthExhausted) {
			sawRefusal = true
		}
	}

	if sawRefusal {
		m.refusals++

		// A model told why it cannot delegate stops asking; the run then
		// completes normally, which is the point of refusing in words.
		return &LLMStepOutput{
			Content:      "understood, answering directly",
			Usage:        entity.Usage{TotalTokens: 10},
			FinishReason: "stop",
		}, nil
	}

	childID := fmt.Sprintf("tc-%d", m.nextID.Add(1))
	m.childOf[childID] = input.RunID

	return &LLMStepOutput{
		Content: "delegating this to a sub-agent",
		ToolCalls: []entity.ToolCall{{
			ID:   childID,
			Type: "function",
			Function: entity.ToolCallFunction{
				Name:      delegateToolName,
				Arguments: `{"system_prompt":"you are a sub-agent","task":"delegate this again"}`,
			},
		}},
		Usage:        entity.Usage{TotalTokens: 10},
		FinishReason: "tool_calls",
	}, nil
}

// deepest reports the depth of the deepest run in the tree.
func (m *selfDelegatingLLM) deepest() int {
	deepest := 0

	for runID := range m.runIDs {
		depth := 0

		for up, delegated := m.childOf[runID]; delegated; up, delegated = m.childOf[up] {
			depth++
		}

		if depth > deepest {
			deepest = depth
		}
	}

	return deepest
}

func TestAgentWorkflowRefusesSelfDelegationAtTheDepthLimit(t *testing.T) {
	t.Parallel()

	const maxDepth = 2

	llm := newSelfDelegatingLLM()

	env := newWorkflowTestEnv()
	env.RegisterActivityWithOptions(llm.call, activity.RegisterOptions{Name: LLMStepActivityName})

	input := testAgentWorkflowInput("run-self-delegate", "answer me")
	input.Delegate = DelegateBudget{MaxDepth: maxDepth}

	env.ExecuteWorkflow(AgentWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "completed", result.LifecycleState)

	llm.mu.Lock()
	defer llm.mu.Unlock()

	require.NotZero(t, llm.refusals, "the model must be told why the delegation was refused")

	// Depth is what the limit bounds. Breadth is not: a level may delegate
	// many times per round, so the tree is a tree — the token budget is what
	// bounds what it costs.
	require.Equal(t, maxDepth, llm.deepest(), "no workflow may run below the depth limit")

	require.Contains(t, strings.Join(llm.toolMsgs, "\n"), delegateReasonDepthExhausted)

	require.Equal(t, int64(llm.llmCalled*10), result.TokensSpent,
		"the whole tree's consumption is accounted for in the root's result, once")
}

// The token budget refuses a delegation whose cost the tree has already
// committed, with its own stable reason.
func TestAgentWorkflowRefusesDelegationPastTheTokenBudget(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		toolMsgs []string
	)

	delegatingOnce := func(_ context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
		mu.Lock()
		defer mu.Unlock()

		for _, msg := range input.Messages {
			if msg.Role == entity.RoleTool {
				toolMsgs = append(toolMsgs, msg.Content)
			}
		}

		return &LLMStepOutput{
			Content: "delegating",
			ToolCalls: []entity.ToolCall{{
				ID:   "tc-budget",
				Type: "function",
				Function: entity.ToolCallFunction{
					Name:      delegateToolName,
					Arguments: `{"system_prompt":"you are a sub-agent","task":"do it"}`,
				},
			}},
			Usage:        entity.Usage{TotalTokens: 50},
			FinishReason: "tool_calls",
		}, nil
	}

	env := newWorkflowTestEnv()
	env.RegisterActivityWithOptions(delegatingOnce, activity.RegisterOptions{Name: LLMStepActivityName})

	input := testAgentWorkflowInput("run-token-budget", "answer me")
	input.Delegate = DelegateBudget{MaxDepth: 5, TokenBudget: 10} // one round costs 50

	env.ExecuteWorkflow(AgentWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	mu.Lock()
	defer mu.Unlock()

	require.Contains(t, strings.Join(toolMsgs, "\n"), delegateReasonBudgetExhausted)
}
