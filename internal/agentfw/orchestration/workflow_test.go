package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// ——— mock activities ———

func mockPrepareActivity(_ context.Context, input *PrepareInput) (*PrepareOutput, error) {
	msgs := append([]entity.Message{}, input.History...)
	msgs = append(msgs, entity.Message{Role: entity.RoleUser, Content: input.Message})

	return &PrepareOutput{
		Messages: msgs,
		Tools:    input.Tools,
	}, nil
}

func mockLLMStepActivity(_ context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
	return &LLMStepOutput{
		Content:      "LLM response for " + input.RunID,
		Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		FinishReason: "stop",
	}, nil
}

// mockLLMStepWithTool returns an LLMStepOutput that includes a tool call on the first
// invocation and then switches to text-only on subsequent invocations.
type mockLLMStepWithTool struct {
	callCount int
}

func (m *mockLLMStepWithTool) fn(_ context.Context, _ *LLMStepInput) (*LLMStepOutput, error) {
	m.callCount++
	if m.callCount == 1 {
		return &LLMStepOutput{
			Content: "I'll call a tool",
			ToolCalls: []entity.ToolCall{
				{ID: "tc1", Type: "function", Function: entity.ToolCallFunction{Name: "mock_tool", Arguments: `{"key":"val"}`}},
			},
			Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
			FinishReason: "tool_calls",
		}, nil
	}

	return &LLMStepOutput{
		Content:      "Tool result received",
		Usage:        entity.Usage{PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30},
		FinishReason: "stop",
	}, nil
}

func mockToolExecActivity(_ context.Context, _ ToolInput) (*ToolOutput, error) {
	return &ToolOutput{
		Output:     `{"result": "mock_output"}`,
		ExitCode:   0,
		IsError:    false,
		DurationMs: 5,
	}, nil
}

// ——— test environment ———

func newWorkflowTestEnv() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})
	env.RegisterActivityWithOptions(mockPrepareActivity, activity.RegisterOptions{
		Name: PrepareActivityName,
	})
	env.RegisterActivityWithOptions(mockLLMStepActivity, activity.RegisterOptions{
		Name: LLMStepActivityName,
	})
	env.RegisterActivityWithOptions(mockToolExecActivity, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	return env
}

// ——— tests ———

func TestAgentWorkflowV2_TextOnly(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()

	env.ExecuteWorkflow(AgentWorkflow, &AgentWorkflowInput{
		RunID:   "run-v2-text",
		Message: "Hello",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "run-v2-text", result.RunID)
	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, int32(1), result.Step)
}

func TestAgentWorkflowV2_ToolRound(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})
	env.RegisterActivityWithOptions(mockPrepareActivity, activity.RegisterOptions{
		Name: PrepareActivityName,
	})

	mockLLM := &mockLLMStepWithTool{}
	env.RegisterActivityWithOptions(mockLLM.fn, activity.RegisterOptions{
		Name: LLMStepActivityName,
	})
	env.RegisterActivityWithOptions(mockToolExecActivity, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	env.ExecuteWorkflow(AgentWorkflow, &AgentWorkflowInput{
		RunID:   "run-v2-tool",
		Message: "Use a tool",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, int32(2), result.Step)
}

func TestAgentWorkflowV2_Cancel(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "cancel")
	}, 0)

	env.ExecuteWorkflow(AgentWorkflow, &AgentWorkflowInput{
		RunID:   "run-v2-cancel",
		Message: "Will be canceled",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "canceled", result.LifecycleState)
}

func TestAgentWorkflowV2_PauseResume(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "pause")
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "resume")
	}, time.Second)

	env.ExecuteWorkflow(AgentWorkflow, &AgentWorkflowInput{
		RunID:   "run-v2-pause",
		Message: "Will be paused",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "completed", result.LifecycleState)
}

func TestAgentWorkflowV2_QueryStatus(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})
	env.RegisterActivityWithOptions(mockPrepareActivity, activity.RegisterOptions{
		Name: PrepareActivityName,
	})
	env.RegisterActivityWithOptions(mockLLMStepActivity, activity.RegisterOptions{
		Name: LLMStepActivityName,
	})
	env.RegisterActivityWithOptions(mockToolExecActivity, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "cancel")
	}, 0)

	env.ExecuteWorkflow(AgentWorkflow, &AgentWorkflowInput{
		RunID:   "run-v2-query",
		Message: "Query test",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "canceled", result.LifecycleState)
}
