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

func testAgentWorkflowInput(runID, message string) *AgentWorkflowInput {
	return &AgentWorkflowInput{
		RunID:      runID,
		Message:    message,
		TaskQueues: testWorkflowTaskQueues(),
	}
}

// ——— tests ———

func TestAgentWorkflowV2_TextOnly(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()
	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-v2-text", "Hello"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "run-v2-text", result.RunID)
	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, int32(1), result.Step)
}

func TestAgentWorkflowV2_UserMessageSignalContinuesRun(t *testing.T) {
	t.Parallel()

	var callCount int

	env := newWorkflowTestEnv()
	env.RegisterActivityWithOptions(func(_ context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
		callCount++
		if callCount == 1 {
			require.Equal(t, []entity.Message{
				{Role: entity.RoleUser, Content: "Hello"},
			}, input.Messages)
		}

		if callCount == 2 {
			require.Equal(t, []entity.Message{
				{Role: entity.RoleUser, Content: "Hello"},
				{Role: entity.RoleAssistant, Content: "response"},
				{Role: entity.RoleUser, Content: "continue"},
			}, input.Messages)
		}

		return &LLMStepOutput{
			Content:      "response",
			Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
			FinishReason: "stop",
		}, nil
	}, activity.RegisterOptions{Name: LLMStepActivityName})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentMessageSignal, UserMessageSignal{Content: "continue"})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "cancel")
	}, 2*time.Second)

	input := testAgentWorkflowInput("run-v2-user-message", "Hello")
	input.AwaitUserInput = true
	env.ExecuteWorkflow(AgentWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, callCount)

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "canceled", result.LifecycleState)
	require.Equal(t, int32(2), result.Step)
}

func TestAgentWorkflowV2_WaitUserInputTimeout(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()
	input := testAgentWorkflowInput("run-v2-wait-timeout", "Hello")
	input.AwaitUserInput = true
	input.AwaitUserInputTimeout = 100 * time.Millisecond
	env.ExecuteWorkflow(AgentWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, string(entity.LifecycleCompleted), result.LifecycleState)

	// The timeout must be visible as the status reason so operators can
	// distinguish a timed-out wait from a normal completion.
	var status RunStatus

	resp, err := env.QueryWorkflow(QueryRunStatus)
	require.NoError(t, err)
	require.NoError(t, resp.Get(&status))
	require.Equal(t, awaitingInputTimeoutReason, status.Reason)
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

	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-v2-tool", "Use a tool"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, int32(2), result.Step)
}

func TestEstimateMessagesStateSizeBytes(t *testing.T) {
	t.Parallel()

	require.Zero(t, estimateMessagesStateSizeBytes(nil))
	require.Zero(t, estimateMessagesStateSizeBytes([]entity.Message{}))

	msgs := []entity.Message{
		{Role: entity.RoleUser, Content: "hello"},
		{Role: entity.RoleAssistant, Content: "hi there"},
	}

	got := estimateMessagesStateSizeBytes(msgs)
	require.Positive(t, got)

	// Deterministic: the same history must yield the same estimate on replay.
	require.Equal(t, got, estimateMessagesStateSizeBytes(msgs))
}

func TestAgentWorkflowV2_ContinueAsNewSnapshotsHistory(t *testing.T) {
	t.Parallel()

	var snapCalls []*SnapshotHistoryInput

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})
	env.RegisterActivityWithOptions(mockPrepareActivity, activity.RegisterOptions{
		Name: PrepareActivityName,
	})

	// Tool-call first, then text: the Continue-As-New decision only runs
	// after the tool round, and a text-only round exits the run early.
	mockLLM := &mockLLMStepWithTool{}
	env.RegisterActivityWithOptions(mockLLM.fn, activity.RegisterOptions{
		Name: LLMStepActivityName,
	})
	env.RegisterActivityWithOptions(mockToolExecActivity, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})
	env.RegisterActivityWithOptions(func(_ context.Context, input *SnapshotHistoryInput) (*SnapshotHistoryOutput, error) {
		snapCalls = append(snapCalls, input)

		return &SnapshotHistoryOutput{Ref: "snap://" + input.RunID}, nil
	}, activity.RegisterOptions{Name: SnapshotHistoryActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, _ *LoadHistoryInput) (*LoadHistoryOutput, error) {
		return &LoadHistoryOutput{Messages: []entity.Message{
			{Role: entity.RoleUser, Content: "restored"},
		}}, nil
	}, activity.RegisterOptions{Name: LoadHistoryActivityName})

	input := testAgentWorkflowInput("run-v2-can-snap", "Hello")
	input.ContinuePolicy = ContinueAsNewPolicy{StateSizeThresholdByte: 1, MaxContinuations: 3}
	env.ExecuteWorkflow(AgentWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())

	// The run must end with Continue-As-New, not a failure, and the snapshot
	// activity must have been invoked exactly once with the conversation.
	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &canErr)
	require.Len(t, snapCalls, 1)
	require.Equal(t, "run-v2-can-snap", snapCalls[0].RunID)
	require.Equal(t, 1, snapCalls[0].Round)
	require.GreaterOrEqual(t, len(snapCalls[0].Messages), 2)
}

func TestAgentWorkflowV2_LoadsHistorySnapshot(t *testing.T) {
	t.Parallel()

	var llmInputs [][]entity.Message

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})
	env.RegisterActivityWithOptions(mockPrepareActivity, activity.RegisterOptions{
		Name: PrepareActivityName,
	})
	env.RegisterActivityWithOptions(func(_ context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
		llmInputs = append(llmInputs, input.Messages)

		return &LLMStepOutput{
			Content:      "restored response",
			Usage:        entity.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
			FinishReason: "stop",
		}, nil
	}, activity.RegisterOptions{Name: LLMStepActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, input *LoadHistoryInput) (*LoadHistoryOutput, error) {
		require.Equal(t, "snap://restored", input.Ref)

		return &LoadHistoryOutput{Messages: []entity.Message{
			{Role: entity.RoleUser, Content: "earlier"},
			{Role: entity.RoleAssistant, Content: "earlier reply"},
			{Role: entity.RoleUser, Content: "continue here"},
		}}, nil
	}, activity.RegisterOptions{Name: LoadHistoryActivityName})

	input := testAgentWorkflowInput("run-v2-load-snap", "ignored initial message")
	input.Continuation = ContinuationPayload{
		RunID:              "run-v2-load-snap",
		HistoryRef:         "snap://restored",
		InitialRequestedAt: time.Now(),
	}
	env.ExecuteWorkflow(AgentWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// The LLM must see the restored conversation, not the initial request.
	require.Len(t, llmInputs, 1)
	require.Equal(t, []entity.Message{
		{Role: entity.RoleUser, Content: "earlier"},
		{Role: entity.RoleAssistant, Content: "earlier reply"},
		{Role: entity.RoleUser, Content: "continue here"},
	}, llmInputs[0])
}

func TestAgentWorkflowV2_Cancel(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "cancel")
	}, 0)

	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-v2-cancel", "Will be canceled"))

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

	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-v2-pause", "Will be paused"))

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

	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-v2-query", "Query test"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "canceled", result.LifecycleState)
}
