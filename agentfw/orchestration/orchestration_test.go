package orchestration

import (
	"context"
	"testing"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// ——— mock activities for orchestration ———

type mockToolExec struct {
	result ToolOutput
	err    error
}

func (m *mockToolExec) fn(_ context.Context, _ ToolInput) (*ToolOutput, error) {
	if m.err != nil {
		return nil, m.err
	}

	return &m.result, nil
}

// ——— test: sequential agent steps ———

func TestOrchestrationWorkflow_SequentialSteps(t *testing.T) {
	t.Parallel()

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()

	// Register child workflow
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})

	// Mock activity for the child workflow — make it a simple no-op
	mockLLM := &mockLLMStepNoTool{}
	env.RegisterActivityWithOptions(mockLLM.fn, activity.RegisterOptions{
		Name: LLMStepActivityName,
	})

	// Make PrepareActivity a simple pass-through
	env.RegisterActivityWithOptions(func(_ context.Context, input *PrepareInput) (*PrepareOutput, error) {
		return &PrepareOutput{
			Messages: []entity.Message{{Role: entity.RoleUser, Content: input.Message}},
			Tools:    input.Tools,
		}, nil
	}, activity.RegisterOptions{
		Name: PrepareActivityName,
	})

	// ToolExecActivity mock (needed by child workflow)
	mockTool := &mockToolExec{
		result: ToolOutput{Output: "mock", ExitCode: 0},
	}
	env.RegisterActivityWithOptions(mockTool.fn, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	steps := []entity.Step{
		{
			ID:     "step-1",
			Type:   entity.StepAgent,
			Name:   "first",
			Input:  map[string]any{"message": "Hello"},
			Status: entity.StepPending,
		},
		{
			ID:     "step-2",
			Type:   entity.StepAgent,
			Name:   "second",
			Input:  map[string]any{"message": "World"},
			Status: entity.StepPending,
		},
	}

	env.ExecuteWorkflow(Workflow, &entity.OrchestrationInput{
		RunID: "test-seq",
		Steps: steps,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result entity.OrchestrationResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "test-seq", result.RunID)
	require.Len(t, result.Steps, 2)
	require.Equal(t, entity.StepCompleted, result.Steps[0].Status)
	require.Equal(t, entity.StepCompleted, result.Steps[1].Status)
}

// ——— test: tool step ———

func TestOrchestrationWorkflow_ToolStep(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()

	mockTool := &mockToolExec{
		result: ToolOutput{Output: `{"result": "data"}`, ExitCode: 0},
	}
	env.RegisterActivityWithOptions(mockTool.fn, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	steps := []entity.Step{
		{
			ID:     "tool-1",
			Type:   entity.StepTool,
			Name:   "search",
			Tool:   "web_search",
			Input:  map[string]any{"query": "test"},
			Status: entity.StepPending,
		},
	}

	env.ExecuteWorkflow(Workflow, &entity.OrchestrationInput{
		RunID: "test-tool",
		Steps: steps,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result entity.OrchestrationResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, entity.StepCompleted, result.Steps[0].Status)
}

// ——— test: cancel signal ———

func TestOrchestrationWorkflow_Cancel(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})

	mockLLM := &mockLLMStepNoTool{}
	env.RegisterActivityWithOptions(mockLLM.fn, activity.RegisterOptions{
		Name: LLMStepActivityName,
	})
	env.RegisterActivityWithOptions(func(_ context.Context, input *PrepareInput) (*PrepareOutput, error) {
		return &PrepareOutput{
			Messages: []entity.Message{{Role: entity.RoleUser, Content: input.Message}},
			Tools:    input.Tools,
		}, nil
	}, activity.RegisterOptions{
		Name: PrepareActivityName,
	})

	mockTool := &mockToolExec{
		result: ToolOutput{Output: "mock", ExitCode: 0},
	}
	env.RegisterActivityWithOptions(mockTool.fn, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	steps := []entity.Step{
		{
			ID:     "step-1",
			Type:   entity.StepAgent,
			Name:   "first",
			Input:  map[string]any{"message": "Hello"},
			Status: entity.StepPending,
		},
	}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AgentCommandSignal, "cancel")
	}, 0)

	env.ExecuteWorkflow(Workflow, &entity.OrchestrationInput{
		RunID: "test-cancel",
		Steps: steps,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

// ——— test: step mutation via signal ———

func TestOrchestrationWorkflow_StepMutation(t *testing.T) {
	t.Parallel()

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{Name: AgentWorkflowName})

	mockLLM := &mockLLMStepNoTool{}
	env.RegisterActivityWithOptions(mockLLM.fn, activity.RegisterOptions{Name: LLMStepActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, input *PrepareInput) (*PrepareOutput, error) {
		return &PrepareOutput{
			Messages: []entity.Message{{Role: entity.RoleUser, Content: input.Message}},
			Tools:    input.Tools,
		}, nil
	}, activity.RegisterOptions{Name: PrepareActivityName})

	mockTool := &mockToolExec{result: ToolOutput{Output: "mock", ExitCode: 0}}
	env.RegisterActivityWithOptions(mockTool.fn, activity.RegisterOptions{Name: ToolExecActivityName})

	// Start with one step, then inject a new step after it via signal
	steps := []entity.Step{
		{ID: "step-1", Type: entity.StepAgent, Name: "first", Input: map[string]any{"message": "Hello"}, Status: entity.StepPending},
	}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(StepModifySignal, entity.StepMutation{
			AppendAfter: "step-1",
			InsertSteps: []entity.Step{
				{ID: "step-2", Type: entity.StepAgent, Name: "injected", Input: map[string]any{"message": "injected"}, Status: entity.StepPending},
			},
		})
	}, 0)

	env.ExecuteWorkflow(Workflow, &entity.OrchestrationInput{RunID: "test-mutation", Steps: steps})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result entity.OrchestrationResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Len(t, result.Steps, 2)
	require.Equal(t, entity.StepCompleted, result.Steps[1].Status)
	require.Equal(t, "injected", result.Steps[1].Name)
}

// ——— test: step dependencies ———

func TestOrchestrationWorkflow_Dependencies(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite

	env := suite.NewTestWorkflowEnvironment()

	mockTool := &mockToolExec{
		result: ToolOutput{Output: "mock", ExitCode: 0},
	}
	env.RegisterActivityWithOptions(mockTool.fn, activity.RegisterOptions{
		Name: ToolExecActivityName,
	})

	// Step-2 depends on step-1
	steps := []entity.Step{
		{
			ID:     "step-1",
			Type:   entity.StepTool,
			Name:   "first",
			Tool:   "tool_a",
			Input:  map[string]any{},
			Status: entity.StepPending,
		},
		{
			ID:        "step-2",
			Type:      entity.StepTool,
			Name:      "second",
			Tool:      "tool_b",
			Input:     map[string]any{},
			DependsOn: []string{"step-1"},
			Status:    entity.StepPending,
		},
	}

	env.ExecuteWorkflow(Workflow, &entity.OrchestrationInput{
		RunID: "test-dep",
		Steps: steps,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result entity.OrchestrationResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, entity.StepCompleted, result.Steps[0].Status)
	require.Equal(t, entity.StepCompleted, result.Steps[1].Status)
}

// ——— mock helpers ———

type mockLLMStepNoTool struct{}

func (m *mockLLMStepNoTool) fn(_ context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
	return &LLMStepOutput{
		Content:      "response for " + input.RunID,
		Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		FinishReason: "stop",
	}, nil
}
