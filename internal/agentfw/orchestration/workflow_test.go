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

func mockStepActivity(_ context.Context, input StepActivityInput) (StepActivityOutput, error) {
	return StepActivityOutput{
		Messages: []entity.Message{
			{Role: entity.RoleAssistant, Content: "Step response for " + input.RunID},
		},
		Usage: entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}, nil
}

func newWorkflowTestEnv() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{
		Name: AgentWorkflowName,
	})
	env.RegisterActivityWithOptions(mockStepActivity, activity.RegisterOptions{
		Name: AgentStepActivityName,
	})
	return env
}

func TestAgentWorkflowStepProgression(t *testing.T) {
	env := newWorkflowTestEnv()

	env.ExecuteWorkflow(AgentWorkflow, WorkflowInput{
		Request: ExecuteRequest{
			RunID: "run-step-progression",
		},
		TargetSteps: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "run-step-progression", result.RunID)
	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, int32(3), result.Step)
}

func TestAgentWorkflowPauseResumeControl(t *testing.T) {
	env := newWorkflowTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalPause, "manual-pause")
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalResume, "manual-resume")
	}, time.Second)

	env.ExecuteWorkflow(AgentWorkflow, WorkflowInput{
		Request: ExecuteRequest{
			RunID: "run-pause-resume",
		},
		TargetSteps: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, int32(2), result.Step)
}

func TestAgentWorkflowCancelControl(t *testing.T) {
	env := newWorkflowTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancel, "user-cancel")
	}, 0)

	env.ExecuteWorkflow(AgentWorkflow, WorkflowInput{
		Request: ExecuteRequest{
			RunID: "run-cancel",
		},
		TargetSteps: 5,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "cancelled", result.LifecycleState)
	require.Equal(t, int32(0), result.Step)
}

func TestAgentWorkflowStatusQueryWhilePaused(t *testing.T) {
	env := newWorkflowTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalPause, "manual-pause")
	}, 0)
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(QueryRunStatus)
		require.NoError(t, err)

		var status RunStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, "paused", status.LifecycleState)

		env.SignalWorkflow(SignalResume, "manual-resume")
	}, time.Second)

	env.ExecuteWorkflow(AgentWorkflow, WorkflowInput{
		Request: ExecuteRequest{
			RunID: "run-status-query",
		},
		TargetSteps: 1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestValidateControlOperation(t *testing.T) {
	require.NoError(t, ValidateControlOperation("running", ControlPause))
	require.NoError(t, ValidateControlOperation("paused", ControlResume))
	require.NoError(t, ValidateControlOperation("running", ControlCancel))

	err := ValidateControlOperation("running", ControlResume)
	require.Error(t, err)
	domainErr, ok := err.(*DomainError)
	require.True(t, ok)
	require.Equal(t, ErrCodeInvalidControlOperation, domainErr.Code)
}
