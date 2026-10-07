package orchestration

import (
	"context"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
)

// A run's terminal state and its reason are two facts. "completed" says the
// workflow stopped; the reason says whether the agent finished its work or the
// loop ran out of rounds — and those are different situations for whoever
// reads the run afterwards.

func TestCanProceedGatesOnBillingAndLimitsOnly(t *testing.T) {
	t.Parallel()

	denied := &PrepBillingOutput{Approved: false}

	tests := []struct {
		name    string
		output  PrepareOutput
		allowed bool
	}{
		{name: "nothing checked", output: PrepareOutput{}, allowed: true},
		{
			name:    "billing denies the run",
			output:  PrepareOutput{Billing: denied},
			allowed: false,
		},
		{
			name: "an unreachable toolset does not deny the run",
			output: PrepareOutput{
				Warnings: []string{"mcp: connect mcp server \"search\": dial refused"},
			},
			allowed: true,
		},
		{
			name: "a malformed tool definition does not deny the run",
			output: PrepareOutput{
				Warnings: []string{"tool: nameless missing name or type"},
			},
			allowed: true,
		},
		{
			name: "billing still denies with warnings present",
			output: PrepareOutput{
				Billing:  denied,
				Warnings: []string{"mcp: connect mcp server \"search\": dial refused"},
			},
			allowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.allowed, tt.output.CanProceed())
		})
	}
}

// A never-finishing agent reports the cap as its reason instead of presenting
// itself as a run that finished.
func TestAgentWorkflowReportsMaxRoundsAsTheReason(t *testing.T) {
	t.Parallel()

	// The model always asks for another tool call, so the loop can only end
	// by hitting its cap.
	alwaysCallingTool := func(_ context.Context, _ *LLMStepInput) (*LLMStepOutput, error) {
		return &LLMStepOutput{
			Content: "one more tool",
			ToolCalls: []entity.ToolCall{{
				ID:   "tc-loop",
				Type: "function",
				Function: entity.ToolCallFunction{
					Name:      "mock_tool",
					Arguments: `{"key":"value"}`,
				},
			}},
			Usage:        entity.Usage{TotalTokens: 5},
			FinishReason: "tool_calls",
		}, nil
	}

	env := newWorkflowTestEnv()
	env.RegisterActivityWithOptions(alwaysCallingTool, activity.RegisterOptions{Name: LLMStepActivityName})

	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-max-rounds", "loop forever"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	require.Equal(t, "completed", result.LifecycleState)
	require.Equal(t, reasonMaxRounds, result.Reason)
	require.Equal(t, int32(maxToolRounds), result.Step, "the cap is what ended the run")
}

// A run that finishes its work carries no reason: nothing capped it.
func TestAgentWorkflowFinishedRunHasNoReason(t *testing.T) {
	t.Parallel()

	env := newWorkflowTestEnv()
	env.ExecuteWorkflow(AgentWorkflow, testAgentWorkflowInput("run-finished", "Hello"))

	var result WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	require.Equal(t, "completed", result.LifecycleState)
	require.Empty(t, result.Reason)
}
