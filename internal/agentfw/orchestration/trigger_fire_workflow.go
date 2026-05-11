package orchestration

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// FireTriggerInput contains the data returned by the fire-trigger activity.
type FireTriggerInput struct {
	RunID        string
	SystemPrompt string
	Message      string
	Tools        []entity.ToolDef
	Config       entity.LLMConfig
}

// TriggerFireWorkflow is a Temporal cron-scheduled workflow that fires
// when a trigger's cron expression matches. It loads the trigger + template
// from the DB via an activity, then dispatches a child AgentWorkflow.
func TriggerFireWorkflow(ctx workflow.Context, triggerID string) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var input FireTriggerInput
	if err := workflow.ExecuteActivity(ctx, FireTriggerActivityName, triggerID).Get(ctx, &input); err != nil {
		return fmt.Errorf("TriggerFireWorkflow - fire activity: %w", err)
	}

	// Dispatch child AgentWorkflow (inherits task queue from parent)
	childID := fmt.Sprintf("trigger-%s-%d", triggerID, workflow.Now(ctx).UnixMilli())
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: childID,
	})

	return workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, AgentWorkflowInput{
		RunID:        input.RunID,
		SystemPrompt: input.SystemPrompt,
		Message:      input.Message,
		Tools:        input.Tools,
		Config:       input.Config,
	}).Get(ctx, nil)
}
