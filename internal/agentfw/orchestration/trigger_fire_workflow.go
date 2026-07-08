package orchestration

import (
	"errors"
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
	TaskQueues   WorkflowTaskQueues
}

// TriggerFireWorkflowInput carries the trigger identity plus infrastructure
// routing decisions persisted in workflow history.
type TriggerFireWorkflowInput struct {
	TriggerID  string
	TaskQueues WorkflowTaskQueues
}

const (
	triggerMaxRetries          = 3
	defaultStartToCloseTimeout = 30 * time.Second
)

var (
	ErrTriggerFireInputRequired          = errors.New("trigger fire input is required")
	ErrTriggerFireInputTriggerIDRequired = errors.New("trigger fire input trigger id is required")
)

// TriggerFireWorkflow is a Temporal cron-scheduled workflow that fires
// when a trigger's cron expression matches. It loads the trigger + template
// from the DB via an activity, then dispatches a child AgentWorkflow.
func TriggerFireWorkflow(ctx workflow.Context, input *TriggerFireWorkflowInput) error {
	if input == nil {
		return nonRetryableWorkflowValidationError(ErrTriggerFireInputRequired)
	}

	if input.TriggerID == "" {
		return nonRetryableWorkflowValidationError(ErrTriggerFireInputTriggerIDRequired)
	}

	if err := input.TaskQueues.ValidateTriggerFire(); err != nil {
		return nonRetryableWorkflowValidationError(err)
	}

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: defaultStartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: time.Minute,
			MaximumAttempts: triggerMaxRetries,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	triggerActivityCtx := withRequiredActivityTaskQueue(ctx, input.TaskQueues.Trigger)

	var fireInput FireTriggerInput
	if err := workflow.ExecuteActivity(triggerActivityCtx, FireTriggerActivityName, input.TriggerID).Get(ctx, &fireInput); err != nil {
		return fmt.Errorf("TriggerFireWorkflow - fire activity: %w", err)
	}

	fireInput.TaskQueues = input.TaskQueues

	childID := fmt.Sprintf("trigger-%s-%d", input.TriggerID, workflow.Now(ctx).UnixMilli())
	childOptions := workflow.ChildWorkflowOptions{
		WorkflowID: childID,
		TaskQueue:  fireInput.TaskQueues.NativeControl,
	}

	childCtx := workflow.WithChildOptions(ctx, childOptions)

	return workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, &AgentWorkflowInput{
		RunID:        fireInput.RunID,
		SystemPrompt: fireInput.SystemPrompt,
		Message:      fireInput.Message,
		Tools:        fireInput.Tools,
		Config:       fireInput.Config,
		TaskQueues:   fireInput.TaskQueues,
	}).Get(ctx, nil)
}
