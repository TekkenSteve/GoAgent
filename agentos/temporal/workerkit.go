package temporal

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerKit registers GoAgent workflows and activities into an existing worker.
type WorkerKit struct {
	activities *orchestration.AgentActivities
}

// NewWorkerKit creates a worker registration kit. Activity wiring will be
// supplied by the application builder once repository/provider construction is
// centralized under agentos/temporal.
func NewWorkerKit(_ context.Context, _ WorkerConfig) (*WorkerKit, error) {
	return &WorkerKit{}, nil
}

// Register installs GoAgent workflow definitions into a Temporal worker.
func (k *WorkerKit) Register(w worker.Worker) error {
	w.RegisterWorkflowWithOptions(orchestration.AgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.AgentWorkflowName,
	})
	w.RegisterWorkflowWithOptions(orchestration.StreamAgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.StreamWorkflowName,
	})
	w.RegisterWorkflowWithOptions(orchestration.Workflow, workflow.RegisterOptions{
		Name: orchestration.OrchestrationWorkflowName,
	})
	w.RegisterWorkflowWithOptions(orchestration.TriggerFireWorkflow, workflow.RegisterOptions{
		Name: orchestration.TriggerFireWorkflowName,
	})

	if k.activities == nil {
		return nil
	}

	w.RegisterActivityWithOptions(k.activities.PrepareActivity, activity.RegisterOptions{
		Name: orchestration.PrepareActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.LLMStepActivity, activity.RegisterOptions{
		Name: orchestration.LLMStepActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.ToolExecActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.InitStreamActivity, activity.RegisterOptions{
		Name: orchestration.InitStreamActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.LLMStreamActivity, activity.RegisterOptions{
		Name: orchestration.LLMStreamActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.ToolExecStreamActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecStreamActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.FinishStreamActivity, activity.RegisterOptions{
		Name: orchestration.FinishStreamActivityName,
	})
	w.RegisterActivityWithOptions(k.activities.FireTriggerActivity, activity.RegisterOptions{
		Name: orchestration.FireTriggerActivityName,
	})

	return nil
}

// Close releases resources owned by the kit.
func (k *WorkerKit) Close() error {
	return nil
}
