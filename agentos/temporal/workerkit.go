package temporal

import (
	"context"
	"errors"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerKit registers GoAgent workflows and activities into an existing worker.
type WorkerKit struct {
	activities     *orchestration.AgentActivities
	planActivities *PlanActivities
	closeFns       []func() error
}

// NewWorkerKit creates a worker registration kit backed by the default
// Temporal/Postgres/Redis/Bifrost implementation.
func NewWorkerKit(ctx context.Context, cfg WorkerConfig) (*WorkerKit, error) {
	return newWorkerKit(ctx, cfg)
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
	w.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{
		Name: PlanWorkflowName,
	})

	if k.activities == nil {
		return k.registerPlanActivities(w)
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

	return k.registerPlanActivities(w)
}

func (k *WorkerKit) registerPlanActivities(w worker.Worker) error {
	if k.planActivities == nil {
		return nil
	}

	w.RegisterActivityWithOptions(k.planActivities.ValidatePlanActivity, activity.RegisterOptions{
		Name: ValidatePlanActivityName,
	})
	w.RegisterActivityWithOptions(k.planActivities.StartPlanNodeActivity, activity.RegisterOptions{
		Name: StartPlanNodeActivityName,
	})
	w.RegisterActivityWithOptions(k.planActivities.StatusPlanNodeActivity, activity.RegisterOptions{
		Name: StatusPlanNodeActivityName,
	})
	w.RegisterActivityWithOptions(k.planActivities.ControlPlanNodeActivity, activity.RegisterOptions{
		Name: ControlPlanNodeActivityName,
	})
	w.RegisterActivityWithOptions(k.planActivities.PublishPlanArtifactsActivity, activity.RegisterOptions{
		Name: PublishPlanArtifactsActivityName,
	})
	w.RegisterActivityWithOptions(k.planActivities.PersistPlanStateActivity, activity.RegisterOptions{
		Name: PersistPlanStateActivityName,
	})

	return nil
}

// Close releases resources owned by the kit.
func (k *WorkerKit) Close() error {
	var err error
	for _, closeFn := range k.closeFns {
		err = errors.Join(err, closeFn())
	}

	return err
}
