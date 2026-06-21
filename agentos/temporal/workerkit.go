package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerKit registers GoAgent workflows and activities into an existing worker.
type WorkerKit struct {
	activities            *orchestration.AgentActivities
	planActivities        *PlanActivities
	planCommandReconciler *planCommandReconciler
	closeFns              []func() error
}

// RegisterPlanWorkflow installs the AgentOS RunPlan workflow into an existing worker.
func RegisterPlanWorkflow(w worker.Worker) error {
	if w == nil {
		return errors.New("agentos temporal workerkit: nil worker")
	}

	w.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{
		Name: PlanWorkflowName,
	})

	return nil
}

// RegisterPlanActivities installs AgentOS RunPlan activities into an existing worker.
func RegisterPlanActivities(w worker.Worker, activities *PlanActivities) error {
	if w == nil {
		return errors.New("agentos temporal workerkit: nil worker")
	}
	if activities == nil {
		return errors.New("agentos temporal workerkit: plan activities are required")
	}

	w.RegisterActivityWithOptions(activities.ValidatePlanActivity, activity.RegisterOptions{
		Name: ValidatePlanActivityName,
	})
	w.RegisterActivityWithOptions(activities.ResolvePlanNodeInputActivity, activity.RegisterOptions{
		Name: ResolvePlanNodeInputActivityName,
	})
	w.RegisterActivityWithOptions(activities.StartPlanNodeActivity, activity.RegisterOptions{
		Name: StartPlanNodeActivityName,
	})
	w.RegisterActivityWithOptions(activities.StatusPlanNodeActivity, activity.RegisterOptions{
		Name: StatusPlanNodeActivityName,
	})
	w.RegisterActivityWithOptions(activities.ControlPlanNodeActivity, activity.RegisterOptions{
		Name: ControlPlanNodeActivityName,
	})
	w.RegisterActivityWithOptions(activities.PublishPlanArtifactsActivity, activity.RegisterOptions{
		Name: PublishPlanArtifactsActivityName,
	})
	w.RegisterActivityWithOptions(activities.EvaluatePlanExpansionActivity, activity.RegisterOptions{
		Name: EvaluatePlanExpansionActivityName,
	})
	w.RegisterActivityWithOptions(activities.PersistPlanStateActivity, activity.RegisterOptions{
		Name: PersistPlanStateActivityName,
	})

	return nil
}

// RegisterPlan installs the AgentOS RunPlan workflow and activities into an existing worker.
func RegisterPlan(w worker.Worker, activities *PlanActivities) error {
	if err := RegisterPlanWorkflow(w); err != nil {
		return err
	}
	if err := RegisterPlanActivities(w, activities); err != nil {
		return fmt.Errorf("agentos temporal workerkit - plan activities: %w", err)
	}

	return nil
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
	if err := RegisterPlanWorkflow(w); err != nil {
		return err
	}

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
	return RegisterPlanActivities(w, k.planActivities)
}

// PlanCommandRecoveryResult summarizes one durable command outbox recovery pass.
type PlanCommandRecoveryResult struct {
	Scanned   int
	Delivered int
	Failed    int
}

// RecoverPlanCommands redelivers pending/failed RunPlan control-plane commands
// from the durable outbox.
func (k *WorkerKit) RecoverPlanCommands(ctx context.Context, limit int) (PlanCommandRecoveryResult, error) {
	if k.planCommandReconciler == nil {
		return PlanCommandRecoveryResult{}, errors.New("agentos temporal workerkit: plan command reconciler is not configured")
	}

	return k.planCommandReconciler.Recover(ctx, limit)
}

// StartPlanCommandRecovery starts periodic durable command outbox recovery for
// this worker kit.
func (k *WorkerKit) StartPlanCommandRecovery(ctx context.Context, cfg PlanCommandRecoveryLoopConfig, observer PlanCommandRecoveryObserver) (*PlanCommandRecoveryLoop, error) {
	return StartPlanCommandRecovery(ctx, k, cfg, observer)
}

// Close releases resources owned by the kit.
func (k *WorkerKit) Close() error {
	var err error
	for _, closeFn := range k.closeFns {
		err = errors.Join(err, closeFn())
	}

	return err
}
