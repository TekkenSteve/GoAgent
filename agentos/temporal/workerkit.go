package temporal

import (
	"context"
	"errors"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerKit registers GoAgent workflows and activities into explicit workload workers.
type WorkerKit struct {
	activities            *orchestration.AgentActivities
	planActivities        *PlanActivities
	planCommandReconciler *planCommandReconciler
	closeFns              []func() error
}

var (
	errWorkerKitNilWorker                          = errors.New("agentos temporal workerkit: nil worker")
	errWorkerKitWorkersRequired                    = errors.New("agentos temporal workerkit: worker set is required")
	errWorkerKitPlanActivitiesRequired             = errors.New("agentos temporal workerkit: plan activities are required")
	errWorkerKitPlanCommandReconcilerNotConfigured = errors.New("agentos temporal workerkit: plan command reconciler is not configured")
)

// WorkerSet contains one Temporal worker per workload class.
type WorkerSet struct {
	PlanControl     worker.Worker
	PlanActivity    worker.Worker
	ProcessControl  worker.Worker
	ProcessActivity worker.Worker
	NativeControl   worker.Worker
	NativeLLM       worker.Worker
	NativeTool      worker.Worker
	Stream          worker.Worker
	Trigger         worker.Worker
}

// RegisterPlanWorkflow installs the AgentOS RunPlan workflow into an existing worker.
func RegisterPlanWorkflow(w worker.Worker) error {
	if w == nil {
		return errWorkerKitNilWorker
	}

	w.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{
		Name: PlanWorkflowName,
	})

	return nil
}

// RegisterPlanActivities installs AgentOS RunPlan activities into an existing worker.
func RegisterPlanActivities(w worker.Worker, activities *PlanActivities) error {
	if w == nil {
		return errWorkerKitNilWorker
	}

	if activities == nil {
		return errWorkerKitPlanActivitiesRequired
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

// NewWorkerKit creates a worker registration kit backed by the default
// Temporal/Postgres/Redis/Bifrost implementation.
func NewWorkerKit(ctx context.Context, cfg *WorkerConfig) (*WorkerKit, error) {
	return newWorkerKit(ctx, cfg)
}

// Register installs workflow definitions and activities into explicit workers.
func (k *WorkerKit) Register(workers *WorkerSet) error {
	if err := k.registerPlanWorkloads(workers); err != nil {
		return err
	}

	if k.activities == nil {
		return nil
	}

	return k.registerNativeWorkloads(workers)
}

func (k *WorkerKit) registerPlanWorkloads(workers *WorkerSet) error {
	if workers == nil || workers.PlanControl == nil || workers.PlanActivity == nil {
		return errWorkerKitWorkersRequired
	}

	workers.PlanControl.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{
		Name: PlanWorkflowName,
	})

	return RegisterPlanActivities(workers.PlanActivity, k.planActivities)
}

func (k *WorkerKit) registerNativeWorkloads(workers *WorkerSet) error {
	if workers.NativeControl == nil || workers.NativeLLM == nil || workers.NativeTool == nil || workers.Stream == nil || workers.Trigger == nil {
		return errWorkerKitWorkersRequired
	}

	k.registerNativeControlWorkloads(workers.NativeControl)
	k.registerNativeActivityWorkloads(workers.NativeLLM, workers.NativeTool)
	k.registerStreamWorkloads(workers.Stream)
	k.registerTriggerWorkloads(workers.Trigger)

	return nil
}

func (k *WorkerKit) registerNativeControlWorkloads(w worker.Worker) {
	w.RegisterWorkflowWithOptions(orchestration.AgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.AgentWorkflowName,
	})
	w.RegisterWorkflowWithOptions(orchestration.Workflow, workflow.RegisterOptions{
		Name: orchestration.OrchestrationWorkflowName,
	})
	w.RegisterActivityWithOptions(k.activities.PrepareActivity, activity.RegisterOptions{
		Name: orchestration.PrepareActivityName,
	})
}

func (k *WorkerKit) registerNativeActivityWorkloads(llmWorker, toolWorker worker.Worker) {
	llmWorker.RegisterActivityWithOptions(k.activities.LLMStepActivity, activity.RegisterOptions{
		Name: orchestration.LLMStepActivityName,
	})
	toolWorker.RegisterActivityWithOptions(k.activities.ToolExecActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecActivityName,
	})
}

func (k *WorkerKit) registerStreamWorkloads(w worker.Worker) {
	w.RegisterWorkflowWithOptions(orchestration.StreamAgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.StreamWorkflowName,
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
}

func (k *WorkerKit) registerTriggerWorkloads(w worker.Worker) {
	w.RegisterWorkflowWithOptions(orchestration.TriggerFireWorkflow, workflow.RegisterOptions{
		Name: orchestration.TriggerFireWorkflowName,
	})
	w.RegisterActivityWithOptions(k.activities.FireTriggerActivity, activity.RegisterOptions{
		Name: orchestration.FireTriggerActivityName,
	})
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
	if k == nil {
		return PlanCommandRecoveryResult{}, errWorkerKitPlanCommandReconcilerNotConfigured
	}

	if k.planCommandReconciler == nil {
		return PlanCommandRecoveryResult{}, errWorkerKitPlanCommandReconcilerNotConfigured
	}

	return k.planCommandReconciler.Recover(ctx, limit)
}

// StartPlanCommandRecovery starts periodic durable command outbox recovery for
// this worker kit.
func (k *WorkerKit) StartPlanCommandRecovery(ctx context.Context, cfg PlanCommandRecoveryLoopConfig, observer PlanCommandRecoveryObserver) (*PlanCommandRecoveryLoop, error) {
	if k == nil {
		return nil, errWorkerKitPlanCommandReconcilerNotConfigured
	}

	return StartPlanCommandRecovery(ctx, k, cfg, observer)
}

// Close releases resources owned by the kit.
func (k *WorkerKit) Close() error {
	if k == nil {
		return nil
	}

	var err error
	for _, closeFn := range k.closeFns {
		err = errors.Join(err, closeFn())
	}

	return err
}
