package temporal

import (
	"context"
	"errors"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// WorkerKit registers GoAgent workflows and activities into explicit workload workers.
type WorkerKit struct {
	activities            *orchestration.AgentActivities
	planActivities        *PlanActivities
	processActivities     *ProcessActivities
	planCommandReconciler *planCommandReconciler
	closeFns              []func() error
}

// PlanWorkerKit owns only the durable RunPlan workload. Applications that use
// external backends do not need to construct unrelated native-agent or process
// workers merely to run AgentOS PlanControl.
type PlanWorkerKit struct {
	planActivities        *PlanActivities
	planCommandReconciler *planCommandReconciler
	closeFns              []func() error
}

// PlanWorkerSet identifies the two Temporal workers required by RunPlan.
type PlanWorkerSet struct {
	Control  WorkloadRegistrar
	Activity WorkloadRegistrar
}

var (
	errWorkerKitNilWorker                          = errors.New("agentos temporal workerkit: nil worker")
	errWorkerKitWorkersRequired                    = errors.New("agentos temporal workerkit: worker set is required")
	errWorkerKitPlanActivitiesRequired             = errors.New("agentos temporal workerkit: plan activities are required")
	errWorkerKitProcessActivitiesRequired          = errors.New("agentos temporal workerkit: process activities are required")
	errWorkerKitPlanCommandReconcilerNotConfigured = errors.New("agentos temporal workerkit: plan command reconciler is not configured")
)

// WorkloadRegistrar is the smallest Temporal worker capability AgentOS needs
// to install its workloads. Hosts retain ownership of worker construction and
// lifecycle, so registration does not depend on unrelated SDK worker methods.
type WorkloadRegistrar interface {
	RegisterWorkflowWithOptions(any, workflow.RegisterOptions)
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}

// WorkerSet contains one workload registrar per Temporal workload class.
type WorkerSet struct {
	PlanControl     WorkloadRegistrar
	PlanActivity    WorkloadRegistrar
	ProcessControl  WorkloadRegistrar
	ProcessActivity WorkloadRegistrar
	NativeControl   WorkloadRegistrar
	NativeLLM       WorkloadRegistrar
	NativeTool      WorkloadRegistrar
	Stream          WorkloadRegistrar
}

// RegisterPlanWorkflow installs the AgentOS RunPlan workflow into an existing worker.
func RegisterPlanWorkflow(w WorkloadRegistrar) error {
	if w == nil {
		return errWorkerKitNilWorker
	}

	w.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{
		Name: PlanWorkflowName,
	})

	return nil
}

// RegisterPlanActivities installs AgentOS RunPlan activities into an existing worker.
func RegisterPlanActivities(w WorkloadRegistrar, activities *PlanActivities) error {
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
	w.RegisterActivityWithOptions(activities.SignalPlanNodeActivity, activity.RegisterOptions{
		Name: SignalPlanNodeActivityName,
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

// RegisterProcessWorkflow installs the AgentOS process workflow into an existing worker.
func RegisterProcessWorkflow(w WorkloadRegistrar) error {
	if w == nil {
		return errWorkerKitNilWorker
	}

	w.RegisterWorkflowWithOptions(ProcessWorkflow, workflow.RegisterOptions{
		Name: ProcessWorkflowName,
	})

	return nil
}

// RegisterProcessActivities installs AgentOS process activities into an existing worker.
func RegisterProcessActivities(w WorkloadRegistrar, activities *ProcessActivities) error {
	if w == nil {
		return errWorkerKitNilWorker
	}

	if activities == nil {
		return errWorkerKitProcessActivitiesRequired
	}

	w.RegisterActivityWithOptions(activities.StartProcessActivity, activity.RegisterOptions{
		Name: StartProcessActivityName,
	})
	w.RegisterActivityWithOptions(activities.SignalProcessActivity, activity.RegisterOptions{
		Name: SignalProcessActivityName,
	})
	w.RegisterActivityWithOptions(activities.ControlProcessActivity, activity.RegisterOptions{
		Name: ControlProcessActivityName,
	})
	w.RegisterActivityWithOptions(activities.FireProcessTimerActivity, activity.RegisterOptions{
		Name: FireProcessTimerActivityName,
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

	if err := k.registerProcessWorkloads(workers); err != nil {
		return err
	}

	if k.activities == nil {
		return nil
	}

	return k.registerNativeWorkloads(workers)
}

// Register installs only the RunPlan workflow and activities.
func (k *PlanWorkerKit) Register(workers *PlanWorkerSet) error {
	if k == nil || workers == nil || workers.Control == nil || workers.Activity == nil {
		return errWorkerKitWorkersRequired
	}

	if err := RegisterPlanWorkflow(workers.Control); err != nil {
		return err
	}

	return RegisterPlanActivities(workers.Activity, k.planActivities)
}

// RecoverPlanCommands redelivers pending RunPlan commands without requiring
// process or native-agent workers.
func (k *PlanWorkerKit) RecoverPlanCommands(ctx context.Context, limit int) (PlanCommandRecoveryResult, error) {
	if k == nil || k.planCommandReconciler == nil {
		return PlanCommandRecoveryResult{}, errWorkerKitPlanCommandReconcilerNotConfigured
	}

	return k.planCommandReconciler.Recover(ctx, limit)
}

// StartPlanCommandRecovery starts periodic recovery for this plan-only kit.
func (k *PlanWorkerKit) StartPlanCommandRecovery(ctx context.Context, cfg PlanCommandRecoveryLoopConfig, observer PlanCommandRecoveryObserver) (*PlanCommandRecoveryLoop, error) {
	if k == nil {
		return nil, errWorkerKitPlanCommandReconcilerNotConfigured
	}

	return StartPlanCommandRecovery(ctx, k, cfg, observer)
}

func (k *WorkerKit) registerProcessWorkloads(workers *WorkerSet) error {
	if workers == nil || workers.ProcessControl == nil || workers.ProcessActivity == nil {
		return errWorkerKitWorkersRequired
	}

	if err := RegisterProcessWorkflow(workers.ProcessControl); err != nil {
		return err
	}

	return RegisterProcessActivities(workers.ProcessActivity, k.processActivities)
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
	if workers.NativeControl == nil || workers.NativeLLM == nil || workers.NativeTool == nil || workers.Stream == nil {
		return errWorkerKitWorkersRequired
	}

	k.registerNativeControlWorkloads(workers.NativeControl)
	k.registerNativeActivityWorkloads(workers.NativeLLM, workers.NativeTool)
	k.registerStreamWorkloads(workers.Stream)

	return nil
}

func (k *WorkerKit) registerNativeControlWorkloads(w WorkloadRegistrar) {
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

func (k *WorkerKit) registerNativeActivityWorkloads(llmWorker, toolWorker WorkloadRegistrar) {
	llmWorker.RegisterActivityWithOptions(k.activities.LLMStepActivity, activity.RegisterOptions{
		Name: orchestration.LLMStepActivityName,
	})
	toolWorker.RegisterActivityWithOptions(k.activities.ToolExecActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecActivityName,
	})
}

func (k *WorkerKit) registerStreamWorkloads(w WorkloadRegistrar) {
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

// Close releases resources owned by the plan-only kit.
func (k *PlanWorkerKit) Close() error {
	if k == nil {
		return nil
	}

	var err error
	for _, closeFn := range k.closeFns {
		err = errors.Join(err, closeFn())
	}

	return err
}
