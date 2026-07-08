package temporal

import (
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ProcessWorkflowName              = "AgentOSProcessWorkflow"
	ProcessStatusQueryName           = "agentos.process.status"
	ProcessSignalName                = "agentos.process.signal"
	ProcessControlSignalName         = "agentos.process.control"
	StartProcessActivityName         = "AgentOSStartProcess"
	SignalProcessActivityName        = "AgentOSSignalProcess"
	ControlProcessActivityName       = "AgentOSControlProcess"
	FireProcessTimerActivityName     = "AgentOSFireProcessTimer"
	defaultProcessSignalBufferLength = 64
)

type processWorkflowInput struct {
	Spec            agentosproc.Spec  `json:"spec"`
	TaskQueues      processTaskQueues `json:"task_queues"`
	WorkflowVersion int               `json:"workflow_version"`
}

type processTaskQueues struct {
	ProcessActivity string `json:"process_activity"`
}

// ProcessWorkflow runs one coarse-grained durable process for an application
// resource. It owns process lifecycle signals and timers, not backend internals.
func ProcessWorkflow(ctx workflow.Context, input *processWorkflowInput) (agentosproc.Status, error) {
	if input == nil {
		return agentosproc.Status{}, temporal.NewNonRetryableApplicationError("ProcessWorkflow input is required", "validation", nil)
	}

	if input.TaskQueues.ProcessActivity == "" {
		return agentosproc.Status{}, temporal.NewNonRetryableApplicationError("ProcessWorkflow process activity task queue is required", "validation", nil)
	}

	if err := validateProcessWorkflowVersion(input.WorkflowVersion); err != nil {
		return agentosproc.Status{}, temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
	}

	activityCtx := newProcessWorkflowActivityContext(ctx, input.TaskQueues.ProcessActivity)

	var status agentosproc.Status
	if err := workflow.ExecuteActivity(activityCtx, StartProcessActivityName, startProcessActivityInput{Spec: input.Spec}).Get(activityCtx, &status); err != nil {
		return status, err
	}

	if err := workflow.SetQueryHandler(ctx, ProcessStatusQueryName, func() (agentosproc.Status, error) {
		return status, nil
	}); err != nil {
		return status, err
	}

	signalCh := workflow.GetSignalChannel(ctx, ProcessSignalName)
	controlCh := workflow.GetSignalChannel(ctx, ProcessControlSignalName)
	ref := processRefFromSpec(&input.Spec)
	timers := newProcessWorkflowTimers(ctx, activityCtx, ref, input.Spec.Timers)
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(signalCh, func(ch workflow.ReceiveChannel, _ bool) {
		var signal agentoscore.Signal
		ch.Receive(ctx, &signal)

		var next agentosproc.Status
		if err := workflow.ExecuteActivity(activityCtx, SignalProcessActivityName, signalProcessActivityInput{Ref: ref, Signal: signal}).Get(activityCtx, &next); err != nil {
			failProcessWorkflowStatus(&status, workflow.Now(ctx), err)

			return
		}

		status = next
	})
	selector.AddReceive(controlCh, func(ch workflow.ReceiveChannel, _ bool) {
		var control agentoscore.ControlRequest
		ch.Receive(ctx, &control)

		var next agentosproc.Status
		if err := workflow.ExecuteActivity(activityCtx, ControlProcessActivityName, controlProcessActivityInput{Ref: ref, Control: control}).Get(activityCtx, &next); err != nil {
			failProcessWorkflowStatus(&status, workflow.Now(ctx), err)

			return
		}

		status = next
	})
	timers.AddToSelector(ctx, selector, &status)

	for !processWorkflowTerminal(status.LifecycleState) {
		selector.Select(ctx)
	}

	return status, nil
}

func newProcessWorkflowActivityContext(ctx workflow.Context, taskQueue string) workflow.Context {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: defaultActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: defaultActivityMaxAttempts,
		},
	})

	return workflow.WithTaskQueue(activityCtx, taskQueue)
}

type processWorkflowTimer struct {
	Timer agentosproc.TimerSpec
	At    time.Time
	Fired bool
}

type processWorkflowTimers struct {
	ActivityCtx workflow.Context
	Ref         agentosproc.Ref
	Timers      []processWorkflowTimer
}

func newProcessWorkflowTimers(ctx, activityCtx workflow.Context, ref agentosproc.Ref, specs []agentosproc.TimerSpec) processWorkflowTimers {
	now := workflow.Now(ctx)

	timers := make([]processWorkflowTimer, 0, len(specs))
	for i := range specs {
		timer := specs[i]

		at := timer.FireAt
		if at.IsZero() {
			at = now.Add(time.Duration(timer.AfterSeconds) * time.Second)
		}

		timers = append(timers, processWorkflowTimer{Timer: timer, At: at})
	}

	return processWorkflowTimers{
		ActivityCtx: activityCtx,
		Ref:         ref,
		Timers:      timers,
	}
}

func (t *processWorkflowTimers) AddToSelector(ctx workflow.Context, selector workflow.Selector, status *agentosproc.Status) {
	now := workflow.Now(ctx)

	for i := range t.Timers {
		if t.Timers[i].Fired {
			continue
		}

		duration := max(t.Timers[i].At.Sub(now), 0)

		index := i
		future := workflow.NewTimer(ctx, duration)
		selector.AddFuture(future, func(workflow.Future) {
			timer := &t.Timers[index]
			timer.Fired = true

			var next agentosproc.Status

			input := fireProcessTimerActivityInput{
				Ref:   t.Ref,
				Timer: timer.Timer,
				At:    timer.At.Format(time.RFC3339Nano),
			}
			if err := workflow.ExecuteActivity(t.ActivityCtx, FireProcessTimerActivityName, input).Get(t.ActivityCtx, &next); err != nil {
				failProcessWorkflowStatus(status, workflow.Now(ctx), err)

				return
			}

			*status = next
		})
	}
}

func failProcessWorkflowStatus(status *agentosproc.Status, now time.Time, err error) {
	status.LifecycleState = agentosproc.ProcessFailed
	status.Reason = err.Error()
	status.UpdatedAt = now
}

func processWorkflowTerminal(lifecycle string) bool {
	switch lifecycle {
	case agentosproc.ProcessSucceeded, agentosproc.ProcessFailed, agentosproc.ProcessCanceled:
		return true
	default:
		return false
	}
}

func processRefFromSpec(spec *agentosproc.Spec) agentosproc.Ref {
	return agentosproc.Ref{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}

func processWorkflowID(processID string) string {
	return "agentos-process-" + processID
}
