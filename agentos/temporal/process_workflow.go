package temporal

import (
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// ProcessWorkflowName is the Temporal workflow type that runs durable AgentOS processes.
	ProcessWorkflowName = "AgentOSProcessWorkflow"
	// ProcessStatusQueryName is the Temporal query name that returns the process's status.
	ProcessStatusQueryName = "agentos.process.status"
	// ProcessSignalName is the Temporal signal name that delivers process-level signals.
	ProcessSignalName = "agentos.process.signal"
	// ProcessControlSignalName is the Temporal signal name that delivers process control requests.
	ProcessControlSignalName = "agentos.process.control"
	// StartProcessActivityName is the Temporal activity name that starts a process.
	StartProcessActivityName = "AgentOSStartProcess"
	// SignalProcessActivityName is the Temporal activity name that signals a process.
	SignalProcessActivityName = "AgentOSSignalProcess"
	// ControlProcessActivityName is the Temporal activity name that sends control to a process.
	ControlProcessActivityName = "AgentOSControlProcess"
	// FireProcessTimerActivityName is the Temporal activity name that fires a process timer.
	FireProcessTimerActivityName     = "AgentOSFireProcessTimer"
	defaultProcessSignalBufferLength = 64

	// processContinueAsNewVersionMarker is the workflow.GetVersion marker that
	// gates Continue-As-New in ProcessWorkflow. Executions that started before
	// this marker keep the pre-CAN loop (no continuation checks) so their
	// history and timer behavior replay unchanged; executions started after it
	// use the bounded-history loop that periodically compact the workflow
	// history.
	processContinueAsNewVersionMarker = "agentos-process-continue-as-new"

	// processSearchAttributesVersionMarker is the workflow.GetVersion marker that
	// gates lifecycle search-attribute upserts. Upserts emit history events, so
	// executions started before the marker replay without them; new executions
	// keep goagent.lifecycle_state fresh on every lifecycle transition.
	processSearchAttributesVersionMarker = "agentos-process-search-attributes"
)

type processWorkflowInput struct {
	Spec              agentosproc.Spec       `json:"spec"`
	TaskQueues        processTaskQueues      `json:"task_queues"`
	WorkflowVersion   int                    `json:"workflow_version"`
	ContinuationCount int32                  `json:"continuation_count,omitempty"`
	Timers            []processWorkflowTimer `json:"timers,omitempty"`
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

	// Executions started before the Continue-As-New change replay the pre-CAN
	// loop so their history, timer schedule, and deduplication behavior stay
	// unchanged. New executions use the bounded-history loop.
	continueAsNewEnabled := workflow.GetVersion(ctx, processContinueAsNewVersionMarker, workflow.DefaultVersion, 1) >= 1
	// Executions started before the search-attributes change replay without the
	// lifecycle upserts so their history stays identical; new executions keep
	// the lifecycle visible to operators on every transition.
	processSearchAttributesEnabled := workflow.GetVersion(ctx, processSearchAttributesVersionMarker, workflow.DefaultVersion, 1)

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
	timers := newProcessWorkflowTimers(ctx, activityCtx, ref, input.Timers, input.Spec.Timers)
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(signalCh, processWorkflowReceiveSignal(ctx, activityCtx, ref, &status))
	selector.AddReceive(controlCh, processWorkflowReceiveControl(ctx, activityCtx, ref, &status))
	timers.AddToSelector(ctx, selector, &status)

	// Sync on every lifecycle transition so a blocked/failed/succeeded process
	// is immediately queryable; change-detection avoids no-op upserts on
	// signal-only iterations bloating history.
	lifecycleSync := newLifecycleSearchAttributeSync(processSearchAttributesEnabled, input.Spec.ProcessID)

	for !processWorkflowTerminal(status.LifecycleState) {
		if canErr := processWorkflowContinuation(ctx, input, &timers, continueAsNewEnabled); canErr != nil {
			lifecycleSync.sync(ctx, status.LifecycleState)

			return status, canErr
		}

		selector.Select(ctx)
		lifecycleSync.sync(ctx, status.LifecycleState)
	}

	lifecycleSync.sync(ctx, status.LifecycleState)

	return status, nil
}

// processWorkflowReceive returns the selector callback that applies a
// channel-delivered request (signal or control) through the durable process
// backend, failing the process on backend error. decode reads the request off
// the channel into the concrete activity input.
func processWorkflowReceive(ctx, activityCtx workflow.Context, status *agentosproc.Status, activityName string, decode func(workflow.ReceiveChannel) any) func(workflow.ReceiveChannel, bool) {
	return func(ch workflow.ReceiveChannel, _ bool) {
		input := decode(ch)

		var next agentosproc.Status
		if err := workflow.ExecuteActivity(activityCtx, activityName, input).Get(activityCtx, &next); err != nil {
			failProcessWorkflowStatus(status, workflow.Now(ctx), err)

			return
		}

		*status = next
	}
}

// processWorkflowReceiveSignal returns the selector callback that applies a
// process-level signal through the durable process backend.
func processWorkflowReceiveSignal(ctx, activityCtx workflow.Context, ref agentosproc.Ref, status *agentosproc.Status) func(workflow.ReceiveChannel, bool) {
	return processWorkflowReceive(ctx, activityCtx, status, SignalProcessActivityName, func(ch workflow.ReceiveChannel) any {
		var signal agentoscore.Signal
		ch.Receive(ctx, &signal)

		return signalProcessActivityInput{Ref: ref, Signal: signal}
	})
}

// processWorkflowReceiveControl returns the selector callback that applies a
// control request (pause/cancel/resume) through the durable process backend.
func processWorkflowReceiveControl(ctx, activityCtx workflow.Context, ref agentosproc.Ref, status *agentosproc.Status) func(workflow.ReceiveChannel, bool) {
	return processWorkflowReceive(ctx, activityCtx, status, ControlProcessActivityName, func(ch workflow.ReceiveChannel) any {
		var control agentoscore.ControlRequest
		ch.Receive(ctx, &control)

		return controlProcessActivityInput{Ref: ref, Control: control}
	})
}

// processWorkflowContinuation returns a non-nil Continue-As-New error when the
// current run should compact its history. The durable process store is the
// source of truth for status and events, so a continuation re-runs
// StartProcessActivity idempotently and picks up any state changed since the
// prior run. Signals delivered to the same workflow ID during the continuation
// handoff are not replayed here; idempotency keys make re-delivered signals
// safe, and the handoff window is brief.
func processWorkflowContinuation(ctx workflow.Context, input *processWorkflowInput, timers *processWorkflowTimers, enabled bool) error {
	if !enabled {
		return nil
	}

	decision := evaluateProcessContinuation(&input.Spec.Policy, workflow.GetInfo(ctx).GetCurrentHistoryLength())
	if !decision.ShouldContinue {
		return nil
	}

	nextInput := continuedProcessWorkflowInput(input, timers)

	return workflow.NewContinueAsNewError(ctx, ProcessWorkflowName, nextInput)
}

// processContinuationDecision explains whether the current process workflow run
// should continue as new before accumulating more history.
type processContinuationDecision struct {
	ShouldContinue bool
	Reason         string
}

// evaluateProcessContinuation evaluates the durable process history guards from
// deterministic workflow inputs. The proactive ContinueAsNewEvents threshold
// triggers before the MaxHistoryEvents ceiling when both are configured
// (validation requires continue-as-new events <= max history events).
func evaluateProcessContinuation(policy *agentosproc.Policy, historyLength int) processContinuationDecision {
	if policy == nil {
		return processContinuationDecision{}
	}

	if policy.ContinueAsNewEvents > 0 && historyLength >= int(policy.ContinueAsNewEvents) {
		return processContinuationDecision{ShouldContinue: true, Reason: "continue_as_new_events"}
	}

	if policy.MaxHistoryEvents > 0 && historyLength >= int(policy.MaxHistoryEvents) {
		return processContinuationDecision{ShouldContinue: true, Reason: "max_history_events"}
	}

	return processContinuationDecision{}
}

// continuedProcessWorkflowInput carries the continuity state across a
// Continue-As-New boundary. The Spec is preserved so the continuation re-runs
// StartProcessActivity idempotently against the durable process store; the
// timer slice keeps the already-computed fire times and fired flags so
// after_seconds timers do not drift when rescheduled on the new run.
func continuedProcessWorkflowInput(input *processWorkflowInput, timers *processWorkflowTimers) processWorkflowInput {
	next := *input
	next.ContinuationCount++

	next.Timers = append([]processWorkflowTimer(nil), timers.Timers...)

	return next
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
	Timer agentosproc.TimerSpec `json:"timer"`
	At    time.Time             `json:"at,omitzero" schema:"optional"`
	Fired bool                  `json:"fired,omitempty"`
}

type processWorkflowTimers struct {
	ActivityCtx workflow.Context
	Ref         agentosproc.Ref
	Timers      []processWorkflowTimer
}

func newProcessWorkflowTimers(ctx, activityCtx workflow.Context, ref agentosproc.Ref, carried []processWorkflowTimer, specs []agentosproc.TimerSpec) processWorkflowTimers {
	// A continuation carries the computed fire times and fired flags so the
	// re-armed timers fire on the same schedule. Without this, after_seconds
	// timers would be rescheduled relative to the new run and drift.
	if len(carried) > 0 {
		return processWorkflowTimers{
			ActivityCtx: activityCtx,
			Ref:         ref,
			Timers:      append([]processWorkflowTimer(nil), carried...),
		}
	}

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
