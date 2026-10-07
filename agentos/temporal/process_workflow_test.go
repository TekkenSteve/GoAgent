package temporal

import (
	"context"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestProcessWorkflowSignalResumesWaitingProcess(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-signal")
	env := newProcessWorkflowTestEnv(t)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "pause-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessSignalName, agentoscore.Signal{
			Type:           "external.update",
			IdempotencyKey: "signal-1",
		})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlCancel,
			IdempotencyKey: "cancel-1",
		})
	}, 3*time.Second)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentosproc.Status
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentosproc.ProcessCanceled, result.LifecycleState)
}

func TestProcessWorkflowRequiresActivityTaskQueue(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-invalid")
	env := newProcessWorkflowTestEnv(t)

	env.ExecuteWorkflow(ProcessWorkflow, &processWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestProcessWorkflowRequiresCurrentWorkflowVersion(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-version-required")
	env := newProcessWorkflowTestEnv(t)
	input := processWorkflowInputForTest(&spec)
	input.WorkflowVersion = currentProcessWorkflowVersion + 1

	env.ExecuteWorkflow(ProcessWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestProcessWorkflowContinuesAsNewWhenHistoryLimitReached(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-can-max-history")
	spec.Policy = agentosproc.Policy{MaxHistoryEvents: 10}
	env := newProcessWorkflowTestEnv(t)
	env.SetCurrentHistoryLength(10)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
}

func TestProcessWorkflowContinuesAsNewOnContinueAsNewEvents(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-can-proactive")
	spec.Policy = agentosproc.Policy{ContinueAsNewEvents: 10, MaxHistoryEvents: 20}
	env := newProcessWorkflowTestEnv(t)
	env.SetCurrentHistoryLength(10)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
}

func TestProcessWorkflowDoesNotContinueAsNewWithoutPolicy(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-can-disabled")
	env := newProcessWorkflowTestEnv(t)
	env.SetCurrentHistoryLength(1000)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlCancel,
			IdempotencyKey: "cancel-1",
		})
	}, time.Second)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentosproc.Status
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentosproc.ProcessCanceled, result.LifecycleState)
}

func TestEvaluateProcessContinuation(t *testing.T) {
	t.Parallel()

	require.False(t, evaluateProcessContinuation(nil, 1000).ShouldContinue)
	require.False(t, evaluateProcessContinuation(&agentosproc.Policy{}, 1000).ShouldContinue)
	require.False(t, evaluateProcessContinuation(&agentosproc.Policy{MaxHistoryEvents: 10}, 9).ShouldContinue)

	proactive := evaluateProcessContinuation(&agentosproc.Policy{ContinueAsNewEvents: 10, MaxHistoryEvents: 20}, 10)
	require.True(t, proactive.ShouldContinue)
	require.Equal(t, "continue_as_new_events", proactive.Reason)

	ceiling := evaluateProcessContinuation(&agentosproc.Policy{MaxHistoryEvents: 20}, 20)
	require.True(t, ceiling.ShouldContinue)
	require.Equal(t, "max_history_events", ceiling.Reason)
}

func TestContinuedProcessWorkflowInputCarriesTimers(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-can-continuation")
	spec.Policy = agentosproc.Policy{MaxHistoryEvents: 10}
	input := processWorkflowInputForTest(&spec)

	firedTimer := agentosproc.TimerSpec{TimerID: "survey", AfterSeconds: 60, Signal: "process.timer.survey"}
	timerState := []processWorkflowTimer{
		{Timer: firedTimer, At: time.Date(2026, 7, 4, 12, 1, 0, 0, time.UTC), Fired: true},
		{Timer: agentosproc.TimerSpec{TimerID: "follow-up", AfterSeconds: 300, Signal: "process.timer.follow-up"}, At: time.Date(2026, 7, 4, 12, 5, 0, 0, time.UTC), Fired: false},
	}
	timers := processWorkflowTimers{Timers: timerState}

	next := continuedProcessWorkflowInput(input, &timers)

	require.Equal(t, int32(1), next.ContinuationCount)
	require.Equal(t, input.Spec, next.Spec)
	require.Equal(t, input.WorkflowVersion, next.WorkflowVersion)
	require.Equal(t, timerState, next.Timers)

	// The continuation input must be independent of the live timer slice so
	// later fires in the current run do not mutate the carried state.
	require.NotSame(t, &timers.Timers[0], &next.Timers[0])
}

func TestNewProcessWorkflowTimersUsesCarriedState(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-timer-carry")
	spec.Timers = []agentosproc.TimerSpec{
		{TimerID: "follow-up", AfterSeconds: 300, Signal: "process.timer.follow-up"},
	}

	carried := []processWorkflowTimer{
		{Timer: spec.Timers[0], At: time.Date(2026, 7, 4, 12, 5, 0, 0, time.UTC), Fired: true},
	}

	// A carried timer that already fired must not be re-armed with a fresh
	// after_seconds schedule. The carried branch does not read the workflow
	// clock, so a nil context is sufficient for this unit test.
	timers := newProcessWorkflowTimers(nil, nil, processRefFromSpec(&spec), carried, spec.Timers)
	require.Len(t, timers.Timers, 1)
	require.True(t, timers.Timers[0].Fired)
	require.Equal(t, time.Date(2026, 7, 4, 12, 5, 0, 0, time.UTC), timers.Timers[0].At)
}

func newProcessWorkflowTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(ProcessWorkflow, workflow.RegisterOptions{Name: ProcessWorkflowName})

	activities := newTestProcessActivities(t)
	env.RegisterActivityWithOptions(activities.StartProcessActivity, activity.RegisterOptions{Name: StartProcessActivityName})
	env.RegisterActivityWithOptions(activities.SignalProcessActivity, activity.RegisterOptions{Name: SignalProcessActivityName})
	env.RegisterActivityWithOptions(activities.ControlProcessActivity, activity.RegisterOptions{Name: ControlProcessActivityName})
	env.RegisterActivityWithOptions(activities.FireProcessTimerActivity, activity.RegisterOptions{Name: FireProcessTimerActivityName})

	return env
}

func processWorkflowTestSpec(processID string) agentosproc.Spec {
	return agentosproc.Spec{
		ProcessID:      processID,
		Kind:           "resource-lifecycle",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "start-" + processID,
		Resource: agentosproc.ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-" + processID,
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		RequestedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
	}
}

func processWorkflowInputForTest(spec *agentosproc.Spec) *processWorkflowInput {
	return &processWorkflowInput{
		Spec:            *spec,
		WorkflowVersion: currentProcessWorkflowVersion,
		TaskQueues: processTaskQueues{
			ProcessActivity: DefaultTaskQueues().ProcessActivity,
		},
	}
}

func TestProcessActivitiesUseDurableRuntime(t *testing.T) {
	t.Parallel()

	store := agentosprocess.NewMemoryStore()
	activities, err := NewProcessActivities(store, store)
	require.NoError(t, err)

	spec := processWorkflowTestSpec("process-activity")
	status, err := activities.StartProcessActivity(context.Background(), &startProcessActivityInput{Spec: spec})
	require.NoError(t, err)
	require.Equal(t, agentosproc.ProcessRunning, status.LifecycleState)
}
