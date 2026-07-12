package temporal

import (
	"context"
	"testing"

	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func TestScheduleOptionsBuildsTemporalSkipSchedule(t *testing.T) {
	t.Parallel()
	schedule := testRecurringSchedule()

	options := scheduleOptions(schedule, "schedule-dispatch")
	require.Equal(t, schedule.ScheduleID, options.ID)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, options.Overlap)
	require.Equal(t, []string{"0 9 * * *"}, options.Spec.CronExpressions)
	require.Equal(t, "Asia/Shanghai", options.Spec.TimeZoneName)

	action, ok := options.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	require.Equal(t, ScheduleDispatchWorkflowName, action.Workflow)
	require.Equal(t, "schedule-dispatch", action.TaskQueue)
	require.Len(t, action.Args, 1)
	input, ok := action.Args[0].(scheduleDispatchWorkflowInput)
	require.True(t, ok)
	require.Equal(t, schedule.DispatchID, input.Dispatch.DispatchID)
	require.Equal(t, currentScheduleDispatchWorkflowVersion, input.WorkflowVersion)
}

func TestScheduleDispatchWorkflowDeliversOneDispatch(t *testing.T) {
	t.Parallel()

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(ScheduleDispatchWorkflow, workflow.RegisterOptions{Name: ScheduleDispatchWorkflowName})
	var received agentosproc.ScheduleDispatch
	env.RegisterActivityWithOptions(func(_ context.Context, dispatch agentosproc.ScheduleDispatch) error {
		received = dispatch
		return nil
	}, activity.RegisterOptions{Name: DispatchRecurringScheduleActivityName})

	env.ExecuteWorkflow(ScheduleDispatchWorkflow, scheduleDispatchWorkflowInput{
		Dispatch:        agentosproc.ScheduleDispatch{DispatchID: "dispatch-1"},
		WorkflowVersion: currentScheduleDispatchWorkflowVersion,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "dispatch-1", received.DispatchID)
}

func TestScheduleDispatchWorkflowRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	for _, input := range []scheduleDispatchWorkflowInput{
		{WorkflowVersion: currentScheduleDispatchWorkflowVersion},
		{Dispatch: agentosproc.ScheduleDispatch{DispatchID: "dispatch-1"}, WorkflowVersion: currentScheduleDispatchWorkflowVersion + 1},
	} {
		env := newAgentOSTemporalWorkflowTestEnv()
		env.RegisterWorkflowWithOptions(ScheduleDispatchWorkflow, workflow.RegisterOptions{Name: ScheduleDispatchWorkflowName})
		env.ExecuteWorkflow(ScheduleDispatchWorkflow, input)
		require.True(t, env.IsWorkflowCompleted())
		require.Error(t, env.GetWorkflowError())
	}
}

func TestRegisterScheduleDispatcher(t *testing.T) {
	t.Parallel()

	w := &fakeWorker{}
	err := RegisterScheduleDispatcher(w, func(context.Context, agentosproc.ScheduleDispatch) error { return nil })
	require.NoError(t, err)
	require.True(t, w.workflowRegistered(ScheduleDispatchWorkflowName))
	require.True(t, w.activityRegistered(DispatchRecurringScheduleActivityName))
}

func testRecurringSchedule() agentosproc.RecurringSchedule {
	return agentosproc.RecurringSchedule{
		ScheduleID:     "schedule-1",
		DispatchID:     "dispatch-1",
		CronExpression: "0 9 * * *",
		TimeZone:       "Asia/Shanghai",
		OverlapPolicy:  agentosproc.ScheduleOverlapSkip,
	}
}
