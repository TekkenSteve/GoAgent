package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalmocks "go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/workflow"
)

var errTemporaryTriggerDispatcherOutage = errors.New("temporary trigger dispatcher outage")

func TestTriggerRuntimeCreateMapsPortableTriggerDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedules := temporalmocks.NewScheduleClient(t)
	spec := temporalTestTriggerSpec()
	spec.Timing.Calendars = []agentosproc.CalendarSpec{{Minute: []agentosproc.CalendarRange{{Start: 15}}}}
	spec.Timing.Intervals = []agentosproc.IntervalSpec{{Every: time.Hour, Offset: 10 * time.Minute}}
	spec.Timing.Exclusions = []agentosproc.CalendarSpec{{DayOfWeek: []agentosproc.CalendarRange{{Start: 0, End: 0}}}}
	spec.Timing.StartAt = time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	spec.Timing.EndAt = time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	spec.Timing.Jitter = time.Minute
	spec.Policy.PauseOnFailure = true
	spec.Policy.Overlap = agentosproc.TriggerOverlapCancelPrevious

	schedules.On("Create", ctx, mock.MatchedBy(func(options client.ScheduleOptions) bool {
		require.Equal(t, temporalTriggerScheduleID(&spec.TriggerRef), options.ID)
		require.Equal(t, spec.Timing.CronExpressions, options.Spec.CronExpressions)
		require.Equal(t, spec.Timing.Calendars[0].Minute[0].Start, options.Spec.Calendars[0].Minute[0].Start)
		require.Equal(t, spec.Timing.Intervals[0], agentosproc.IntervalSpec{Every: options.Spec.Intervals[0].Every, Offset: options.Spec.Intervals[0].Offset})
		require.Equal(t, temporalOverlapPolicy(spec.Policy.Overlap), options.Overlap)
		require.Equal(t, spec.Policy.CatchupWindow, options.CatchupWindow)
		require.True(t, options.PauseOnFailure)
		require.True(t, options.Paused)
		require.Equal(t, "review required", options.Note)
		action, ok := options.Action.(*client.ScheduleWorkflowAction)
		require.True(t, ok)
		require.Equal(t, temporalTriggerWorkflowID(&spec.TriggerRef), action.ID)
		require.Equal(t, TriggerDispatchWorkflowName, action.Workflow)
		require.Equal(t, "trigger-dispatch", action.TaskQueue)

		return true
	})).Return(nil, nil).Once()

	runtime, err := newTriggerRuntime(schedules, "trigger-dispatch")
	require.NoError(t, err)
	require.NoError(t, runtime.CreateTrigger(ctx, &spec, agentosproc.TriggerCreateOptions{Paused: true, Note: " review required "}))
}

func TestTemporalTriggerScheduleIDIsTenantScopedAndOpaque(t *testing.T) {
	t.Parallel()

	first := temporalTestTriggerSpec().TriggerRef
	second := first
	second.AccountID = "account-2"

	firstID := temporalTriggerScheduleID(&first)
	secondID := temporalTriggerScheduleID(&second)
	require.NotEqual(t, firstID, secondID)
	require.False(t, strings.Contains(firstID, first.TriggerID))
	require.False(t, strings.Contains(firstID, first.AccountID))
}

func TestTriggerRuntimeUpdatePreservesLifecycleState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedules := temporalmocks.NewScheduleClient(t)
	handle := temporalmocks.NewScheduleHandle(t)
	spec := temporalTestTriggerSpec()
	paused := &client.ScheduleState{Paused: true, Note: "waiting for review"}

	schedules.On("GetHandle", ctx, temporalTriggerScheduleID(&spec.TriggerRef)).Return(handle).Once()
	handle.On("Update", ctx, mock.Anything).Run(func(args mock.Arguments) {
		options, ok := args.Get(1).(client.ScheduleUpdateOptions)
		require.True(t, ok)

		update, err := options.DoUpdate(client.ScheduleUpdateInput{Description: client.ScheduleDescription{Schedule: client.Schedule{State: paused}}})
		require.NoError(t, err)
		require.Same(t, paused, update.Schedule.State)
		action, ok := update.Schedule.Action.(*client.ScheduleWorkflowAction)
		require.True(t, ok)
		require.Equal(t, temporalTriggerWorkflowID(&spec.TriggerRef), action.ID)
	}).Return(nil).Once()

	runtime, err := newTriggerRuntime(schedules, "trigger-dispatch")
	require.NoError(t, err)
	require.NoError(t, runtime.UpdateTrigger(ctx, &spec))
}

func TestTriggerRuntimeObserveProjectsTemporalState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedules := temporalmocks.NewScheduleClient(t)
	handle := temporalmocks.NewScheduleHandle(t)
	spec := temporalTestTriggerSpec()
	now := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	description := &client.ScheduleDescription{
		Schedule: client.Schedule{State: &client.ScheduleState{Paused: true, Note: "paused by operator"}},
		Info: client.ScheduleInfo{
			NumActions:                    4,
			NumActionsSkippedOverlap:      2,
			NumActionsMissedCatchupWindow: 1,
			NextActionTimes:               []time.Time{now.Add(time.Hour)},
			RunningWorkflows:              []client.ScheduleWorkflowExecution{{WorkflowID: "delivery-1", FirstExecutionRunID: "run-1"}},
			RecentActions: []client.ScheduleActionResult{{
				ScheduleTime:        now.Add(-time.Hour),
				ActualTime:          now.Add(-time.Hour + time.Second),
				StartWorkflowResult: &client.ScheduleWorkflowExecution{WorkflowID: "delivery-0", FirstExecutionRunID: "run-0"},
			}},
			CreatedAt:    now.Add(-24 * time.Hour),
			LastUpdateAt: now,
		},
	}

	schedules.On("GetHandle", ctx, temporalTriggerScheduleID(&spec.TriggerRef)).Return(handle).Once()
	handle.On("Describe", ctx).Return(description, nil).Once()

	runtime, err := newTriggerRuntime(schedules, "trigger-dispatch")
	require.NoError(t, err)
	observation, err := runtime.ObserveTrigger(ctx, &spec.TriggerRef)
	require.NoError(t, err)
	require.Equal(t, spec.TriggerRef, observation.Trigger)
	require.True(t, observation.State.Paused)
	require.Equal(t, "paused by operator", observation.State.Note)
	require.Equal(t, 4, observation.DeliveryCount)
	require.Len(t, observation.RunningDeliveries, 1)
	require.Equal(t, "delivery-0", observation.RecentDeliveries[0].WorkflowID)
	require.Equal(t, now.Add(-time.Hour), observation.RecentDeliveries[0].ScheduledAt)
}

func TestTriggerDispatchWorkflowDeliversUniqueExecutionIdentity(t *testing.T) {
	t.Parallel()

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(TriggerDispatchWorkflow, workflow.RegisterOptions{Name: TriggerDispatchWorkflowName})

	var delivered agentosproc.TriggerDelivery

	env.RegisterActivityWithOptions(func(_ context.Context, delivery agentosproc.TriggerDelivery) error {
		delivered = delivery

		return nil
	}, activity.RegisterOptions{Name: DispatchTriggerActivityName})

	spec := temporalTestTriggerSpec()

	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "trigger-delivery-test"})
	env.ExecuteWorkflow(TriggerDispatchWorkflow, &triggerDispatchWorkflowInput{
		Trigger:         spec.TriggerRef,
		Target:          spec.Target,
		WorkflowVersion: currentTriggerDispatchWorkflowVersion,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, spec.TriggerRef, delivered.Trigger)
	require.Equal(t, spec.Target, delivered.Target)
	require.NotEmpty(t, delivered.DeliveryID)
	require.Contains(t, delivered.DeliveryID, "trigger-delivery-test:")
	require.False(t, delivered.TriggeredAt.IsZero())
}

func TestTriggerDispatchWorkflowRetriesWithStableDeliveryIdentity(t *testing.T) {
	t.Parallel()

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(TriggerDispatchWorkflow, workflow.RegisterOptions{Name: TriggerDispatchWorkflowName})

	var deliveries []agentosproc.TriggerDelivery

	env.RegisterActivityWithOptions(func(_ context.Context, delivery agentosproc.TriggerDelivery) error {
		deliveries = append(deliveries, delivery)
		if len(deliveries) == 1 {
			return errTemporaryTriggerDispatcherOutage
		}

		return nil
	}, activity.RegisterOptions{Name: DispatchTriggerActivityName})

	spec := temporalTestTriggerSpec()

	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "trigger-delivery-retry"})
	env.ExecuteWorkflow(TriggerDispatchWorkflow, &triggerDispatchWorkflowInput{
		Trigger:         spec.TriggerRef,
		Target:          spec.Target,
		WorkflowVersion: currentTriggerDispatchWorkflowVersion,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, deliveries, 2)
	require.Equal(t, deliveries[0].DeliveryID, deliveries[1].DeliveryID)
}

func TestRegisterTriggerDispatcherRegistersVersionedWorkload(t *testing.T) {
	t.Parallel()

	w := &fakeWorker{}
	require.NoError(t, RegisterTriggerDispatcher(w, func(context.Context, agentosproc.TriggerDelivery) error { return nil }))
	require.True(t, w.workflowRegistered(TriggerDispatchWorkflowName))
	require.True(t, w.activityRegistered(DispatchTriggerActivityName))
}

func temporalTestTriggerSpec() agentosproc.TriggerSpec {
	return agentosproc.TriggerSpec{
		TriggerRef: agentosproc.TriggerRef{TriggerID: "trigger-1", AccountID: "account-1", ProjectID: "project-1"},
		Target: agentosproc.ResourceRef{
			Kind:       "automation",
			ResourceID: "automation-1",
			AccountID:  "account-1",
			ProjectID:  "project-1",
		},
		Timing: agentosproc.TriggerTiming{
			CronExpressions: []string{"0 * * * *"},
			TimeZone:        "UTC",
		},
		Policy: agentosproc.TriggerPolicy{
			Overlap:       agentosproc.TriggerOverlapBufferOne,
			CatchupWindow: 5 * time.Minute,
		},
	}
}
