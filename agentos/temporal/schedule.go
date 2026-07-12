package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
)

const (
	// ScheduleDispatchWorkflowName is the workflow invoked by Temporal Schedule
	// actions created through ScheduleRuntime.
	ScheduleDispatchWorkflowName = "AgentOSScheduleDispatchWorkflow"
	// DispatchRecurringScheduleActivityName invokes the host application's
	// schedule dispatcher for one due schedule action.
	DispatchRecurringScheduleActivityName = "AgentOSDispatchRecurringSchedule"
)

var (
	errScheduleRuntimeNilClient      = errors.New("agentos temporal schedule runtime: nil temporal client")
	errScheduleRuntimeNilScheduleAPI = errors.New("agentos temporal schedule runtime: nil schedule client")
	errScheduleRuntimeNotConfigured  = errors.New("agentos temporal schedule runtime: not configured")
	errScheduleDispatchQueueRequired = errors.New("agentos temporal schedule runtime: dispatch task queue is required")
	errScheduleDispatcherRequired    = errors.New("agentos temporal schedule runtime: dispatcher is required")
	errScheduleWorkerRequired        = errors.New("agentos temporal schedule runtime: worker is required")
)

// ScheduleDispatcher starts application-owned work for a due recurring
// schedule. It must be idempotent for its DispatchID because dispatch is an
// external side effect at the application boundary.
type ScheduleDispatcher func(context.Context, agentosproc.ScheduleDispatch) error

// ScheduleRuntime implements the public recurring schedule port with Temporal
// Schedule resources. The application owns schedule persistence and work; this
// adapter owns only durable timing and delivery to its dispatcher worker.
type ScheduleRuntime struct {
	schedules     client.ScheduleClient
	dispatchQueue string
}

// NewScheduleRuntime creates a recurring schedule runtime using an existing
// Temporal client. The caller retains client lifecycle ownership.
func NewScheduleRuntime(c client.Client, dispatchQueue string) (*ScheduleRuntime, error) {
	if c == nil {
		return nil, errScheduleRuntimeNilClient
	}
	return newScheduleRuntime(c.ScheduleClient(), dispatchQueue)
}

func newScheduleRuntime(schedules client.ScheduleClient, dispatchQueue string) (*ScheduleRuntime, error) {
	if schedules == nil {
		return nil, errScheduleRuntimeNilScheduleAPI
	}
	if strings.TrimSpace(dispatchQueue) == "" {
		return nil, errScheduleDispatchQueueRequired
	}
	return &ScheduleRuntime{schedules: schedules, dispatchQueue: strings.TrimSpace(dispatchQueue)}, nil
}

// CreateSchedule creates one durable Temporal Schedule resource.
func (r *ScheduleRuntime) CreateSchedule(ctx context.Context, schedule agentosproc.RecurringSchedule) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}
	if err := agentosproc.ValidateRecurringSchedule(schedule); err != nil {
		return err
	}
	_, err := r.schedules.Create(ctx, scheduleOptions(schedule, r.dispatchQueue))
	return err
}

// UpdateSchedule replaces the mutable Temporal Schedule definition.
func (r *ScheduleRuntime) UpdateSchedule(ctx context.Context, schedule agentosproc.RecurringSchedule) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}
	if err := agentosproc.ValidateRecurringSchedule(schedule); err != nil {
		return err
	}
	return r.schedules.GetHandle(ctx, schedule.ScheduleID).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			definition := scheduleDefinition(schedule, r.dispatchQueue)
			return &client.ScheduleUpdate{Schedule: &definition}, nil
		},
	})
}

// PauseSchedule pauses future recurring dispatches and records the supplied
// human-readable note in Temporal.
func (r *ScheduleRuntime) PauseSchedule(ctx context.Context, ref agentosproc.ScheduleRef, note string) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}
	if err := agentosproc.ValidateScheduleRef(ref); err != nil {
		return err
	}
	return r.schedules.GetHandle(ctx, ref.ScheduleID).Pause(ctx, client.SchedulePauseOptions{Note: strings.TrimSpace(note)})
}

// ResumeSchedule resumes future recurring dispatches and records the supplied
// human-readable note in Temporal.
func (r *ScheduleRuntime) ResumeSchedule(ctx context.Context, ref agentosproc.ScheduleRef, note string) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}
	if err := agentosproc.ValidateScheduleRef(ref); err != nil {
		return err
	}
	return r.schedules.GetHandle(ctx, ref.ScheduleID).Unpause(ctx, client.ScheduleUnpauseOptions{Note: strings.TrimSpace(note)})
}

// DeleteSchedule removes the durable Temporal Schedule resource.
func (r *ScheduleRuntime) DeleteSchedule(ctx context.Context, ref agentosproc.ScheduleRef) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}
	if err := agentosproc.ValidateScheduleRef(ref); err != nil {
		return err
	}
	return r.schedules.GetHandle(ctx, ref.ScheduleID).Delete(ctx)
}

func (r *ScheduleRuntime) validateConfigured() error {
	if r == nil || r.schedules == nil || strings.TrimSpace(r.dispatchQueue) == "" {
		return errScheduleRuntimeNotConfigured
	}
	return nil
}

func scheduleOptions(schedule agentosproc.RecurringSchedule, dispatchQueue string) client.ScheduleOptions {
	return client.ScheduleOptions{
		ID:      schedule.ScheduleID,
		Spec:    client.ScheduleSpec{CronExpressions: []string{schedule.CronExpression}, TimeZoneName: schedule.TimeZone},
		Action:  scheduleWorkflowAction(schedule, dispatchQueue),
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		Paused:  schedule.Paused,
		Note:    schedule.Note,
	}
}

func scheduleDefinition(schedule agentosproc.RecurringSchedule, dispatchQueue string) client.Schedule {
	return client.Schedule{
		Spec:   &client.ScheduleSpec{CronExpressions: []string{schedule.CronExpression}, TimeZoneName: schedule.TimeZone},
		Action: scheduleWorkflowAction(schedule, dispatchQueue),
		Policy: &client.SchedulePolicies{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
		State:  &client.ScheduleState{Paused: schedule.Paused, Note: schedule.Note},
	}
}

func scheduleWorkflowAction(schedule agentosproc.RecurringSchedule, dispatchQueue string) *client.ScheduleWorkflowAction {
	return &client.ScheduleWorkflowAction{
		ID:        "agentos-schedule-" + schedule.ScheduleID,
		Workflow:  ScheduleDispatchWorkflowName,
		TaskQueue: dispatchQueue,
		Args: []interface{}{scheduleDispatchWorkflowInput{
			Dispatch:        agentosproc.ScheduleDispatch{DispatchID: schedule.DispatchID},
			WorkflowVersion: currentScheduleDispatchWorkflowVersion,
		}},
	}
}

type scheduleDispatchWorkflowInput struct {
	Dispatch        agentosproc.ScheduleDispatch `json:"dispatch"`
	WorkflowVersion int                          `json:"workflow_version"`
}

// ScheduleDispatchWorkflow validates one due schedule action and delegates to
// the registered application dispatcher. Activity retries are disabled because
// a retry after the dispatcher starts work could duplicate application output.
func ScheduleDispatchWorkflow(ctx workflow.Context, input scheduleDispatchWorkflowInput) error {
	if err := agentosproc.ValidateScheduleDispatch(input.Dispatch); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
	}
	if err := validateScheduleDispatchWorkflowVersion(input.WorkflowVersion); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteActivity(activityCtx, DispatchRecurringScheduleActivityName, input.Dispatch).Get(activityCtx, nil)
}

// RegisterScheduleDispatcher registers the workflow and activity needed to
// execute recurring schedules on an application-owned Temporal worker.
func RegisterScheduleDispatcher(w worker.Worker, dispatcher ScheduleDispatcher) error {
	if w == nil {
		return errScheduleWorkerRequired
	}
	if dispatcher == nil {
		return errScheduleDispatcherRequired
	}
	w.RegisterWorkflowWithOptions(ScheduleDispatchWorkflow, workflow.RegisterOptions{Name: ScheduleDispatchWorkflowName})
	w.RegisterActivityWithOptions(func(ctx context.Context, dispatch agentosproc.ScheduleDispatch) error {
		return dispatcher(ctx, dispatch)
	}, activity.RegisterOptions{Name: DispatchRecurringScheduleActivityName})
	return nil
}

// StartScheduleWorker starts a dedicated worker for application-owned
// recurring schedule dispatches. Stop the returned function before closing the
// Temporal client.
func StartScheduleWorker(c client.Client, dispatchQueue string, dispatcher ScheduleDispatcher) (func(), error) {
	if c == nil {
		return nil, errScheduleRuntimeNilClient
	}
	if strings.TrimSpace(dispatchQueue) == "" {
		return nil, errScheduleDispatchQueueRequired
	}
	w := worker.New(c, strings.TrimSpace(dispatchQueue), worker.Options{})
	if err := RegisterScheduleDispatcher(w, dispatcher); err != nil {
		return nil, err
	}
	if err := w.Start(); err != nil {
		return nil, fmt.Errorf("start recurring schedule worker: %w", err)
	}
	return w.Stop, nil
}
