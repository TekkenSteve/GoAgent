package temporal

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// TriggerDispatchWorkflowName is the versioned AgentOS workflow invoked by
	// Temporal Schedule actions created through TriggerRuntime.
	TriggerDispatchWorkflowName = "AgentOSTriggerDispatchWorkflow"
	// DispatchTriggerActivityName invokes an application-owned dispatcher for
	// one due trigger delivery.
	DispatchTriggerActivityName    = "AgentOSDispatchTrigger"
	triggerDispatchActivityTimeout = 30 * time.Second
)

var (
	errTriggerRuntimeNilClient      = errors.New("agentos temporal trigger runtime: nil temporal client")
	errTriggerRuntimeNilScheduleAPI = errors.New("agentos temporal trigger runtime: nil schedule client")
	errTriggerRuntimeNotConfigured  = errors.New("agentos temporal trigger runtime: not configured")
	errTriggerDispatchQueueRequired = errors.New("agentos temporal trigger runtime: dispatch task queue is required")
	errTriggerDispatcherRequired    = errors.New("agentos temporal trigger runtime: dispatcher is required")
	errTriggerWorkerRequired        = errors.New("agentos temporal trigger runtime: worker is required")
)

// TriggerDispatcher receives immutable trigger deliveries. It must persist
// DeliveryID as its idempotency key before starting application-owned work.
type TriggerDispatcher func(context.Context, agentosproc.TriggerDelivery) error

// TriggerRuntime implements process.TriggerRuntime with Temporal Schedule
// resources. It owns durable timing; applications own target persistence and
// the external work started by TriggerDispatcher.
type TriggerRuntime struct {
	schedules     client.ScheduleClient
	dispatchQueue string
}

// NewTriggerRuntime creates a trigger runtime using an existing Temporal
// client. The caller retains client lifecycle ownership.
func NewTriggerRuntime(c client.Client, dispatchQueue string) (*TriggerRuntime, error) {
	if c == nil {
		return nil, errTriggerRuntimeNilClient
	}

	return newTriggerRuntime(c.ScheduleClient(), dispatchQueue)
}

func newTriggerRuntime(schedules client.ScheduleClient, dispatchQueue string) (*TriggerRuntime, error) {
	if schedules == nil {
		return nil, errTriggerRuntimeNilScheduleAPI
	}

	if strings.TrimSpace(dispatchQueue) == "" {
		return nil, errTriggerDispatchQueueRequired
	}

	return &TriggerRuntime{schedules: schedules, dispatchQueue: strings.TrimSpace(dispatchQueue)}, nil
}

// CreateTrigger creates one durable Temporal Schedule resource with its
// initial lifecycle state applied atomically.
func (r *TriggerRuntime) CreateTrigger(ctx context.Context, spec *agentosproc.TriggerSpec, options agentosproc.TriggerCreateOptions) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerSpec(spec); err != nil {
		return err
	}

	_, err := r.schedules.Create(ctx, temporalTriggerOptions(spec, options, r.dispatchQueue))

	return err
}

// UpdateTrigger replaces timing, delivery policy, and target while preserving
// the observed paused state and note set by lifecycle operations.
func (r *TriggerRuntime) UpdateTrigger(ctx context.Context, spec *agentosproc.TriggerSpec) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerSpec(spec); err != nil {
		return err
	}

	return r.scheduleHandle(ctx, &spec.TriggerRef).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			definition := temporalTriggerDefinition(spec, r.dispatchQueue, input.Description.Schedule.State)

			return &client.ScheduleUpdate{Schedule: &definition}, nil
		},
	})
}

// PauseTrigger pauses future recurring deliveries and records the supplied
// human-readable note in Temporal.
func (r *TriggerRuntime) PauseTrigger(ctx context.Context, ref *agentosproc.TriggerRef, note string) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerRef(ref); err != nil {
		return err
	}

	return r.scheduleHandle(ctx, ref).Pause(ctx, client.SchedulePauseOptions{Note: strings.TrimSpace(note)})
}

// ResumeTrigger resumes future recurring deliveries and records the supplied
// human-readable note in Temporal.
func (r *TriggerRuntime) ResumeTrigger(ctx context.Context, ref *agentosproc.TriggerRef, note string) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerRef(ref); err != nil {
		return err
	}

	return r.scheduleHandle(ctx, ref).Unpause(ctx, client.ScheduleUnpauseOptions{Note: strings.TrimSpace(note)})
}

// DeleteTrigger removes the durable Temporal Schedule resource.
func (r *TriggerRuntime) DeleteTrigger(ctx context.Context, ref *agentosproc.TriggerRef) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerRef(ref); err != nil {
		return err
	}

	return r.scheduleHandle(ctx, ref).Delete(ctx)
}

// ObserveTrigger returns timing-engine state and execution history. It does
// not reconstruct the application-owned TriggerSpec from Temporal internals.
func (r *TriggerRuntime) ObserveTrigger(ctx context.Context, ref *agentosproc.TriggerRef) (agentosproc.TriggerObservation, error) {
	if err := r.validateConfigured(); err != nil {
		return agentosproc.TriggerObservation{}, err
	}

	if err := agentosproc.ValidateTriggerRef(ref); err != nil {
		return agentosproc.TriggerObservation{}, err
	}

	description, err := r.scheduleHandle(ctx, ref).Describe(ctx)
	if err != nil {
		return agentosproc.TriggerObservation{}, err
	}

	return triggerObservation(*ref, description), nil
}

// TriggerNow requests one immediate delivery using the explicitly supplied
// overlap policy.
func (r *TriggerRuntime) TriggerNow(ctx context.Context, ref *agentosproc.TriggerRef, request agentosproc.TriggerNowRequest) error {
	if err := r.validateConfigured(); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerRef(ref); err != nil {
		return err
	}

	if err := agentosproc.ValidateTriggerOverlapPolicy(request.Overlap); err != nil {
		return err
	}

	return r.scheduleHandle(ctx, ref).Trigger(ctx, client.ScheduleTriggerOptions{Overlap: temporalOverlapPolicy(request.Overlap)})
}

func (r *TriggerRuntime) scheduleHandle(ctx context.Context, ref *agentosproc.TriggerRef) client.ScheduleHandle {
	return r.schedules.GetHandle(ctx, temporalTriggerScheduleID(ref))
}

func (r *TriggerRuntime) validateConfigured() error {
	if r == nil || r.schedules == nil || strings.TrimSpace(r.dispatchQueue) == "" {
		return errTriggerRuntimeNotConfigured
	}

	return nil
}

func temporalTriggerOptions(spec *agentosproc.TriggerSpec, state agentosproc.TriggerCreateOptions, queue string) client.ScheduleOptions {
	return client.ScheduleOptions{
		ID:             temporalTriggerScheduleID(&spec.TriggerRef),
		Spec:           *temporalTiming(&spec.Timing),
		Action:         temporalTriggerAction(spec, queue),
		Overlap:        temporalOverlapPolicy(spec.Policy.Overlap),
		CatchupWindow:  spec.Policy.CatchupWindow,
		PauseOnFailure: spec.Policy.PauseOnFailure,
		Paused:         state.Paused,
		Note:           strings.TrimSpace(state.Note),
	}
}

func temporalTriggerDefinition(spec *agentosproc.TriggerSpec, queue string, state *client.ScheduleState) client.Schedule {
	return client.Schedule{
		Spec:   temporalTiming(&spec.Timing),
		Action: temporalTriggerAction(spec, queue),
		Policy: &client.SchedulePolicies{
			Overlap:        temporalOverlapPolicy(spec.Policy.Overlap),
			CatchupWindow:  spec.Policy.CatchupWindow,
			PauseOnFailure: spec.Policy.PauseOnFailure,
		},
		State: state,
	}
}

func temporalTiming(timing *agentosproc.TriggerTiming) *client.ScheduleSpec {
	return &client.ScheduleSpec{
		CronExpressions: timing.CronExpressions,
		Calendars:       temporalCalendars(timing.Calendars),
		Intervals:       temporalIntervals(timing.Intervals),
		Skip:            temporalCalendars(timing.Exclusions),
		StartAt:         timing.StartAt,
		EndAt:           timing.EndAt,
		Jitter:          timing.Jitter,
		TimeZoneName:    timing.TimeZone,
	}
}

func temporalCalendars(values []agentosproc.CalendarSpec) []client.ScheduleCalendarSpec {
	result := make([]client.ScheduleCalendarSpec, 0, len(values))
	for i := range values {
		value := &values[i]
		result = append(result, client.ScheduleCalendarSpec{
			Second:     temporalRanges(value.Second),
			Minute:     temporalRanges(value.Minute),
			Hour:       temporalRanges(value.Hour),
			DayOfMonth: temporalRanges(value.DayOfMonth),
			Month:      temporalRanges(value.Month),
			Year:       temporalRanges(value.Year),
			DayOfWeek:  temporalRanges(value.DayOfWeek),
			Comment:    value.Comment,
		})
	}

	return result
}

func temporalRanges(values []agentosproc.CalendarRange) []client.ScheduleRange {
	result := make([]client.ScheduleRange, 0, len(values))
	for _, value := range values {
		result = append(result, client.ScheduleRange{Start: value.Start, End: value.End, Step: value.Step})
	}

	return result
}

func temporalIntervals(values []agentosproc.IntervalSpec) []client.ScheduleIntervalSpec {
	result := make([]client.ScheduleIntervalSpec, 0, len(values))
	for _, value := range values {
		result = append(result, client.ScheduleIntervalSpec{Every: value.Every, Offset: value.Offset})
	}

	return result
}

func temporalOverlapPolicy(policy agentosproc.TriggerOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	switch policy {
	case agentosproc.TriggerOverlapSkip:
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	case agentosproc.TriggerOverlapBufferOne:
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
	case agentosproc.TriggerOverlapBufferAll:
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	case agentosproc.TriggerOverlapCancelPrevious:
		return enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
	case agentosproc.TriggerOverlapTerminatePrev:
		return enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	case agentosproc.TriggerOverlapAllowConcurrent:
		return enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	default:
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
	}
}

func temporalTriggerAction(spec *agentosproc.TriggerSpec, queue string) *client.ScheduleWorkflowAction {
	return &client.ScheduleWorkflowAction{
		ID:        temporalTriggerWorkflowID(&spec.TriggerRef),
		Workflow:  TriggerDispatchWorkflowName,
		TaskQueue: queue,
		Args: []any{&triggerDispatchWorkflowInput{
			Trigger:         spec.TriggerRef,
			Target:          spec.Target,
			WorkflowVersion: currentTriggerDispatchWorkflowVersion,
		}},
	}
}

// Temporal schedules live in one namespace, while public trigger IDs are only
// tenant-scoped. Hashing the canonical reference creates a stable namespace
// safe schedule identifier without leaking application identifiers.
func temporalTriggerScheduleID(ref *agentosproc.TriggerRef) string {
	return "agentos-trigger-schedule-" + temporalTriggerIdentity(ref)
}

func temporalTriggerWorkflowID(ref *agentosproc.TriggerRef) string {
	return "agentos-trigger-delivery-" + temporalTriggerIdentity(ref)
}

func temporalTriggerIdentity(ref *agentosproc.TriggerRef) string {
	encoded, err := json.Marshal(ref)
	if err != nil {
		panic(fmt.Sprintf("marshal trigger reference: %v", err))
	}

	digest := sha256.Sum256(encoded)

	return base64.RawURLEncoding.EncodeToString(digest[:])
}

type triggerDispatchWorkflowInput struct {
	Trigger         agentosproc.TriggerRef  `json:"trigger"`
	Target          agentosproc.ResourceRef `json:"target"`
	WorkflowVersion int                     `json:"workflow_version"`
}

// TriggerDispatchWorkflow creates one immutable delivery identity from its
// Temporal execution and passes it exactly once to the registered dispatcher.
// The dispatcher persists DeliveryID before starting external work; activity
// retries are disabled to prevent duplicate external execution.
func TriggerDispatchWorkflow(ctx workflow.Context, input *triggerDispatchWorkflowInput) error {
	if input == nil {
		return temporal.NewNonRetryableApplicationError("trigger dispatch input is required", "validation", nil)
	}

	if err := agentosproc.ValidateTriggerRef(&input.Trigger); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
	}

	if err := agentosproc.ValidateResourceRef(input.Target); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
	}

	if input.Target.AccountID != input.Trigger.AccountID || input.Target.ProjectID != input.Trigger.ProjectID {
		return temporal.NewNonRetryableApplicationError("trigger target must belong to the trigger tenant scope", "validation", nil)
	}

	if err := validateTriggerDispatchWorkflowVersion(input.WorkflowVersion); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
	}

	info := workflow.GetInfo(ctx)
	delivery := agentosproc.TriggerDelivery{
		DeliveryID:  info.WorkflowExecution.ID + ":" + info.WorkflowExecution.RunID,
		Trigger:     input.Trigger,
		Target:      input.Target,
		TriggeredAt: workflow.Now(ctx),
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: triggerDispatchActivityTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	return workflow.ExecuteActivity(activityCtx, DispatchTriggerActivityName, delivery).Get(activityCtx, nil)
}

// RegisterTriggerDispatcher registers the generic trigger workflow and its
// application-owned delivery activity on an explicit Temporal worker.
func RegisterTriggerDispatcher(w worker.Worker, dispatcher TriggerDispatcher) error {
	if w == nil {
		return errTriggerWorkerRequired
	}

	if dispatcher == nil {
		return errTriggerDispatcherRequired
	}

	w.RegisterWorkflowWithOptions(TriggerDispatchWorkflow, workflow.RegisterOptions{Name: TriggerDispatchWorkflowName})
	w.RegisterActivityWithOptions(func(ctx context.Context, delivery agentosproc.TriggerDelivery) error {
		return dispatcher(ctx, delivery)
	}, activity.RegisterOptions{Name: DispatchTriggerActivityName})

	return nil
}

// StartTriggerWorker starts a dedicated trigger dispatch worker. Stop the
// returned function before closing the Temporal client.
func StartTriggerWorker(c client.Client, queue string, dispatcher TriggerDispatcher) (func(), error) {
	if c == nil {
		return nil, errTriggerRuntimeNilClient
	}

	if strings.TrimSpace(queue) == "" {
		return nil, errTriggerDispatchQueueRequired
	}

	w := worker.New(c, strings.TrimSpace(queue), worker.Options{})
	if err := RegisterTriggerDispatcher(w, dispatcher); err != nil {
		return nil, err
	}

	if err := w.Start(); err != nil {
		return nil, fmt.Errorf("start trigger worker: %w", err)
	}

	return w.Stop, nil
}

func triggerObservation(ref agentosproc.TriggerRef, description *client.ScheduleDescription) agentosproc.TriggerObservation {
	if description == nil {
		return agentosproc.TriggerObservation{Trigger: ref}
	}

	result := agentosproc.TriggerObservation{
		Trigger:             ref,
		NextDeliveryTimes:   description.Info.NextActionTimes,
		DeliveryCount:       description.Info.NumActions,
		SkippedOverlapCount: description.Info.NumActionsSkippedOverlap,
		MissedCatchupCount:  description.Info.NumActionsMissedCatchupWindow,
		CreatedAt:           description.Info.CreatedAt,
		UpdatedAt:           description.Info.LastUpdateAt,
	}
	if state := description.Schedule.State; state != nil {
		result.State = agentosproc.TriggerState{Paused: state.Paused, Note: state.Note}
	}

	for _, execution := range description.Info.RunningWorkflows {
		result.RunningDeliveries = append(result.RunningDeliveries, agentosproc.TriggerExecution{
			WorkflowID: execution.WorkflowID,
			RunID:      execution.FirstExecutionRunID,
		})
	}

	for _, action := range description.Info.RecentActions {
		if action.StartWorkflowResult == nil {
			continue
		}

		result.RecentDeliveries = append(result.RecentDeliveries, agentosproc.TriggerExecution{
			WorkflowID:  action.StartWorkflowResult.WorkflowID,
			RunID:       action.StartWorkflowResult.FirstExecutionRunID,
			ScheduledAt: action.ScheduleTime,
			StartedAt:   action.ActualTime,
		})
	}

	return result
}
