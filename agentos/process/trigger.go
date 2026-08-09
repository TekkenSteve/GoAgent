package process

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// TriggerOverlapPolicy controls the behavior when a delivery is due while an
// earlier delivery is still executing.
type TriggerOverlapPolicy string

// TriggerOverlapPolicy values controlling behavior when a delivery is due
// while an earlier delivery is still executing.
const (
	TriggerOverlapSkip            TriggerOverlapPolicy = "skip"
	TriggerOverlapBufferOne       TriggerOverlapPolicy = "buffer_one"
	TriggerOverlapBufferAll       TriggerOverlapPolicy = "buffer_all"
	TriggerOverlapCancelPrevious  TriggerOverlapPolicy = "cancel_previous"
	TriggerOverlapTerminatePrev   TriggerOverlapPolicy = "terminate_previous"
	TriggerOverlapAllowConcurrent TriggerOverlapPolicy = "allow_concurrent"
)

var (
	errTriggerTimeZoneRequired     = errors.New("trigger time zone is required")
	errTriggerTimingRequired       = errors.New("trigger timing is required")
	errTriggerCronExpressionEmpty  = errors.New("trigger cron expression is empty")
	errTriggerIntervalInvalid      = errors.New("trigger interval is invalid")
	errTriggerTimeRangeInvalid     = errors.New("trigger time range is invalid")
	errTriggerJitterInvalid        = errors.New("trigger jitter is invalid")
	errTriggerCalendarRangeInvalid = errors.New("trigger calendar range is invalid")
	errTriggerCatchupWindowInvalid = errors.New("trigger catchup window is invalid")
	errTriggerOverlapPolicyInvalid = errors.New("trigger overlap policy is invalid")
	errTriggerTimeZoneInvalid      = errors.New("trigger time zone is invalid")
	errTriggerDeliveryTimeout      = errors.New("trigger delivery timeout is invalid")
	errTriggerRetryInitial         = errors.New("trigger retry initial interval is invalid")
	errTriggerRetryMaximum         = errors.New("trigger retry maximum interval is invalid")
	errTriggerRetryBackoff         = errors.New("trigger retry backoff coefficient is invalid")
	errTriggerRetryAttempts        = errors.New("trigger retry maximum attempts is invalid")
)

// TriggerRef identifies an application-owned trigger inside an account/project
// boundary. TriggerID only needs to be unique within that boundary.
type TriggerRef struct {
	TriggerID string `json:"trigger_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// CalendarRange selects values in one calendar field. Field bounds are owned
// by the timing adapter because their exact semantics depend on its engine.
type CalendarRange struct {
	Start int `json:"start"`
	End   int `json:"end,omitempty"`
	Step  int `json:"step,omitempty"`
}

// CalendarSpec describes one calendar-based recurring time expression.
type CalendarSpec struct {
	Second     []CalendarRange `json:"second,omitempty"`
	Minute     []CalendarRange `json:"minute,omitempty"`
	Hour       []CalendarRange `json:"hour,omitempty"`
	DayOfMonth []CalendarRange `json:"day_of_month,omitempty"`
	Month      []CalendarRange `json:"month,omitempty"`
	Year       []CalendarRange `json:"year,omitempty"`
	DayOfWeek  []CalendarRange `json:"day_of_week,omitempty"`
	Comment    string          `json:"comment,omitempty"`
}

// IntervalSpec describes a recurring interval with an optional phase offset.
type IntervalSpec struct {
	Every  time.Duration `json:"every"`
	Offset time.Duration `json:"offset,omitempty"`
}

// TriggerTiming is a timing-engine-independent recurring time definition.
// At least one of CronExpressions, Calendars, or Intervals is required.
type TriggerTiming struct {
	CronExpressions []string       `json:"cron_expressions,omitempty"`
	Calendars       []CalendarSpec `json:"calendars,omitempty"`
	Intervals       []IntervalSpec `json:"intervals,omitempty"`
	Exclusions      []CalendarSpec `json:"exclusions,omitempty"`
	StartAt         time.Time      `json:"start_at,omitzero"`
	EndAt           time.Time      `json:"end_at,omitzero"`
	Jitter          time.Duration  `json:"jitter,omitempty"`
	TimeZone        string         `json:"time_zone"`
}

// TriggerDeliveryRetryPolicy controls retries after a dispatcher returns an
// ordinary error. MaximumAttempts of zero means retry attempts are unbounded.
type TriggerDeliveryRetryPolicy struct {
	InitialInterval    time.Duration `json:"initial_interval"`
	MaximumInterval    time.Duration `json:"maximum_interval"`
	BackoffCoefficient float64       `json:"backoff_coefficient"`
	MaximumAttempts    int32         `json:"maximum_attempts"`
}

// TriggerDeliveryPolicy controls the execution of one due delivery. Every
// trigger declares this policy explicitly so timing adapters never invent
// operational behavior for an application.
type TriggerDeliveryPolicy struct {
	StartToCloseTimeout time.Duration              `json:"start_to_close_timeout"`
	Retry               TriggerDeliveryRetryPolicy `json:"retry"`
}

// TriggerPolicy controls durable scheduling and delivery behavior for a
// trigger.
type TriggerPolicy struct {
	Overlap        TriggerOverlapPolicy  `json:"overlap"`
	CatchupWindow  time.Duration         `json:"catchup_window"`
	PauseOnFailure bool                  `json:"pause_on_failure,omitempty"`
	Delivery       TriggerDeliveryPolicy `json:"delivery"`
}

// TriggerSpec is a generic recurring trigger declaration. Target is an opaque
// application resource; AgentOS does not interpret its domain kind. Paused
// state is intentionally not part of this declaration and is changed through
// the lifecycle methods on TriggerRuntime.
type TriggerSpec struct {
	TriggerRef
	Target ResourceRef   `json:"target"`
	Timing TriggerTiming `json:"timing"`
	Policy TriggerPolicy `json:"policy"`
}

// TriggerState is the current operator-facing lifecycle state of a trigger.
type TriggerState struct {
	Paused bool   `json:"paused"`
	Note   string `json:"note,omitempty"`
}

// TriggerCreateOptions configures the initial mutable state atomically with
// trigger creation. Later declaration updates preserve that state.
type TriggerCreateOptions struct {
	Paused bool   `json:"paused,omitempty"`
	Note   string `json:"note,omitempty"`
}

// TriggerDelivery is one immutable due action. DeliveryID is unique for an
// execution attempt and must be persisted by applications as their idempotency
// key before they start external work.
type TriggerDelivery struct {
	DeliveryID  string      `json:"delivery_id"`
	Trigger     TriggerRef  `json:"trigger"`
	Target      ResourceRef `json:"target"`
	TriggeredAt time.Time   `json:"triggered_at"`
}

// TriggerExecution is the timing adapter's record of a delivery workflow.
// DeliveryID is excluded because timing engines do not generally retain it.
type TriggerExecution struct {
	WorkflowID  string    `json:"workflow_id"`
	RunID       string    `json:"run_id"`
	ScheduledAt time.Time `json:"scheduled_at,omitzero"`
	StartedAt   time.Time `json:"started_at,omitzero"`
}

// TriggerObservation is the durable read view that timing adapters can report
// without claiming to reconstruct application-owned trigger declarations.
type TriggerObservation struct {
	Trigger             TriggerRef         `json:"trigger"`
	State               TriggerState       `json:"state"`
	NextDeliveryTimes   []time.Time        `json:"next_delivery_times,omitempty"`
	RecentDeliveries    []TriggerExecution `json:"recent_deliveries,omitempty"`
	RunningDeliveries   []TriggerExecution `json:"running_deliveries,omitempty"`
	DeliveryCount       int                `json:"delivery_count"`
	SkippedOverlapCount int                `json:"skipped_overlap_count"`
	MissedCatchupCount  int                `json:"missed_catchup_count"`
	CreatedAt           time.Time          `json:"created_at,omitzero"`
	UpdatedAt           time.Time          `json:"updated_at,omitzero"`
}

// TriggerNowRequest requests one immediate delivery with an explicit overlap
// policy. Explicit policy prevents an operator action from silently changing
// behavior when the trigger declaration is later updated.
type TriggerNowRequest struct {
	Overlap TriggerOverlapPolicy `json:"overlap"`
}

// TriggerRuntime manages generic recurring trigger declarations and their
// lifecycle. ApplyTrigger makes the durable timing resource match a desired
// declaration while preserving its current mutable lifecycle state.
// Applications own domain configuration and consume TriggerDelivery values
// through a dispatcher worker.
type TriggerRuntime interface {
	ApplyTrigger(context.Context, *TriggerSpec, TriggerCreateOptions) error
	PauseTrigger(context.Context, *TriggerRef, string) error
	ResumeTrigger(context.Context, *TriggerRef, string) error
	DeleteTrigger(context.Context, *TriggerRef) error
	ObserveTrigger(context.Context, *TriggerRef) (TriggerObservation, error)
	TriggerNow(context.Context, *TriggerRef, TriggerNowRequest) error
}

// TriggerDeclaration is the desired state an application persists for one
// trigger. Mutable lifecycle state stays in the timing adapter and is not
// overwritten during reconciliation.
type TriggerDeclaration struct {
	Spec          TriggerSpec          `json:"spec"`
	CreateOptions TriggerCreateOptions `json:"create_options"`
}

// TriggerDeclarationStore provides application-owned trigger declarations to
// a reconciler. Deletion remains an explicit application lifecycle operation:
// callers delete a runtime trigger when they delete its persisted declaration.
type TriggerDeclarationStore interface {
	ListTriggerDeclarations(context.Context) ([]TriggerDeclaration, error)
}

// ReconcileTriggerDeclarations applies every persisted declaration to a
// runtime. This is safe to call at process startup because ApplyTrigger is
// declarative and preserves existing mutable lifecycle state.
func ReconcileTriggerDeclarations(ctx context.Context, runtime TriggerRuntime, store TriggerDeclarationStore) error {
	if runtime == nil {
		return fmt.Errorf("%w: trigger runtime is required", core.ErrInvalidTrigger)
	}

	if store == nil {
		return fmt.Errorf("%w: trigger declaration store is required", core.ErrInvalidTrigger)
	}

	declarations, err := store.ListTriggerDeclarations(ctx)
	if err != nil {
		return err
	}

	for i := range declarations {
		declaration := &declarations[i]
		if err := runtime.ApplyTrigger(ctx, &declaration.Spec, declaration.CreateOptions); err != nil {
			return fmt.Errorf("reconcile trigger %q: %w", declaration.Spec.TriggerID, err)
		}
	}

	return nil
}

// ValidateTriggerSpec checks portable trigger invariants. Cron and calendar
// syntax is validated by the selected timing adapter, which owns its dialect.
func ValidateTriggerSpec(spec *TriggerSpec) error {
	if spec == nil {
		return fmt.Errorf("%w: trigger spec is required", core.ErrInvalidTrigger)
	}

	if err := ValidateTriggerRef(&spec.TriggerRef); err != nil {
		return fmt.Errorf("%w: %w", core.ErrInvalidTrigger, err)
	}

	if err := ValidateResourceRef(spec.Target); err != nil {
		return fmt.Errorf("%w: target: %w", core.ErrInvalidTrigger, err)
	}

	if spec.Target.AccountID != spec.AccountID || spec.Target.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: target must belong to the trigger tenant scope", core.ErrInvalidTrigger)
	}

	if err := ValidateTriggerTiming(&spec.Timing); err != nil {
		return fmt.Errorf("%w: %w", core.ErrInvalidTrigger, err)
	}

	if err := ValidateTriggerPolicy(spec.Policy); err != nil {
		return fmt.Errorf("%w: %w", core.ErrInvalidTrigger, err)
	}

	return nil
}

// ValidateTriggerRef validates a tenant-scoped trigger reference.
func ValidateTriggerRef(ref *TriggerRef) error {
	if ref == nil {
		return fmt.Errorf("%w: trigger ref is required", core.ErrInvalidTriggerScope)
	}

	switch {
	case strings.TrimSpace(ref.TriggerID) == "":
		return fmt.Errorf("%w: trigger id is required", core.ErrInvalidTriggerScope)
	case strings.TrimSpace(ref.AccountID) == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidTriggerScope)
	case strings.TrimSpace(ref.ProjectID) == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidTriggerScope)
	default:
		return nil
	}
}

// ValidateTriggerTiming checks timing invariants that do not depend on a
// particular timing engine.
func ValidateTriggerTiming(timing *TriggerTiming) error {
	if timing == nil {
		return errTriggerTimingRequired
	}

	if err := validateTriggerTimeZone(timing.TimeZone); err != nil {
		return err
	}

	if err := validateTriggerSources(timing); err != nil {
		return err
	}

	if err := validateTriggerCalendars(timing.Calendars); err != nil {
		return err
	}

	if err := validateTriggerCalendars(timing.Exclusions); err != nil {
		return err
	}

	if !timing.StartAt.IsZero() && !timing.EndAt.IsZero() && timing.EndAt.Before(timing.StartAt) {
		return errTriggerTimeRangeInvalid
	}

	if timing.Jitter < 0 {
		return errTriggerJitterInvalid
	}

	return nil
}

func validateTriggerTimeZone(timeZone string) error {
	if strings.TrimSpace(timeZone) == "" {
		return errTriggerTimeZoneRequired
	}

	if _, err := time.LoadLocation(timeZone); err != nil {
		return fmt.Errorf("%w: %w", errTriggerTimeZoneInvalid, err)
	}

	return nil
}

func validateTriggerSources(timing *TriggerTiming) error {
	if len(timing.CronExpressions) == 0 && len(timing.Calendars) == 0 && len(timing.Intervals) == 0 {
		return errTriggerTimingRequired
	}

	for _, expression := range timing.CronExpressions {
		if strings.TrimSpace(expression) == "" {
			return errTriggerCronExpressionEmpty
		}
	}

	for _, interval := range timing.Intervals {
		if interval.Every <= 0 || interval.Offset < 0 {
			return errTriggerIntervalInvalid
		}
	}

	return nil
}

func validateTriggerCalendars(calendars []CalendarSpec) error {
	for i := range calendars {
		if err := validateCalendarRanges(&calendars[i]); err != nil {
			return err
		}
	}

	return nil
}

func validateCalendarRanges(calendar *CalendarSpec) error {
	fields := [][]CalendarRange{
		calendar.Second,
		calendar.Minute,
		calendar.Hour,
		calendar.DayOfMonth,
		calendar.Month,
		calendar.Year,
		calendar.DayOfWeek,
	}
	for _, ranges := range fields {
		for _, value := range ranges {
			if value.Start < 0 || value.End < 0 || value.Step < 0 {
				return errTriggerCalendarRangeInvalid
			}

			if value.End != 0 && value.End < value.Start {
				return errTriggerCalendarRangeInvalid
			}
		}
	}

	return nil
}

// ValidateTriggerPolicy validates a complete recurring delivery policy.
func ValidateTriggerPolicy(policy TriggerPolicy) error {
	if policy.CatchupWindow <= 0 {
		return errTriggerCatchupWindowInvalid
	}

	if err := ValidateTriggerOverlapPolicy(policy.Overlap); err != nil {
		return err
	}

	return ValidateTriggerDeliveryPolicy(policy.Delivery)
}

// ValidateTriggerDeliveryPolicy validates the portable execution and retry
// policy carried by every delivery workflow.
func ValidateTriggerDeliveryPolicy(policy TriggerDeliveryPolicy) error {
	if policy.StartToCloseTimeout <= 0 {
		return errTriggerDeliveryTimeout
	}

	retry := policy.Retry
	if retry.InitialInterval <= 0 {
		return errTriggerRetryInitial
	}

	if retry.MaximumInterval < retry.InitialInterval {
		return errTriggerRetryMaximum
	}

	if retry.BackoffCoefficient < 1 || math.IsNaN(retry.BackoffCoefficient) || math.IsInf(retry.BackoffCoefficient, 0) {
		return errTriggerRetryBackoff
	}

	if retry.MaximumAttempts < 0 {
		return errTriggerRetryAttempts
	}

	return nil
}

// ValidateTriggerOverlapPolicy validates a policy used by a scheduled or
// immediate trigger delivery.
func ValidateTriggerOverlapPolicy(policy TriggerOverlapPolicy) error {
	switch policy {
	case TriggerOverlapSkip, TriggerOverlapBufferOne, TriggerOverlapBufferAll, TriggerOverlapCancelPrevious, TriggerOverlapTerminatePrev, TriggerOverlapAllowConcurrent:
		return nil
	default:
		return fmt.Errorf("%w: %q", errTriggerOverlapPolicyInvalid, policy)
	}
}

// ValidateTriggerDelivery validates the host-facing immutable delivery.
func ValidateTriggerDelivery(delivery *TriggerDelivery) error {
	if delivery == nil {
		return fmt.Errorf("%w: trigger delivery is required", core.ErrInvalidTrigger)
	}

	if strings.TrimSpace(delivery.DeliveryID) == "" || delivery.TriggeredAt.IsZero() {
		return fmt.Errorf("%w: delivery id and triggered_at are required", core.ErrInvalidTrigger)
	}

	if err := ValidateTriggerRef(&delivery.Trigger); err != nil {
		return fmt.Errorf("%w: %w", core.ErrInvalidTrigger, err)
	}

	if err := ValidateResourceRef(delivery.Target); err != nil {
		return fmt.Errorf("%w: target: %w", core.ErrInvalidTrigger, err)
	}

	if delivery.Target.AccountID != delivery.Trigger.AccountID || delivery.Target.ProjectID != delivery.Trigger.ProjectID {
		return fmt.Errorf("%w: target must belong to the trigger tenant scope", core.ErrInvalidTrigger)
	}

	return nil
}
