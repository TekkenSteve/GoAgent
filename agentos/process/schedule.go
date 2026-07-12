package process

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ScheduleOverlapPolicy controls what happens when a recurring dispatch is
// due while a previous dispatch is still running.
type ScheduleOverlapPolicy string

const (
	// ScheduleOverlapSkip prevents duplicate work when a previous dispatch has
	// not completed before the next scheduled time.
	ScheduleOverlapSkip ScheduleOverlapPolicy = "skip"
)

// RecurringSchedule is an application-owned recurring dispatch definition.
// AgentOS stores no application data for it: the application owns persistence
// and supplies the dispatch identifier that is returned to its worker.
type RecurringSchedule struct {
	ScheduleID     string                `json:"schedule_id"`
	DispatchID     string                `json:"dispatch_id"`
	CronExpression string                `json:"cron_expression"`
	TimeZone       string                `json:"time_zone"`
	OverlapPolicy  ScheduleOverlapPolicy `json:"overlap_policy"`
	Paused         bool                  `json:"paused,omitempty"`
	Note           string                `json:"note,omitempty"`
}

// ScheduleRef identifies an application-owned recurring schedule.
type ScheduleRef struct {
	ScheduleID string `json:"schedule_id"`
}

// ScheduleDispatch is delivered to an application dispatcher for each due
// schedule action. The application resolves its own persisted configuration
// from DispatchID before starting any domain work.
type ScheduleDispatch struct {
	DispatchID string `json:"dispatch_id"`
}

// ScheduleRuntime manages recurring schedule definitions. Implementations are
// responsible for durable timing; applications remain responsible for their
// own schedule metadata and dispatched work.
type ScheduleRuntime interface {
	CreateSchedule(context.Context, RecurringSchedule) error
	UpdateSchedule(context.Context, RecurringSchedule) error
	PauseSchedule(context.Context, ScheduleRef, string) error
	ResumeSchedule(context.Context, ScheduleRef, string) error
	DeleteSchedule(context.Context, ScheduleRef) error
}

// ValidateRecurringSchedule checks the portable constraints required before
// an adapter creates or updates a recurring schedule. Cron syntax itself is
// intentionally validated by the timing adapter, which is the authority for
// the schedule dialect it executes.
func ValidateRecurringSchedule(schedule RecurringSchedule) error {
	if strings.TrimSpace(schedule.ScheduleID) == "" {
		return fmt.Errorf("recurring schedule ID is required")
	}
	if strings.TrimSpace(schedule.DispatchID) == "" {
		return fmt.Errorf("recurring schedule dispatch ID is required")
	}
	if strings.TrimSpace(schedule.CronExpression) == "" {
		return fmt.Errorf("recurring schedule cron expression is required")
	}
	if strings.TrimSpace(schedule.TimeZone) == "" {
		return fmt.Errorf("recurring schedule time zone is required")
	}
	if _, err := time.LoadLocation(schedule.TimeZone); err != nil {
		return fmt.Errorf("recurring schedule time zone: %w", err)
	}
	if schedule.OverlapPolicy != ScheduleOverlapSkip {
		return fmt.Errorf("unsupported recurring schedule overlap policy %q", schedule.OverlapPolicy)
	}
	return nil
}

// ValidateScheduleRef checks the identity used by a schedule lifecycle call.
func ValidateScheduleRef(ref ScheduleRef) error {
	if strings.TrimSpace(ref.ScheduleID) == "" {
		return fmt.Errorf("recurring schedule ID is required")
	}
	return nil
}

// ValidateScheduleDispatch checks the application dispatch identity emitted by
// a due recurring schedule action.
func ValidateScheduleDispatch(dispatch ScheduleDispatch) error {
	if strings.TrimSpace(dispatch.DispatchID) == "" {
		return fmt.Errorf("recurring schedule dispatch ID is required")
	}
	return nil
}
