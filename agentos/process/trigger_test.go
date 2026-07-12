package process

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestValidateTriggerSpecAcceptsTenantScopedRecurringTrigger(t *testing.T) {
	t.Parallel()

	spec := validTriggerSpec()
	if err := ValidateTriggerSpec(&spec); err != nil {
		t.Fatalf("ValidateTriggerSpec: %v", err)
	}
}

func TestValidateTriggerSpecRejectsInvalidDeclarations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*TriggerSpec)
	}{
		{name: "trigger id", edit: func(spec *TriggerSpec) { spec.TriggerID = "" }},
		{name: "cross tenant target", edit: func(spec *TriggerSpec) { spec.Target.ProjectID = "other-project" }},
		{name: "missing timing", edit: func(spec *TriggerSpec) { spec.Timing = TriggerTiming{TimeZone: "UTC"} }},
		{name: "invalid interval", edit: func(spec *TriggerSpec) {
			spec.Timing.Intervals = []IntervalSpec{{Every: 0}}
			spec.Timing.CronExpressions = nil
		}},
		{name: "negative calendar range", edit: func(spec *TriggerSpec) {
			spec.Timing.Calendars = []CalendarSpec{{Hour: []CalendarRange{{Start: -1}}}}
		}},
		{name: "reverse calendar range", edit: func(spec *TriggerSpec) {
			spec.Timing.Calendars = []CalendarSpec{{Hour: []CalendarRange{{Start: 10, End: 1}}}}
		}},
		{name: "invalid overlap", edit: func(spec *TriggerSpec) { spec.Policy.Overlap = "unknown" }},
		{name: "invalid catchup", edit: func(spec *TriggerSpec) { spec.Policy.CatchupWindow = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validTriggerSpec()
			tt.edit(&spec)

			err := ValidateTriggerSpec(&spec)
			if !errors.Is(err, core.ErrInvalidTrigger) {
				t.Fatalf("ValidateTriggerSpec error = %v, want core.ErrInvalidTrigger", err)
			}
		})
	}
}

func TestValidateTriggerRefAndDeliveryRequireTenantScope(t *testing.T) {
	t.Parallel()

	if err := ValidateTriggerRef(nil); !errors.Is(err, core.ErrInvalidTriggerScope) {
		t.Fatalf("ValidateTriggerRef(nil) error = %v, want core.ErrInvalidTriggerScope", err)
	}

	spec := validTriggerSpec()

	delivery := TriggerDelivery{
		DeliveryID:  "delivery-1",
		Trigger:     spec.TriggerRef,
		Target:      spec.Target,
		TriggeredAt: time.Now().UTC(),
	}
	if err := ValidateTriggerDelivery(&delivery); err != nil {
		t.Fatalf("ValidateTriggerDelivery: %v", err)
	}

	delivery.Target.AccountID = "other-account"
	if err := ValidateTriggerDelivery(&delivery); !errors.Is(err, core.ErrInvalidTrigger) {
		t.Fatalf("ValidateTriggerDelivery error = %v, want core.ErrInvalidTrigger", err)
	}
}

func validTriggerSpec() TriggerSpec {
	return TriggerSpec{
		TriggerRef: TriggerRef{TriggerID: "trigger-1", AccountID: "account-1", ProjectID: "project-1"},
		Target: ResourceRef{
			Kind:       "automation",
			ResourceID: "automation-1",
			AccountID:  "account-1",
			ProjectID:  "project-1",
		},
		Timing: TriggerTiming{
			CronExpressions: []string{"0 * * * *"},
			TimeZone:        "UTC",
		},
		Policy: TriggerPolicy{
			Overlap:       TriggerOverlapBufferOne,
			CatchupWindow: 5 * time.Minute,
		},
	}
}
