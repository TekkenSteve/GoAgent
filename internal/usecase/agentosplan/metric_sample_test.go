package agentosplan

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanMetricSampleRequiresDurableIdentity(t *testing.T) {
	t.Parallel()

	sample := validPlanMetricSample()
	sample.EventID = ""

	err := ValidatePlanMetricSample(&sample)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ValidatePlanMetricSample error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanMetricSampleRejectsNonFiniteValue(t *testing.T) {
	t.Parallel()

	sample := validPlanMetricSample()
	sample.Value = math.Inf(1)

	err := ValidatePlanMetricSample(&sample)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ValidatePlanMetricSample error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanMetricSampleIdempotencyNormalizesReplay(t *testing.T) {
	t.Parallel()

	first := validPlanMetricSample()
	first.Labels = nil
	replay := first
	replay.Timestamp = first.Timestamp.Local().Add(123 * time.Nanosecond)
	replay.Labels = map[string]string{}

	if err := ValidatePlanMetricSampleIdempotency(&first, &replay); err != nil {
		t.Fatalf("ValidatePlanMetricSampleIdempotency: %v", err)
	}
}

func TestValidatePlanMetricSampleIdempotencyRejectsChangedValue(t *testing.T) {
	t.Parallel()

	first := validPlanMetricSample()
	replay := first
	replay.Value = 2

	err := ValidatePlanMetricSampleIdempotency(&first, &replay)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ValidatePlanMetricSampleIdempotency error = %v, want ErrInvalidRunPlan", err)
	}
}

func validPlanMetricSample() PlanMetricSample {
	return PlanMetricSample{
		Name:      PlanMetricPlanStartedTotal,
		Value:     1,
		Unit:      "count",
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		EventID:   "plan-1:1",
		Sequence:  1,
		Timestamp: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		Labels:    map[string]string{"lifecycle_state": agentos.PlanLifecycleRunning},
	}
}
