package agentosplan

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanMetricSampleRequiresDurableIdentity(t *testing.T) {
	sample := validPlanMetricSample()
	sample.EventID = ""

	err := ValidatePlanMetricSample(sample)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ValidatePlanMetricSample error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanMetricSampleRejectsNonFiniteValue(t *testing.T) {
	sample := validPlanMetricSample()
	sample.Value = math.Inf(1)

	err := ValidatePlanMetricSample(sample)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ValidatePlanMetricSample error = %v, want ErrInvalidRunPlan", err)
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
