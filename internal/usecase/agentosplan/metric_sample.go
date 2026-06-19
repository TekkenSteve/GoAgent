package agentosplan

import (
	"fmt"
	"math"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidatePlanMetricSample verifies the durable identity and tenant scope of a
// projected PlanMetricSample before a sink records it.
func ValidatePlanMetricSample(sample PlanMetricSample) error {
	if sample.Name == "" {
		return fmt.Errorf("%w: metric name is required", agentos.ErrInvalidRunPlan)
	}
	if sample.PlanID == "" {
		return fmt.Errorf("%w: metric plan id is required", agentos.ErrInvalidRunPlan)
	}
	if sample.AccountID == "" {
		return fmt.Errorf("%w: metric account id is required", agentos.ErrInvalidRunPlan)
	}
	if sample.ProjectID == "" {
		return fmt.Errorf("%w: metric project id is required", agentos.ErrInvalidRunPlan)
	}
	if sample.EventID == "" {
		return fmt.Errorf("%w: metric event id is required", agentos.ErrInvalidRunPlan)
	}
	if sample.Sequence <= 0 {
		return fmt.Errorf("%w: metric event sequence must be positive", agentos.ErrInvalidRunPlan)
	}
	if sample.Unit == "" {
		return fmt.Errorf("%w: metric unit is required", agentos.ErrInvalidRunPlan)
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		return fmt.Errorf("%w: metric value must be finite", agentos.ErrInvalidRunPlan)
	}
	if sample.Timestamp.IsZero() {
		return fmt.Errorf("%w: metric timestamp is required", agentos.ErrInvalidRunPlan)
	}
	for key := range sample.Labels {
		if key == "" {
			return fmt.Errorf("%w: metric label key is required", agentos.ErrInvalidRunPlan)
		}
	}

	return nil
}
