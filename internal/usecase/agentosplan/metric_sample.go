package agentosplan

import (
	"fmt"
	"maps"
	"math"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// NormalizePlanMetricSample returns the canonical representation used for
// durable metric sample idempotency.
func NormalizePlanMetricSample(sample PlanMetricSample) PlanMetricSample {
	sample.Timestamp = sample.Timestamp.UTC().Truncate(planMetricSampleTimestampPrecision)
	if sample.Labels == nil {
		sample.Labels = map[string]string{}
	}

	return sample
}

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

// ValidatePlanMetricSampleIdempotency verifies that a replayed metric sample is
// the same durable fact as the existing sample for its idempotency key.
func ValidatePlanMetricSampleIdempotency(existing, requested PlanMetricSample) error {
	existing = NormalizePlanMetricSample(existing)
	requested = NormalizePlanMetricSample(requested)
	if existing.Name != requested.Name ||
		existing.PlanID != requested.PlanID ||
		existing.AccountID != requested.AccountID ||
		existing.ProjectID != requested.ProjectID ||
		existing.NodeID != requested.NodeID ||
		existing.RunID != requested.RunID ||
		existing.EventID != requested.EventID ||
		existing.Sequence != requested.Sequence ||
		existing.Value != requested.Value ||
		existing.Unit != requested.Unit ||
		!existing.Timestamp.Equal(requested.Timestamp) ||
		!maps.Equal(existing.Labels, requested.Labels) {
		return fmt.Errorf("%w: metric sample identity conflict for plan %q event %q", agentos.ErrInvalidRunPlan, requested.PlanID, requested.EventID)
	}

	return nil
}
