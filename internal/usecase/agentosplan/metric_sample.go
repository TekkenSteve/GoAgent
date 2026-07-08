package agentosplan

import (
	"fmt"
	"maps"
	"math"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// NormalizePlanMetricSample returns the canonical representation used for
// durable metric sample idempotency.
func NormalizePlanMetricSample(sample *PlanMetricSample) PlanMetricSample {
	normalized := *sample
	normalized.Timestamp = normalized.Timestamp.UTC().Truncate(planMetricSampleTimestampPrecision)

	if normalized.Labels == nil {
		normalized.Labels = map[string]string{}
	}

	return normalized
}

// ValidatePlanMetricSample verifies the durable identity and tenant scope of a
// projected PlanMetricSample before a sink records it.
func ValidatePlanMetricSample(sample *PlanMetricSample) error {
	if err := validatePlanMetricSampleIdentity(sample); err != nil {
		return err
	}

	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		return fmt.Errorf("%w: metric value must be finite", agentoscore.ErrInvalidRunPlan)
	}

	if sample.Timestamp.IsZero() {
		return fmt.Errorf("%w: metric timestamp is required", agentoscore.ErrInvalidRunPlan)
	}

	for key := range sample.Labels {
		if key == "" {
			return fmt.Errorf("%w: metric label key is required", agentoscore.ErrInvalidRunPlan)
		}
	}

	return nil
}

func validatePlanMetricSampleIdentity(sample *PlanMetricSample) error {
	required := []struct {
		value string
		label string
	}{
		{value: string(sample.Name), label: "metric name"},
		{value: sample.PlanID, label: "metric plan id"},
		{value: sample.AccountID, label: "metric account id"},
		{value: sample.ProjectID, label: "metric project id"},
		{value: sample.EventID, label: "metric event id"},
		{value: sample.Unit, label: "metric unit"},
	}

	for _, field := range required {
		if field.value == "" {
			return fmt.Errorf("%w: %s is required", agentoscore.ErrInvalidRunPlan, field.label)
		}
	}

	if sample.Sequence <= 0 {
		return fmt.Errorf("%w: metric event sequence must be positive", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}

// ValidatePlanMetricSampleIdempotency verifies that a replayed metric sample is
// the same durable fact as the existing sample for its idempotency key.
func ValidatePlanMetricSampleIdempotency(existing, requested *PlanMetricSample) error {
	existingSample := NormalizePlanMetricSample(existing)

	requestedSample := NormalizePlanMetricSample(requested)

	if !samePlanMetricSampleFact(&existingSample, &requestedSample) {
		return fmt.Errorf("%w: metric sample identity conflict for plan %q event %q", agentoscore.ErrInvalidRunPlan, requestedSample.PlanID, requestedSample.EventID)
	}

	return nil
}

func samePlanMetricSampleFact(existing, requested *PlanMetricSample) bool {
	stringFields := []struct {
		existing  string
		requested string
	}{
		{existing: string(existing.Name), requested: string(requested.Name)},
		{existing: existing.PlanID, requested: requested.PlanID},
		{existing: existing.AccountID, requested: requested.AccountID},
		{existing: existing.ProjectID, requested: requested.ProjectID},
		{existing: existing.NodeID, requested: requested.NodeID},
		{existing: existing.RunID, requested: requested.RunID},
		{existing: existing.EventID, requested: requested.EventID},
		{existing: existing.Unit, requested: requested.Unit},
	}

	for _, field := range stringFields {
		if field.existing != field.requested {
			return false
		}
	}

	return existing.Sequence == requested.Sequence &&
		existing.Value == requested.Value &&
		existing.Timestamp.Equal(requested.Timestamp) &&
		maps.Equal(existing.Labels, requested.Labels)
}
