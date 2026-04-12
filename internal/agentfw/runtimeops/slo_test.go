package runtimeops

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvaluateSLOPassesWhenWithinThresholds(t *testing.T) {
	thresholds := DefaultV1SLOThresholds()
	snapshot := SLOSnapshot{
		AdmissionLatencyP95Ms:    150,
		StepLatencyP95Ms:         1200,
		CompletionRate:           0.99,
		RecoveryTimeP95Seconds:   60,
		ContinuationSuccessRatio: 0.998,
	}

	violations := EvaluateSLO(snapshot, thresholds)
	require.Empty(t, violations)
}

func TestEvaluateSLOReturnsAllViolations(t *testing.T) {
	thresholds := DefaultV1SLOThresholds()
	snapshot := SLOSnapshot{
		AdmissionLatencyP95Ms:    250,
		StepLatencyP95Ms:         1800,
		CompletionRate:           0.92,
		RecoveryTimeP95Seconds:   180,
		ContinuationSuccessRatio: 0.90,
	}

	violations := EvaluateSLO(snapshot, thresholds)
	require.Len(t, violations, 5)
	require.Equal(t, "admission_latency_p95_ms", violations[0].Metric)
	require.Equal(t, "step_latency_p95_ms", violations[1].Metric)
	require.Equal(t, "completion_rate", violations[2].Metric)
	require.Equal(t, "recovery_time_p95_seconds", violations[3].Metric)
	require.Equal(t, "continuation_success_ratio", violations[4].Metric)
}
