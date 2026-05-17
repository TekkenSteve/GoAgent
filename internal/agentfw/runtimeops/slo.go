package runtimeops

import "fmt"

const sloCheckCount = 5

// Default V1 SLO threshold values.
const (
	defaultAdmissionLatencyP95MsMax    = 200
	defaultStepLatencyP95MsMax         = 1500
	defaultCompletionRateMin           = 0.97
	defaultRecoveryTimeP95SecondsMax   = 120
	defaultContinuationSuccessRatioMin = 0.995
)

// SLOSnapshot is an aggregated runtime view used for threshold checks.
type SLOSnapshot struct {
	AdmissionLatencyP95Ms    float64
	StepLatencyP95Ms         float64
	CompletionRate           float64
	RecoveryTimeP95Seconds   float64
	ContinuationSuccessRatio float64
}

// SLOThresholds defines v1 SLO guardrails.
type SLOThresholds struct {
	AdmissionLatencyP95MsMax    float64
	StepLatencyP95MsMax         float64
	CompletionRateMin           float64
	RecoveryTimeP95SecondsMax   float64
	ContinuationSuccessRatioMin float64
}

// DefaultV1SLOThresholds returns baseline dashboard thresholds for v1 rollout.
func DefaultV1SLOThresholds() SLOThresholds {
	return SLOThresholds{
		AdmissionLatencyP95MsMax:    defaultAdmissionLatencyP95MsMax,
		StepLatencyP95MsMax:         defaultStepLatencyP95MsMax,
		CompletionRateMin:           defaultCompletionRateMin,
		RecoveryTimeP95SecondsMax:   defaultRecoveryTimeP95SecondsMax,
		ContinuationSuccessRatioMin: defaultContinuationSuccessRatioMin,
	}
}

// SLOViolation contains a threshold breach.
type SLOViolation struct {
	Metric   string
	Actual   float64
	Expected string
}

// EvaluateSLO returns all threshold violations for a snapshot.
func EvaluateSLO(snapshot SLOSnapshot, thresholds SLOThresholds) []SLOViolation {
	violations := make([]SLOViolation, 0, sloCheckCount)
	if snapshot.AdmissionLatencyP95Ms > thresholds.AdmissionLatencyP95MsMax {
		violations = append(violations, SLOViolation{
			Metric:   "admission_latency_p95_ms",
			Actual:   snapshot.AdmissionLatencyP95Ms,
			Expected: fmt.Sprintf("<= %.3f", thresholds.AdmissionLatencyP95MsMax),
		})
	}

	if snapshot.StepLatencyP95Ms > thresholds.StepLatencyP95MsMax {
		violations = append(violations, SLOViolation{
			Metric:   "step_latency_p95_ms",
			Actual:   snapshot.StepLatencyP95Ms,
			Expected: fmt.Sprintf("<= %.3f", thresholds.StepLatencyP95MsMax),
		})
	}

	if snapshot.CompletionRate < thresholds.CompletionRateMin {
		violations = append(violations, SLOViolation{
			Metric:   "completion_rate",
			Actual:   snapshot.CompletionRate,
			Expected: fmt.Sprintf(">= %.3f", thresholds.CompletionRateMin),
		})
	}

	if snapshot.RecoveryTimeP95Seconds > thresholds.RecoveryTimeP95SecondsMax {
		violations = append(violations, SLOViolation{
			Metric:   "recovery_time_p95_seconds",
			Actual:   snapshot.RecoveryTimeP95Seconds,
			Expected: fmt.Sprintf("<= %.3f", thresholds.RecoveryTimeP95SecondsMax),
		})
	}

	if snapshot.ContinuationSuccessRatio < thresholds.ContinuationSuccessRatioMin {
		violations = append(violations, SLOViolation{
			Metric:   "continuation_success_ratio",
			Actual:   snapshot.ContinuationSuccessRatio,
			Expected: fmt.Sprintf(">= %.3f", thresholds.ContinuationSuccessRatioMin),
		})
	}

	return violations
}
