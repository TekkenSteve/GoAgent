package orchestration

import (
	"fmt"
	"time"
)

// ContinueAsNewPolicy controls continuation trigger decisions.
type ContinueAsNewPolicy struct {
	HistoryLengthThreshold int
	StateSizeThresholdByte int
	StepThreshold          int32
	WallClockThreshold     time.Duration
	MaxContinuations       int32
}

// ContinueAsNewSnapshot is deterministic input for continuation policy checks.
type ContinueAsNewSnapshot struct {
	HistoryLength   int
	StateSizeBytes  int
	Step            int32
	Elapsed         time.Duration
	ContinuationCnt int32
}

// ContinueAsNewDecision describes whether to continue and why.
type ContinueAsNewDecision struct {
	ShouldContinue bool
	Reason         string
}

// ContinuationPayload carries continuity fields across Continue-As-New boundaries.
type ContinuationPayload struct {
	RunID              string
	ThreadID           string
	ContinuationCount  int32
	PreviousWorkflowID string
	CarriedStep        int32
	CarriedAt          time.Time
	InitialRequestedAt time.Time
}

// EvaluateContinueAsNew evaluates continuation policy from deterministic inputs.
func EvaluateContinueAsNew(policy ContinueAsNewPolicy, snapshot ContinueAsNewSnapshot) ContinueAsNewDecision {
	if policy.MaxContinuations > 0 && snapshot.ContinuationCnt >= policy.MaxContinuations {
		return ContinueAsNewDecision{}
	}

	if policy.HistoryLengthThreshold > 0 && snapshot.HistoryLength >= policy.HistoryLengthThreshold {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "history_length_threshold"}
	}

	if policy.StateSizeThresholdByte > 0 && snapshot.StateSizeBytes >= policy.StateSizeThresholdByte {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "state_size_threshold"}
	}

	if policy.StepThreshold > 0 && snapshot.Step >= policy.StepThreshold {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "step_threshold"}
	}

	if policy.WallClockThreshold > 0 && snapshot.Elapsed >= policy.WallClockThreshold {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "wall_clock_threshold"}
	}

	return ContinueAsNewDecision{}
}

// ValidateContinuationPayload validates required continuity fields.
func ValidateContinuationPayload(payload ContinuationPayload) error {
	if payload.RunID == "" {
		return fmt.Errorf("continuation payload: run_id is required")
	}
	if payload.CarriedStep < 0 {
		return fmt.Errorf("continuation payload: carried_step must be >= 0")
	}
	if payload.ContinuationCount < 0 {
		return fmt.Errorf("continuation payload: continuation_count must be >= 0")
	}
	return nil
}

// BuildContinuationPayload creates carry-over payload for Continue-As-New handoff.
func BuildContinuationPayload(
	current WorkflowInput,
	status RunStatus,
	workflowID string,
	now time.Time,
) (ContinuationPayload, error) {
	prevCount := int32(0)
	initialRequestedAt := current.Request.RequestedAt
	if !current.Continuation.InitialRequestedAt.IsZero() {
		initialRequestedAt = current.Continuation.InitialRequestedAt
	}
	if initialRequestedAt.IsZero() {
		initialRequestedAt = now
	}
	if current.Continuation.ContinuationCount > 0 {
		prevCount = current.Continuation.ContinuationCount
	}

	payload := ContinuationPayload{
		RunID:              status.RunID,
		ThreadID:           current.Request.ThreadID,
		ContinuationCount:  prevCount + 1,
		PreviousWorkflowID: workflowID,
		CarriedStep:        status.Step,
		CarriedAt:          now,
		InitialRequestedAt: initialRequestedAt,
	}

	if err := ValidateContinuationPayload(payload); err != nil {
		return ContinuationPayload{}, err
	}
	return payload, nil
}
