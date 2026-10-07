package orchestration

import (
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
	// HistoryRef is the claim-check reference to the message history snapshot
	// (see SnapshotHistoryActivity). Empty when no snapshot was taken (no blob
	// store configured, or the run started before the feature shipped); the
	// continued workflow then rebuilds messages from the initial request.
	HistoryRef string
}

func thresholdExceeded(threshold, value int) bool {
	return threshold > 0 && value >= threshold
}

// EvaluateContinueAsNew evaluates continuation policy from deterministic inputs.
func EvaluateContinueAsNew(policy ContinueAsNewPolicy, snapshot ContinueAsNewSnapshot) ContinueAsNewDecision {
	if thresholdExceeded(int(policy.MaxContinuations), int(snapshot.ContinuationCnt)) {
		return ContinueAsNewDecision{}
	}

	if thresholdExceeded(policy.HistoryLengthThreshold, snapshot.HistoryLength) {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "history_length_threshold"}
	}

	if thresholdExceeded(policy.StateSizeThresholdByte, snapshot.StateSizeBytes) {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "state_size_threshold"}
	}

	if thresholdExceeded(int(policy.StepThreshold), int(snapshot.Step)) {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "step_threshold"}
	}

	if thresholdExceeded(int(policy.WallClockThreshold), int(snapshot.Elapsed)) {
		return ContinueAsNewDecision{ShouldContinue: true, Reason: "wall_clock_threshold"}
	}

	return ContinueAsNewDecision{}
}
