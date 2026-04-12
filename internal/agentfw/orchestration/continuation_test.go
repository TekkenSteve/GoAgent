package orchestration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvaluateContinueAsNew(t *testing.T) {
	policy := ContinueAsNewPolicy{
		HistoryLengthThreshold: 10,
		StateSizeThresholdByte: 1024,
		StepThreshold:          3,
		WallClockThreshold:     30 * time.Second,
		MaxContinuations:       2,
	}

	decision := EvaluateContinueAsNew(policy, ContinueAsNewSnapshot{
		HistoryLength:   11,
		StateSizeBytes:  10,
		Step:            1,
		Elapsed:         time.Second,
		ContinuationCnt: 0,
	})
	require.True(t, decision.ShouldContinue)
	require.Equal(t, "history_length_threshold", decision.Reason)

	decision = EvaluateContinueAsNew(policy, ContinueAsNewSnapshot{
		HistoryLength:   1,
		StateSizeBytes:  2048,
		Step:            1,
		Elapsed:         time.Second,
		ContinuationCnt: 0,
	})
	require.True(t, decision.ShouldContinue)
	require.Equal(t, "state_size_threshold", decision.Reason)

	decision = EvaluateContinueAsNew(policy, ContinueAsNewSnapshot{
		HistoryLength:   1,
		StateSizeBytes:  10,
		Step:            3,
		Elapsed:         time.Second,
		ContinuationCnt: 0,
	})
	require.True(t, decision.ShouldContinue)
	require.Equal(t, "step_threshold", decision.Reason)

	decision = EvaluateContinueAsNew(policy, ContinueAsNewSnapshot{
		HistoryLength:   1,
		StateSizeBytes:  10,
		Step:            1,
		Elapsed:         31 * time.Second,
		ContinuationCnt: 0,
	})
	require.True(t, decision.ShouldContinue)
	require.Equal(t, "wall_clock_threshold", decision.Reason)

	decision = EvaluateContinueAsNew(policy, ContinueAsNewSnapshot{
		HistoryLength:   99,
		StateSizeBytes:  2048,
		Step:            9,
		Elapsed:         time.Hour,
		ContinuationCnt: 2,
	})
	require.False(t, decision.ShouldContinue)
}

func TestBuildAndValidateContinuationPayload(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	input := WorkflowInput{
		Request: ExecuteRequest{
			RunID:       "run-1",
			ThreadID:    "thread-1",
			RequestedAt: now.Add(-10 * time.Second),
		},
		Continuation: ContinuationPayload{
			ContinuationCount:  1,
			InitialRequestedAt: now.Add(-30 * time.Second),
		},
	}
	status := RunStatus{
		RunID: "run-1",
		Step:  42,
	}

	payload, err := BuildContinuationPayload(input, status, "wf-123", now)
	require.NoError(t, err)
	require.Equal(t, "run-1", payload.RunID)
	require.Equal(t, "thread-1", payload.ThreadID)
	require.Equal(t, int32(2), payload.ContinuationCount)
	require.Equal(t, "wf-123", payload.PreviousWorkflowID)
	require.Equal(t, int32(42), payload.CarriedStep)
	require.Equal(t, now.Add(-30*time.Second), payload.InitialRequestedAt)
	require.NoError(t, ValidateContinuationPayload(payload))
}

func TestValidateContinuationPayloadFail(t *testing.T) {
	err := ValidateContinuationPayload(ContinuationPayload{
		RunID:             "",
		CarriedStep:       -1,
		ContinuationCount: -1,
	})
	require.Error(t, err)
}
