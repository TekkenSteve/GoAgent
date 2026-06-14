package orchestration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvaluateContinueAsNew(t *testing.T) {
	t.Parallel()

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
