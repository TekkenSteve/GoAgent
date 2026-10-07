package conversation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

var errTestConnectionReset = errors.New("connection reset")

// Serializable isolation is what lets the runtime promise ordered, once-only
// ingestion under concurrent writers. Postgres aborts a transaction when that
// promise is at risk; the abort is the mechanism working, so it is replayed
// rather than handed to the caller as a failure.

func TestIsRetryableTxFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "serialization failure",
			err:  &pgconn.PgError{Code: pgSerializationFailure, Message: "could not serialize access"},
			want: true,
		},
		{
			name: "deadlock detected",
			err:  &pgconn.PgError{Code: pgDeadlockDetected, Message: "deadlock detected"},
			want: true,
		},
		{
			name: "a duplicate source event is a race the replay resolves",
			err:  &pgconn.PgError{Code: pgUniqueViolation, Message: "duplicate key value"},
			want: true,
		},
		{
			name: "a constraint violation that is not a race stays visible",
			err:  &pgconn.PgError{Code: "23503", Message: "foreign key violation"},
		},
		{
			name: "wrapped serialization failure",
			err:  error(&pgconn.PgError{Code: pgSerializationFailure}),
			want: true,
		},
		{
			name: "not a database error",
			err:  errTestConnectionReset,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, isRetryableTxFailure(tt.err))
		})
	}
}

// The retry loop replays a serialization failure through the real policy,
// reports each replay, and gives up after the bounded number of attempts
// rather than looping forever.
func TestRetrySerializableReplaysAndGivesUp(t *testing.T) {
	t.Parallel()

	serializationFailure := &pgconn.PgError{Code: pgSerializationFailure}

	var (
		attempts int
		observed []int
	)

	_, err := retrySerializable(t.Context(), func(attempt int, err error) {
		observed = append(observed, attempt)

		require.ErrorIs(t, err, serializationFailure)
	}, func() (string, error) {
		attempts++

		return "", serializationFailure
	})

	require.ErrorIs(t, err, serializationFailure, "an unresolvable abort reaches the caller")
	require.Equal(t, conversationTxRetries+1, attempts, "the initial attempt plus the bounded replays")
	require.Equal(t, []int{1, 2, 3}, observed)
}

func TestRetrySerializableReturnsTheFirstSuccess(t *testing.T) {
	t.Parallel()

	serializationFailure := &pgconn.PgError{Code: pgSerializationFailure}

	var attempts int

	result, err := retrySerializable(t.Context(), nil, func() (string, error) {
		attempts++

		if attempts == 1 {
			return "", serializationFailure
		}

		return "persisted", nil
	})

	require.NoError(t, err)
	require.Equal(t, "persisted", result)
	require.Equal(t, 2, attempts)
}

// A failure a replay cannot resolve is not replayed: the caller sees exactly
// what the database said.
func TestRetrySerializableDoesNotReplayPermanentFailures(t *testing.T) {
	t.Parallel()

	foreignKeyViolation := &pgconn.PgError{Code: "23503"}

	var attempts int

	_, err := retrySerializable(t.Context(), nil, func() (string, error) {
		attempts++

		return "", foreignKeyViolation
	})

	require.ErrorIs(t, err, foreignKeyViolation)
	require.Equal(t, 1, attempts)
}

func TestWaitForRetryHonorsContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, waitForRetry(ctx, conversationTxRetries), context.Canceled)
}

func TestWaitForRetryBacksOff(t *testing.T) {
	t.Parallel()

	start := time.Now()

	require.NoError(t, waitForRetry(t.Context(), 0))
	require.GreaterOrEqual(t, time.Since(start), conversationTxRetryBackoff)

	// The pause grows with each attempt: an instant replay against contention
	// is the least likely to succeed.
	require.Greater(t, conversationTxRetryBackoff<<1, conversationTxRetryBackoff)
}
