package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
)

var errPlain = errors.New("plain error")

func rateLimitAgentErr() error {
	return &entity.AgentError{
		Code:      entity.ErrorCodeLLMRateLimit,
		Message:   "429 too many requests",
		Retryable: true,
	}
}

func TestToActivityError(t *testing.T) {
	t.Parallel()

	t.Run("non-retryable AgentError becomes non-retryable ApplicationError", func(t *testing.T) {
		t.Parallel()

		agentErr := &entity.AgentError{
			Code:        entity.ErrorCodeLLMContentFilter,
			Message:     "content filtered",
			UserMessage: "The response was filtered.",
			Retryable:   false,
		}

		converted := toActivityError(agentErr)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, converted, &appErr)
		require.True(t, appErr.NonRetryable())
		require.Equal(t, string(entity.ErrorCodeLLMContentFilter), appErr.Type())
	})

	t.Run("retryable AgentError passes through unchanged", func(t *testing.T) {
		t.Parallel()

		agentErr := &entity.AgentError{
			Code:        entity.ErrorCodeLLMRateLimit,
			Message:     "rate limited",
			UserMessage: "Please wait.",
			Retryable:   true,
		}

		require.Same(t, agentErr, toActivityError(agentErr))
	})

	t.Run("non-AgentError passes through unchanged", func(t *testing.T) {
		t.Parallel()

		require.Same(t, errPlain, toActivityError(errPlain))
	})

	t.Run("wrapped non-retryable AgentError is detected", func(t *testing.T) {
		t.Parallel()

		agentErr := &entity.AgentError{
			Code:      entity.ErrorCodeContextLength,
			Message:   "context too long",
			Retryable: false,
		}
		wrapped := errors.Join(errPlain, agentErr)

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, toActivityError(wrapped), &appErr)
		require.True(t, appErr.NonRetryable())
	})
}

func TestRetryRateLimited(t *testing.T) {
	t.Parallel()

	t.Run("429 is retried with backoff then succeeds", func(t *testing.T) {
		t.Parallel()

		calls := 0
		result, err := retryRateLimited(context.Background(), func() (string, error) {
			calls++
			if calls == 1 {
				return "", rateLimitAgentErr()
			}

			return "ok", nil
		})

		require.NoError(t, err)
		require.Equal(t, "ok", result)
		require.Equal(t, 2, calls)
	})

	t.Run("persistent 429 exhausts retries and returns the error", func(t *testing.T) {
		t.Parallel()

		calls := 0
		_, err := retryRateLimited(context.Background(), func() (string, error) {
			calls++

			return "", rateLimitAgentErr()
		})

		require.Error(t, err)
		require.Equal(t, llmRateLimitRetries+1, calls)
	})

	t.Run("non-rate-limit error is not retried", func(t *testing.T) {
		t.Parallel()

		calls := 0
		got, gotErr := retryRateLimited(context.Background(), func() (string, error) {
			calls++

			return "", errPlain
		})

		require.Same(t, errPlain, gotErr)
		require.Empty(t, got)
		require.Equal(t, 1, calls)
	})
}

func TestRetryRateLimitedContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := retryRateLimited(ctx, func() (string, error) {
		calls++

		return "", rateLimitAgentErr()
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
