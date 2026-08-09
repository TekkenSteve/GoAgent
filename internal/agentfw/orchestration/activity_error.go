package orchestration

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
)

// llmRateLimitRetries is how many times a rate-limit (HTTP 429) failure is
// retried inside the activity, with exponential backoff + jitter, before the
// error is returned to the platform retry policy. Temporal's RetryPolicy has
// no jitter, so a fleet of agents sharing one provider rate limit would
// otherwise retry in lockstep and re-trigger the limit (retry storm).
const llmRateLimitRetries = 2

// toActivityError maps a domain error onto a Temporal ApplicationError so the
// workflow retry policy can distinguish permanent failures from transient
// ones. AgentErrors marked non-retryable (content filters, context-length
// errors, missing provider support) become non-retryable ApplicationErrors —
// the platform gives up instead of burning retries on a failure more calls
// cannot fix. Everything else passes through unchanged and stays retryable
// under the workflow-level RetryPolicy.
func toActivityError(err error) error {
	var agentErr *entity.AgentError
	if errors.As(err, &agentErr) && !agentErr.Retryable {
		return temporal.NewNonRetryableApplicationError(
			agentErr.Error(),
			string(agentErr.Code),
			agentErr,
			agentErr.UserMessage,
		)
	}

	return err
}

// retryRateLimited runs fn, retrying rate-limit failures (LLM_RATE_LIMIT) up
// to llmRateLimitRetries extra times with exponential backoff + jitter. All
// other errors — including context cancellation — are returned immediately.
//
// Retrying is safe at the call sites in this package: a 429 from the LLM
// provider arrives before any stream delta is written or any cost is
// deducted, so re-running fn produces no duplicate side effects.
func retryRateLimited[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	for attempt := 0; ; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}

		var agentErr *entity.AgentError
		if !errors.As(err, &agentErr) || agentErr.Code != entity.ErrorCodeLLMRateLimit || attempt >= llmRateLimitRetries {
			return result, err
		}

		// Exponential backoff with up to 100% jitter, so agents retrying the
		// same provider limit spread out instead of hitting it in sync.
		delay := time.Duration(1<<attempt) * time.Second
		delay += time.Duration(rand.Int64N(int64(delay))) //nolint:gosec // jitter spreads retries; unpredictability, not security

		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(delay):
		}
	}
}
