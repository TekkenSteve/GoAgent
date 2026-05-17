package pipeline

import (
	"context"
	"time"
)

// jitterPercent is the denominator used for jitter calculation in exponential backoff.
const (
	jitterPercent = 100
	jitterBase    = 0.75
)

// RetryPolicy defines how to retry an operation.
type RetryPolicy interface {
	Delay(attempt int) time.Duration
	ShouldRetry(attempt int, err error) bool
}

// ExponentialBackoff implements RetryPolicy with exponential backoff + jitter.
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	MaxRetry  int
}

func (e *ExponentialBackoff) Delay(attempt int) time.Duration {
	exp := float64(e.BaseDelay)
	for range attempt {
		exp *= 2
	}

	if exp > float64(e.MaxDelay) {
		exp = float64(e.MaxDelay)
	}
	// jitter: ±25%
	jitter := exp * (jitterBase + 0.5*float64(time.Now().UnixNano()%jitterPercent)/float64(jitterPercent))

	return time.Duration(jitter)
}

func (e *ExponentialBackoff) ShouldRetry(attempt int, _ error) bool {
	return attempt < e.MaxRetry
}

// RetryFn runs fn with retry policy. If all retries fail, the last error is returned.
func RetryFn(ctx context.Context, fn func(context.Context) error, policy RetryPolicy) error {
	var lastErr error

	for attempt := 0; ; attempt++ {
		if err := fn(ctx); err != nil {
			lastErr = err
			if !policy.ShouldRetry(attempt, err) {
				return lastErr
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(policy.Delay(attempt)):
			}

			continue
		}

		return nil
	}
}
