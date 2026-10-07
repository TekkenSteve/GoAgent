package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReaderConfigKeepsPoisonPolicy locks the two halves of the poison contract:
// the zero value keeps retrying (a projection whose store is unreachable must
// never have its facts parked), and an explicit policy survives normalization.
func TestReaderConfigKeepsPoisonPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policy   PoisonPolicy
		expected PoisonPolicy
	}{
		{name: "default retries", policy: PoisonRetry, expected: PoisonRetry},
		{name: "parking is carried through", policy: PoisonDeadLetter, expected: PoisonDeadLetter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := ReaderConfig{
				Config:   Config{URL: "nats://127.0.0.1:4222"},
				Domain:   "run.timeline",
				Consumer: "poison",
				OnPoison: tt.policy,
			}

			normalized, err := config.normalized()
			require.NoError(t, err)
			require.Equal(t, tt.expected, normalized.OnPoison)
			require.Equal(t, _defaultMaxDeliver, normalized.MaxDeliver)
			require.Equal(t, 30*time.Second, normalized.AckWait)
		})
	}
}
