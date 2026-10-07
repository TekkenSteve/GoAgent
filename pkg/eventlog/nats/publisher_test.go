package nats

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/require"
)

// TestPublisherRejectsInvalidRecordKey locks the subject's integrity: the key
// is the subject's last segment, so a key with a separator would reshape the
// subject and a wildcard would widen it. Both are refused before any bytes
// reach the broker — a producer's bug becomes a publish error, not a record
// nobody can find again.
func TestPublisherRejectsInvalidRecordKey(t *testing.T) {
	t.Parallel()

	publisher := &Publisher{config: Config{SubjectPrefix: "agentos", Shards: 4}}

	for _, key := range []string{"", "run.abc", "run abc", "*", ">"} {
		require.ErrorIs(t, publisher.Publish(t.Context(), &eventlog.Record{
			Domain: eventlog.DomainPlanEvents,
			Key:    key,
		}), ErrRecordKeyInvalid, "key %q", key)
	}
}
