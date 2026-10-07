package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/pkg/redis"
)

const (
	// DefaultEventStoreTTL is the TTL for event dedupe keys.
	DefaultEventStoreTTL = 2 * time.Hour
)

// EventDedupeStore stores event idempotency keys for external AgentOS event ingest.
type EventDedupeStore struct {
	rdb *redis.Redis
}

// NewEventDedupeStore creates a Redis-backed dedupe store.
func NewEventDedupeStore(rdb *redis.Redis) *EventDedupeStore {
	return &EventDedupeStore{rdb: rdb}
}

// ClaimEvent returns true when the event ID has not been seen for the run.
func (s *EventDedupeStore) ClaimEvent(ctx context.Context, runID, eventID string) (bool, error) {
	key := fmt.Sprintf("agent:event_dedupe:%s:%s", runID, eventID)

	claimed, err := s.rdb.SetNX(ctx, key, "1", DefaultEventStoreTTL)
	if err != nil {
		return false, fmt.Errorf("event_dedupe: claim: %w", err)
	}

	return claimed, nil
}
