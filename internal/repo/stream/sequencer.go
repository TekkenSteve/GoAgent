// Package stream provides concrete implementations of agentfw/stream interfaces.
package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/redis"
)

// sequencerKeyTTL is how long sequencer keys live after last access.
// Aligned with DefaultEventStoreTTL so sequence space and event store
// are cleaned up on the same cadence.
const sequencerKeyTTL = 2 * time.Hour

// RedisSequencer implements stream.Sequencer using Redis INCR for distributed sequence generation.
// The sequence key is scoped per sessionID, with format "agent:seq:<sessionID>".
type RedisSequencer struct {
	rdb *redis.Redis
}

// NewRedisSequencer creates a RedisSequencer.
func NewRedisSequencer(rdb *redis.Redis) *RedisSequencer {
	return &RedisSequencer{rdb: rdb}
}

// Next atomically increments the sequence counter for the given session.
// Best-effort refreshes the key TTL so stale sessions are eventually cleaned up.
func (s *RedisSequencer) Next(ctx context.Context, sessionID string) (int64, error) {
	key := fmt.Sprintf("agent:seq:%s", sessionID)

	seq, err := s.rdb.Incr(ctx, key)
	if err != nil {
		return 0, err
	}
	// Best-effort TTL refresh
	if _, err := s.rdb.Expire(ctx, key, sequencerKeyTTL); err != nil {
		_ = err
	}

	return seq, nil
}
