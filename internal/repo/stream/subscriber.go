package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
)

const subscriberChannelBufferSize = 256

// RedisSubscriber implements stream.Subscriber using redis.StreamHub for fan-out.
//
// It maintains one pump goroutine per stream via StreamHub, so multiple
// subscribers to the same session share a single XREAD connection.
type RedisSubscriber struct {
	hub *redis.StreamHub
}

// NewRedisSubscriber creates a RedisSubscriber.
func NewRedisSubscriber(hub *redis.StreamHub) *RedisSubscriber {
	return &RedisSubscriber{hub: hub}
}

// Subscribe starts receiving events for a session.
// afterSequence maps to the Redis Stream entry ID <sequence>-0 for catch-up.
func (s *RedisSubscriber) Subscribe(_ context.Context, sessionID string, afterSequence int64) (*stream.Subscription, error) {
	streamKey := fmt.Sprintf("agent:events:%s", sessionID)

	// Convert sequence to Stream lastID:
	//   afterSequence=0 → "$" (live-only, no history)
	//   afterSequence>0 → "<sequence>-0" (catch-up from afterSequence+1)
	lastID := "$"
	if afterSequence > 0 {
		lastID = fmt.Sprintf("%d-0", afterSequence)
	}

	hubSub := s.hub.Subscribe(streamKey, lastID)

	// Translate XStreamEntry → stream.StoredEvent via UnmarshalEvent
	ch := make(chan stream.StoredEvent, subscriberChannelBufferSize)

	go func() {
		defer close(ch)

		for entry := range hubSub.C {
			data, ok := entry.Values["data"]
			if !ok || data == "" {
				continue
			}

			event, err := entity.UnmarshalEvent([]byte(data))
			if err != nil {
				continue // skip corrupt events
			}

			seq := parseSequenceFromHubID(entry.ID)
			select {
			case ch <- stream.StoredEvent{
				Event:    event,
				Sequence: seq,
				StoredAt: time.Now().UTC(),
			}:
			default:
				// channel full, drop event (slow consumer)
			}
		}
	}()

	return stream.NewSubscription(ch, hubSub.Close), nil
}

const entryIDParts = 2

func parseSequenceFromHubID(entryID string) int64 {
	// Entry ID format: "<sequence>-0"
	parts := strings.SplitN(entryID, "-", entryIDParts)
	if len(parts) == 0 {
		return 0
	}

	seq, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}

	return seq
}
