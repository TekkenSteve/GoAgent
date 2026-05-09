package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
)

// Subscriber receives events from the EventStore for a given session.
// This abstracts away the underlying stream transport (Redis Stream, channel, etc.)
// and provides parsed StreamEvent values.
type Subscriber interface {
	// Subscribe starts receiving events for a session starting from afterSequence.
	// afterSequence=0 starts from the beginning of the stream ("$" for live-only).
	// Returns a Subscription that delivers events until Close() is called.
	Subscribe(ctx context.Context, sessionID string, afterSequence int64) (*Subscription, error)
}

// Subscription provides a channel of stored events.
// The caller must call Close() to release resources.
type Subscription struct {
	C       <-chan StoredEvent
	closeFn func()
}

// Close terminates the subscription and releases underlying resources.
func (s *Subscription) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

// RedisSubscriber implements Subscriber using redis.StreamHub for fan-out.
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
func (s *RedisSubscriber) Subscribe(_ context.Context, sessionID string, afterSequence int64) (*Subscription, error) {
	streamKey := fmt.Sprintf("agent:events:%s", sessionID)

	// Convert sequence to Stream lastID:
	//   afterSequence=0 → "$" (live-only, no history)
	//   afterSequence>0 → "<sequence>-0" (catch-up from afterSequence+1)
	lastID := "$"
	if afterSequence > 0 {
		lastID = fmt.Sprintf("%d-0", afterSequence)
	}

	hubSub := s.hub.Subscribe(streamKey, lastID)

	// Translate XStreamEntry → StoredEvent via UnmarshalEvent
	ch := make(chan StoredEvent, 256)
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
			case ch <- StoredEvent{
				Event:    event,
				Sequence: seq,
				StoredAt: time.Now().UTC(),
			}:
			default:
				// channel full, drop event (slow consumer)
			}
		}
	}()

	return &Subscription{
		C:       ch,
		closeFn: hubSub.Close,
	}, nil
}

func parseSequenceFromHubID(entryID string) int64 {
	// Entry ID format: "<sequence>-0"
	parts := strings.SplitN(entryID, "-", 2)
	if len(parts) < 1 {
		return 0
	}
	seq, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}
	return seq
}
