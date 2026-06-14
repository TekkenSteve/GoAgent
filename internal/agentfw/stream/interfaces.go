// Package stream defines the core streaming event abstractions for the agent framework.
//
// It provides interfaces for Event Sourcing (EventStore), event sequencing (Sequencer),
// pub/sub (Subscriber), and transport gateways (StatelessGateway, StatefulGateway).
// This package depends only on entity/ and belongs to the inner (business logic) layer.
//
// Concrete implementations backed by Redis, WebSocket, etc. live in repo/stream/.
package stream

import (
	"context"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// EventStore is the Event Sourcing interface for agent run events.
// Events are append-only, immutable, and ordered by sequence number per session.
type EventStore interface {
	// Append appends an event to the session's event log.
	// sessionID/runID are injected into the event's BaseEvent metadata during storage.
	// Returns the assigned sequence number.
	Append(ctx context.Context, sessionID, runID string, event entity.StreamEvent) (sequence int64, err error)

	// Replay replays events after a given sequence number.
	// afterSequence=0 replays from the beginning.
	// Returns up to limit events, ordered by sequence ascending.
	Replay(ctx context.Context, sessionID string, afterSequence int64, limit int) ([]StoredEvent, error)

	// SaveSnapshot persists a snapshot of the session state at a given sequence.
	SaveSnapshot(ctx context.Context, sessionID string, sequence int64, state map[string]any) error

	// GetSnapshot retrieves the latest snapshot for a session.
	// Returns (0, nil, nil) if no snapshot exists.
	GetSnapshot(ctx context.Context, sessionID string) (sequence int64, state map[string]any, err error)
}

// StoredEvent represents a single event retrieved from the store.
type StoredEvent struct {
	Event    entity.StreamEvent
	Sequence int64
	StoredAt time.Time
}

// Sequencer generates monotonically increasing sequence numbers for events.
// Each session has its own sequence space, independent of other sessions.
type Sequencer interface {
	Next(ctx context.Context, sessionID string) (int64, error)
}

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

// NewSubscription creates a Subscription with the given channel and close function.
func NewSubscription(ch <-chan StoredEvent, closeFn func()) *Subscription {
	return &Subscription{C: ch, closeFn: closeFn}
}

// Close terminates the subscription and releases underlying resources.
func (s *Subscription) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

// StatelessGateway converts a StreamEvent to a wire format byte slice.
// Each Convert call is independent — no buffering, no aggregation.
type StatelessGateway interface {
	Convert(event entity.StreamEvent) ([]byte, error)
}

// StatefulGateway extends StatelessGateway with buffered output and state reset.
// Used when events need to be aggregated or transformed across multiple events
// before being flushed (e.g., AGUI protocol).
type StatefulGateway interface {
	StatelessGateway
	Flush() ([]byte, error)
	Reset()
}
