package stream

import (
	"context"
	"time"
)

// LiveOnly is the subscribe cursor that skips replay entirely and delivers
// only events published after the subscription is established.
const LiveOnly = -1

// Publisher is the write side of the data plane. Every backend depends only
// on this interface: map its internal output to AG-UI, Publish it to the
// channel, and stop — step 2 of the backend integration contract.
type Publisher interface {
	Publish(ctx context.Context, handle *Handle, ev *Event) error
}

// Subscriber is the read side of a data plane channel. The frontend gateway
// and the control-plane projector both consume through it.
type Subscriber interface {
	// Subscribe starts delivering stored events with Sequence > after.
	// after=0 replays from the start of the retained history; after=LiveOnly
	// delivers only newly published events. Returns a Subscription that
	// delivers until Close is called.
	Subscribe(ctx context.Context, handle *Handle, after int64) (*Subscription, error)
}

// Projector is the control plane's consumption role: it reads the same
// channel the frontend subscribes to and reduces the stream to its durable
// projection. NewProjector promotes any Subscriber into a Projector;
// ProjectToCore is the pure milestone mapping.
type Projector interface {
	Consume(ctx context.Context, handle *Handle, after int64) (*Subscription, error)
}

// StoredEvent is one delivered event with the sequence the bus assigned it.
// Sequence is monotonic per channel and is the cursor for reconnects.
type StoredEvent struct {
	Event    Event
	Sequence int64
	StoredAt time.Time
}

// Subscription delivers stored events until Close is called. The caller must
// Close to release the transport resources (removes the subscriber from the
// bus fan-out).
type Subscription struct {
	C       <-chan StoredEvent
	closeFn func()
}

// NewSubscription creates a Subscription over a delivery channel.
func NewSubscription(ch <-chan StoredEvent, closeFn func()) *Subscription {
	return &Subscription{C: ch, closeFn: closeFn}
}

// Close terminates the subscription and releases its resources. Safe to call
// more than once.
func (s *Subscription) Close() {
	if s.closeFn != nil {
		s.closeFn()
		s.closeFn = nil
	}
}

// projector promotes a Subscriber into the control plane's read role.
type projector struct {
	sub Subscriber
}

// NewProjector wraps a Subscriber as a Projector.
func NewProjector(sub Subscriber) Projector {
	return projector{sub: sub}
}

// Consume delegates to the wrapped subscriber. The returned subscription
// carries the full channel stream; ProjectToCore reduces it to milestones.
func (p projector) Consume(ctx context.Context, handle *Handle, after int64) (*Subscription, error) {
	return p.sub.Subscribe(ctx, handle, after)
}
