// Package memstream provides an in-process data-plane bus used by tests and
// single-node dev. It implements agentos/stream.Publisher and
// agentos/stream.Subscriber with ordered, replayable, per-channel
// history — the same delivery contract the production bus (Centrifugo)
// must satisfy.
package memstream

import (
	"context"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
)

const (
	defaultHistory = 1000 // max events retained per channel
	defaultBuffer  = 64   // per-subscriber delivery buffer
)

// Options tune the in-memory bus.
type Options struct {
	// History is the max number of events retained per channel for replay.
	History int
	// Buffer is the per-subscriber delivery buffer before slow consumers drop.
	Buffer int
}

// Option mutates Options.
type Option func(*Options)

// WithHistory sets the retained replay window per channel.
func WithHistory(n int) Option {
	return func(o *Options) { o.History = n }
}

// WithBuffer sets the per-subscriber delivery buffer.
func WithBuffer(n int) Option {
	return func(o *Options) { o.Buffer = n }
}

func (o *Options) normalize() {
	if o.History <= 0 {
		o.History = defaultHistory
	}

	if o.Buffer <= 0 {
		o.Buffer = defaultBuffer
	}
}

// Bus is an in-process Publisher+Subscriber. One bus serves many channels
// (run timelines); each channel keeps a bounded history and fans out to live
// subscribers. A slow subscriber is dropped rather than blocking the
// publisher — the same policy as the Redis fan-out it replaces.
type Bus struct {
	mu       sync.Mutex
	history  int
	buffer   int
	channels map[string]*channel
}

// New creates a Bus with defaults (or Options overrides).
func New(opts ...Option) *Bus {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}

	o.normalize()

	return &Bus{
		history:  o.History,
		buffer:   o.Buffer,
		channels: make(map[string]*channel),
	}
}

type channel struct {
	events  []stream.StoredEvent // bounded, oldest-first
	next    int64                // next sequence to assign (starts at 1)
	nextSub int64
	live    map[int64]*memSub
}

// memSub is one live subscriber. Its live channel is written only by Publish
// and drained by the subscription pump, which owns ordering.
type memSub struct {
	id   int64
	live chan stream.StoredEvent
	done chan struct{}
}

// Publish appends the event to the channel history and fans it out to live
// subscribers. The event and handle are validated at this boundary, so a
// backend publishing garbage is rejected here, not at the frontend.
func (b *Bus) Publish(ctx context.Context, handle *stream.Handle, ev *stream.Event) error {
	if err := handle.Validate(); err != nil {
		return err
	}

	if err := ev.Validate(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	ch := b.channelFor(handle.Channel)
	stored := stream.StoredEvent{Event: *ev, Sequence: ch.next, StoredAt: time.Now()}
	ch.next++
	ch.events = append(ch.events, stored)

	if len(ch.events) > b.history {
		ch.events = ch.events[len(ch.events)-b.history:]
	}

	subs := make([]*memSub, 0, len(ch.live))
	for _, sub := range ch.live {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	for _, sub := range subs {
		select {
		case sub.live <- stored:
		default: // slow consumer: drop, same as the Redis fan-out it replaces
		}
	}

	return nil
}

// Subscribe delivers events with Sequence > after (after=0 replays from the
// start of the retained history; after=stream.LiveOnly is live-only). The
// subscription MUST be closed to release the pump goroutine.
func (b *Bus) Subscribe(ctx context.Context, handle *stream.Handle, after int64) (*stream.Subscription, error) {
	if err := handle.Validate(); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	ch := b.channelFor(handle.Channel)
	replay := ch.replayAfter(after)
	sub := &memSub{id: ch.nextSub, live: make(chan stream.StoredEvent, b.buffer), done: make(chan struct{})}
	ch.nextSub++
	ch.live[sub.id] = sub
	b.mu.Unlock()

	// The pump is the single writer to out: it drains replay (older) first,
	// then bridges live events (newer), so order is preserved even while
	// publishes race the subscription setup — the registration happened
	// atomically with the replay capture.
	out := make(chan stream.StoredEvent, b.buffer)
	go pump(out, sub.done, replay, sub.live)

	return stream.NewSubscription(out, func() {
		b.mu.Lock()
		delete(ch.live, sub.id)
		b.mu.Unlock()

		close(sub.done)
	}), nil
}

// pump delivers a subscription's events, closing out when done fires. It is
// the only writer to out, which is what guarantees strict sequence ordering.
func pump(out chan<- stream.StoredEvent, done <-chan struct{}, replay []stream.StoredEvent, live <-chan stream.StoredEvent) {
	defer close(out)

	for i := range replay {
		if !forward(out, &replay[i], done) {
			return
		}
	}

	for {
		select {
		case ev := <-live:
			if !forward(out, &ev, done) {
				return
			}
		case <-done:
			return
		}
	}
}

// forward writes one event to out, aborting when the subscription is closed.
func forward(out chan<- stream.StoredEvent, ev *stream.StoredEvent, done <-chan struct{}) bool {
	select {
	case out <- *ev:
		return true
	case <-done:
		return false
	}
}

// channelFor returns the channel, creating it on first use (subscribing to a
// channel with no history yet is legal and just waits for the first publish).
func (b *Bus) channelFor(name string) *channel {
	ch := b.channels[name]
	if ch != nil {
		return ch
	}

	ch = &channel{next: 1, live: make(map[int64]*memSub)}
	b.channels[name] = ch

	return ch
}

// replayAfter returns retained events with Sequence > after. stream.LiveOnly
// (-1) must yield nothing even though sequences start at 1.
func (c *channel) replayAfter(after int64) []stream.StoredEvent {
	if after == stream.LiveOnly {
		return nil
	}

	if len(c.events) == 0 {
		return nil
	}

	for i := range c.events {
		if c.events[i].Sequence > after {
			return c.events[i:]
		}
	}

	return nil
}
