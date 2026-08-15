package centrifugo

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	centrifuge "github.com/centrifugal/centrifuge-go"
)

// Subscriber implements stream.Subscriber over a centrifuge-go websocket
// client. Each Subscribe establishes its own connection and replays the
// retained history window (Sequence > after) before bridging to live delivery,
// so the stream the consumer sees is ordered and gapless even when events were
// published while the subscription was being set up.
type Subscriber struct {
	config Config
}

// NewSubscriber creates a Subscriber for one Centrifugo endpoint.
func NewSubscriber(config Config) (*Subscriber, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Subscriber{config: config.normalize()}, nil
}

// Subscribe delivers stored events with Sequence > after, in order.
//
// The centrifuge-go client in this version (v0.10.x, pinned to the server's
// protocol) cannot seed a recovery cursor on a fresh subscribe, so replay is
// done explicitly: History(since after) for the retained window, then live
// delivery via OnPublication. A per-subscription pump owns the single write to
// the returned channel, which is what guarantees the replay→live ordering.
func (s *Subscriber) Subscribe(ctx context.Context, handle *stream.Handle, after int64) (*stream.Subscription, error) {
	if err := handle.Validate(); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cli := centrifuge.NewJsonClient(wsEndpoint(s.config.BaseURL), centrifuge.Config{})

	// The client outlives this function: on the success path the returned
	// subscription closes it via closeFn. Every error path closes it here.
	sub, err := s.establish(ctx, cli, handle.Channel)
	if err != nil {
		cli.Close()

		return nil, err
	}

	// live is the single ordered source for events arriving after subscribe.
	// The OnPublication callback runs on the client read loop and must never
	// block it, so a slow consumer drops — the same policy as the memstream
	// fan-out.
	live := make(chan centrifuge.PublicationEvent, subscriptionBuffer)

	sub.OnPublication(func(e centrifuge.PublicationEvent) {
		deliverPub(live, e)
	})

	replay, err := s.replay(ctx, sub, after)
	if err != nil {
		cli.Close()

		return nil, err
	}

	out := make(chan stream.StoredEvent, subscriptionBuffer)
	done := make(chan struct{})
	stopped := make(chan struct{})

	var closeOnce sync.Once

	closeFn := func() {
		closeOnce.Do(func() {
			close(done)

			// Wait until the pump has closed out: after Close returns, no
			// further event can be delivered to the consumer.
			<-stopped

			// Closing the client tears the connection (and the subscription)
			// down; an explicit Unsubscribe would be redundant here.
			cli.Close()
		})
	}

	go pump(out, done, replay, live, stopped)

	return stream.NewSubscription(out, closeFn), nil
}

// establish dials the websocket, subscribes to the run channel, and blocks
// until the server acknowledges both, reports a failure, times out, or the
// caller cancels.
func (s *Subscriber) establish(ctx context.Context, cli *centrifuge.Client, channel string) (*centrifuge.Subscription, error) {
	connected := make(chan struct{}, 1)

	cli.OnConnected(func(centrifuge.ConnectedEvent) {
		signal(connected)
	})

	connErr := make(chan error, 1)

	cli.OnError(func(e centrifuge.ErrorEvent) {
		report(connErr, e.Error)
	})

	if err := cli.Connect(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectFailed, err)
	}

	if err := s.await(ctx, connected, connErr, ErrConnectFailed, "connection timed out"); err != nil {
		return nil, err
	}

	sub, err := cli.NewSubscription(channel, centrifuge.SubscriptionConfig{
		Positioned:  true,
		Recoverable: true,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSubscribeFailed, err)
	}

	subscribed := make(chan struct{}, 1)

	sub.OnSubscribed(func(centrifuge.SubscribedEvent) {
		signal(subscribed)
	})

	subErr := make(chan error, 1)

	sub.OnError(func(e centrifuge.SubscriptionErrorEvent) {
		report(subErr, e.Error)
	})

	if err := sub.Subscribe(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSubscribeFailed, err)
	}

	if err := s.await(ctx, subscribed, subErr, ErrSubscribeFailed, "subscribe timed out"); err != nil {
		return nil, err
	}

	return sub, nil
}

// await blocks until the server acknowledges an operation, reports an error,
// times out, or the caller cancels.
func (s *Subscriber) await(ctx context.Context, ok <-chan struct{}, errs <-chan error, sentinel error, timeoutMsg string) error {
	select {
	case <-ok:
		return nil
	case err := <-errs:
		return fmt.Errorf("%w: %w", sentinel, err)
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.config.ConnectTimeout):
		return fmt.Errorf("%w: %s", sentinel, timeoutMsg)
	}
}

// signal notifies a waiter non-blockingly; the waiter either observes it or a
// later error/timeout path wins.
func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// report forwards an error non-blockingly; a slow consumer drops it rather
// than stalling the client read loop.
func report(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}

// deliverPub forwards one live publication non-blockingly; a slow consumer
// drops it — the same policy as the memstream fan-out.
func deliverPub(ch chan<- centrifuge.PublicationEvent, e centrifuge.PublicationEvent) {
	select {
	case ch <- e:
	default:
	}
}

// replay fetches the retained history window from the server. A subscription
// must be established before History can be read. The LiveOnly sentinel skips
// history entirely; any other negative cursor (a caller bug) is clamped to the
// start of the window.
func (s *Subscriber) replay(ctx context.Context, sub *centrifuge.Subscription, after int64) ([]stream.StoredEvent, error) {
	if after == stream.LiveOnly {
		return nil, nil
	}

	if after < 0 {
		after = 0
	}

	hctx, cancel := context.WithTimeout(ctx, s.config.ConnectTimeout)
	defer cancel()

	res, err := sub.History(hctx,
		centrifuge.WithHistoryLimit(historyLimit),
		centrifuge.WithHistorySince(&centrifuge.StreamPosition{Offset: uint64(after)}),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrHistoryFailed, err)
	}

	replay := make([]stream.StoredEvent, 0, len(res.Publications))
	for _, pub := range res.Publications {
		ev, ok := decodePublication(pub)
		if !ok {
			continue
		}

		replay = append(replay, ev)
	}

	return replay, nil
}

// decodePublication turns one wire publication into the contract's StoredEvent.
// An oversized offset (beyond int64) is dropped rather than wrapping negative —
// it cannot be a real sequence number on any broker the adapter targets.
func decodePublication(pub centrifuge.Publication) (stream.StoredEvent, bool) {
	var ev stream.Event
	if err := json.Unmarshal(pub.Data, &ev); err != nil {
		return stream.StoredEvent{}, false
	}

	if pub.Offset > math.MaxInt64 {
		return stream.StoredEvent{}, false
	}

	return stream.StoredEvent{
		Event:    ev,
		Sequence: int64(pub.Offset),
		StoredAt: time.Now(),
	}, true
}

// pump is the single writer to out: it flushes the replayed window first, then
// bridges live publications. Because History completed before the pump starts,
// any live event that overlaps the replayed window (published while History
// was in flight) carries an offset already delivered and is skipped. done
// closes out when the subscription is closed.
func pump(out chan<- stream.StoredEvent, done <-chan struct{}, replay []stream.StoredEvent, live <-chan centrifuge.PublicationEvent, stopped chan<- struct{}) {
	defer close(out)
	defer close(stopped)

	var last int64

	for i := range replay {
		ev := replay[i]
		if !forward(out, &ev, done) {
			return
		}

		last = ev.Sequence
	}

	for {
		select {
		case pub := <-live:
			ev, ok := decodePublication(pub.Publication)
			if !ok || ev.Sequence <= last {
				continue
			}

			if !forward(out, &ev, done) {
				return
			}

			last = ev.Sequence
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
