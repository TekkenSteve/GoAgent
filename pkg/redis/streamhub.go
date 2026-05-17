package redis

import (
	"context"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	subscriberChannelBufSize = 256
	xreadCount               = 10
	xreadBlockDuration       = 500 * time.Millisecond
	xreadRetryDelay          = 100 * time.Millisecond
)

// StreamHub multiplexes one XREAD per stream key to N subscribers.
// Instead of N goroutines doing XREAD on the same stream (which wastes connections
// and can overload Redis), one pump goroutine per stream reads entries and
// fans them out to all subscribed channels.
//
// Usage:
//
//	sub := hub.Subscribe("mystream", "$")
//	defer sub.Close()
//	for msg := range sub.C {
//	    // process msg
//	}
type StreamHub struct {
	client goredis.Cmdable

	mu     sync.Mutex
	closed bool
	pumps  map[string]context.CancelFunc
	subs   map[string]map[*Subscription]struct{}
	wg     sync.WaitGroup

	// Metrics
	streamsActive    int
	subscribersTotal int
	messagesDropped  int
}

// Subscription represents a single subscriber's stream read session.
// Callers read from C and must call Close() when done.
type Subscription struct {
	C      <-chan XStreamEntry
	ch     chan XStreamEntry
	hub    *StreamHub
	stream string
}

// XStreamEntry is a single entry from a Redis stream.
type XStreamEntry struct {
	Stream string
	ID     string
	Values map[string]string
}

// NewStreamHub creates a StreamHub backed by the given Redis client.
func NewStreamHub(client goredis.Cmdable) *StreamHub {
	return &StreamHub{
		client: client,
		pumps:  make(map[string]context.CancelFunc),
		subs:   make(map[string]map[*Subscription]struct{}),
	}
}

// Subscribe subscribes to a stream starting from lastID ("$" for new only, "0" for all).
// Slow consumers that don't read fast enough will have messages dropped (not block the pump).
//
// The pump goroutine starts when the first subscriber arrives and stops when the
// last subscriber leaves. When the hub is closed, all subscriber channels are
// closed so range loops exit.
func (h *StreamHub) Subscribe(stream, lastID string) *Subscription {
	ch := make(chan XStreamEntry, subscriberChannelBufSize)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		close(ch)

		return &Subscription{
			C:      ch,
			ch:     ch,
			stream: stream,
		}
	}

	sub := &Subscription{
		C:      ch,
		ch:     ch,
		hub:    h,
		stream: stream,
	}

	if h.subs[stream] == nil {
		h.subs[stream] = make(map[*Subscription]struct{})
	}

	h.subs[stream][sub] = struct{}{}
	h.subscribersTotal++

	if _, running := h.pumps[stream]; !running {
		pumpCtx, cancel := context.WithCancel(context.Background())
		h.pumps[stream] = cancel
		h.streamsActive++
		h.wg.Go(func() {
			defer cancel()

			h.pump(pumpCtx, stream, lastID)
		})
	}

	return sub
}

// Unsubscribe removes a subscription. After this, the subscriber channel is closed.
func (h *StreamHub) Unsubscribe(sub *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	subs := h.subs[sub.stream]
	if subs == nil {
		return
	}

	delete(subs, sub)
	close(sub.ch)

	if len(subs) == 0 {
		if cancel, ok := h.pumps[sub.stream]; ok {
			cancel()
			delete(h.pumps, sub.stream)
			h.streamsActive--
		}

		delete(h.subs, sub.stream)
	}
}

// Close cancels all pump goroutines and closes all subscriber channels.
// After Close returns, all range loops on subscriber channels have exited.
func (h *StreamHub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()

		return
	}

	h.closed = true

	for _, cancel := range h.pumps {
		cancel()
	}
	h.mu.Unlock()

	// Wait for all pumps to finish before closing channels.
	// This ensures no pump can send to a closed channel.
	h.wg.Wait()

	h.mu.Lock()
	for _, subs := range h.subs {
		for sub := range subs {
			close(sub.ch)
		}
	}

	h.pumps = make(map[string]context.CancelFunc)
	h.subs = make(map[string]map[*Subscription]struct{})
	h.streamsActive = 0
	h.mu.Unlock()
}

// Close unsubscribes and releases the subscription.
func (s *Subscription) Close() {
	if s.hub != nil {
		s.hub.Unsubscribe(s)
	}
}

// pump is the goroutine that reads from a Redis stream and fans out to subscribers.
func (h *StreamHub) pump(ctx context.Context, stream, lastID string) {
	currentID := lastID

	for {
		if h.shouldStop(ctx) {
			return
		}

		entries, err := h.xreadStream(ctx, stream, currentID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			time.Sleep(xreadRetryDelay)

			continue
		}

		if !h.processStreamEntries(stream, entries, &currentID) {
			return
		}
	}
}

// processStreamEntries fans out all stream entries to subscribers and updates currentID.
// Returns false if the hub is closed.
func (h *StreamHub) processStreamEntries(stream string, entries []goredis.XStream, currentID *string) bool {
	for _, xs := range entries {
		for _, msg := range xs.Messages {
			*currentID = msg.ID

			entry := buildStreamEntry(stream, msg)

			if !h.fanOutEntry(stream, entry) {
				return false
			}
		}
	}

	return true
}

// shouldStop checks if the context has been canceled.
func (h *StreamHub) shouldStop(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// xreadStream performs a blocking XREAD on the Redis stream.
func (h *StreamHub) xreadStream(ctx context.Context, stream, currentID string) ([]goredis.XStream, error) {
	return h.client.XRead(ctx, &goredis.XReadArgs{
		Streams: []string{stream, currentID},
		Count:   xreadCount,
		Block:   xreadBlockDuration,
	}).Result()
}

// buildStreamEntry converts a Redis XMessage into an XStreamEntry.
func buildStreamEntry(stream string, msg goredis.XMessage) XStreamEntry {
	strValues := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		strValues[k] = toString(v)
	}

	return XStreamEntry{
		Stream: stream,
		ID:     msg.ID,
		Values: strValues,
	}
}

// fanOutEntry delivers an entry to all subscribers of the stream.
// Returns false if the hub is closed.
func (h *StreamHub) fanOutEntry(stream string, entry XStreamEntry) bool {
	chans := h.collectSubscriberChannels(stream)
	if chans == nil {
		return false
	}

	for _, ch := range chans {
		h.sendOrDrop(ch, entry)
	}

	return true
}

// collectSubscriberChannels returns a snapshot of all subscriber channels
// for the given stream. Returns nil if the hub is closed.
func (h *StreamHub) collectSubscriberChannels(stream string) []chan XStreamEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}

	subs := h.subs[stream]
	chans := make([]chan XStreamEntry, 0, len(subs))

	for sub := range subs {
		chans = append(chans, sub.ch)
	}

	return chans
}

// sendOrDrop tries to send an entry to a subscriber channel.
// If the channel is full, the message is dropped and the drop counter is incremented.
func (h *StreamHub) sendOrDrop(ch chan XStreamEntry, entry XStreamEntry) {
	select {
	case ch <- entry:
	default:
		h.mu.Lock()
		h.messagesDropped++
		h.mu.Unlock()
	}
}

// Stats returns hub metrics.
func (h *StreamHub) Stats() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()

	return map[string]any{
		"streams_active":    h.streamsActive,
		"subscribers_total": h.subscribersTotal,
		"messages_dropped":  h.messagesDropped,
	}
}

func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	default:
		return ""
	}
}
