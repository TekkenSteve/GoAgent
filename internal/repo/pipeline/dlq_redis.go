package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	dlqQueueKey   = "dlq:failed_writes"
	dlqMaxEntries = 10000
	dlqEntryTTL   = 7 * 24 * time.Hour
)

// DLQEntry represents a failed WAL entry sent to the dead letter queue.
type DLQEntry struct {
	EntryID   string  `json:"entry_id"`
	RunID     string  `json:"run_id"`
	WriteType string  `json:"write_type"`
	Data      string  `json:"data"`
	Error     string  `json:"error"`
	Attempts  int     `json:"attempts"`
	CreatedAt float64 `json:"created_at"`
	FailedAt  float64 `json:"failed_at"`
}

// DLQHandler is a callback for new DLQ entries.
type DLQHandler func(DLQEntry)

// DeadLetterQueue collects WAL entries that failed after max retries.
// Uses a global Redis Stream with TTL for observability and recovery.
type DeadLetterQueue struct {
	client   goredis.Cmdable
	mu       sync.RWMutex
	handlers []DLQHandler
}

// NewDeadLetterQueue creates a DLQ backed by the given Redis client.
func NewDeadLetterQueue(client goredis.Cmdable) *DeadLetterQueue {
	return &DeadLetterQueue{
		client:   client,
		handlers: make([]DLQHandler, 0),
	}
}

// OnEntry registers a handler invoked when a new entry is added to the DLQ.
func (q *DeadLetterQueue) OnEntry(handler DLQHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers = append(q.handlers, handler)
}

// Send adds a failed entry to the DLQ.
func (q *DeadLetterQueue) Send(ctx context.Context, entry DLQEntry) error {
	entry.FailedAt = float64(time.Now().UnixNano()) / 1e9

	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("DLQ - Send - marshal: %w", err)
	}

	err = q.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: dlqQueueKey,
		Values: map[string]any{"payload": string(payload)},
		MaxLen: dlqMaxEntries,
		Approx: true,
	}).Err()
	if err != nil {
		return fmt.Errorf("DLQ - Send - XAdd: %w", err)
	}

	q.client.Expire(ctx, dlqQueueKey, dlqEntryTTL)

	q.mu.RLock()
	handlers := make([]DLQHandler, len(q.handlers))
	copy(handlers, q.handlers)
	q.mu.RUnlock()

	for _, h := range handlers {
		h(entry)
	}

	return nil
}

// GetEntries returns DLQ entries, optionally filtered by runID.
func (q *DeadLetterQueue) GetEntries(ctx context.Context, count int, runID string) ([]DLQEntry, error) {
	results, err := q.client.XRange(ctx, dlqQueueKey, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("DLQ - GetEntries - XRange: %w", err)
	}

	entries := make([]DLQEntry, 0, len(results))
	for _, msg := range results {
		payload, ok := msg.Values["payload"].(string)
		if !ok {
			continue
		}
		var entry DLQEntry
		if err := json.Unmarshal([]byte(payload), &entry); err != nil {
			continue
		}
		if runID == "" || entry.RunID == runID {
			entries = append(entries, entry)
			if count > 0 && len(entries) >= count {
				break
			}
		}
	}

	return entries, nil
}

// RetryEntry moves an entry from DLQ back to the WAL for reprocessing.
func (q *DeadLetterQueue) RetryEntry(ctx context.Context, entryID string, wal *WriteAheadLog) error {
	results, err := q.client.XRange(ctx, dlqQueueKey, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("DLQ - RetryEntry - XRange: %w", err)
	}

	for _, msg := range results {
		payload, ok := msg.Values["payload"].(string)
		if !ok {
			continue
		}
		var entry DLQEntry
		if err := json.Unmarshal([]byte(payload), &entry); err != nil {
			continue
		}
		if entry.EntryID != entryID {
			continue
		}

		if _, err := wal.Append(ctx, entry.RunID, WriteType(entry.WriteType), entry.Data); err != nil {
			return fmt.Errorf("DLQ - RetryEntry - wal.Append: %w", err)
		}

		q.client.XDel(ctx, dlqQueueKey, msg.ID)
		return nil
	}

	return fmt.Errorf("DLQ - RetryEntry - entry %s not found", entryID)
}

// DeleteEntry removes an entry from the DLQ.
func (q *DeadLetterQueue) DeleteEntry(ctx context.Context, entryID string) error {
	results, err := q.client.XRange(ctx, dlqQueueKey, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("DLQ - DeleteEntry - XRange: %w", err)
	}

	for _, msg := range results {
		payload, ok := msg.Values["payload"].(string)
		if !ok {
			continue
		}
		var entry DLQEntry
		if err := json.Unmarshal([]byte(payload), &entry); err != nil {
			continue
		}
		if entry.EntryID == entryID {
			return q.client.XDel(ctx, dlqQueueKey, msg.ID).Err()
		}
	}

	return nil
}

// Len returns the total number of entries in the DLQ.
func (q *DeadLetterQueue) Len(ctx context.Context) (int64, error) {
	return q.client.XLen(ctx, dlqQueueKey).Result()
}

// Purge removes all entries from the DLQ.
func (q *DeadLetterQueue) Purge(ctx context.Context) error {
	return q.client.Del(ctx, dlqQueueKey).Err()
}

// CollectEntries returns a random sample of DLQ entries for observability.
func (q *DeadLetterQueue) CollectEntries(ctx context.Context, sampleSize int) ([]DLQEntry, error) {
	entries, err := q.GetEntries(ctx, sampleSize*2, "")
	if err != nil {
		return nil, err
	}

	if len(entries) <= sampleSize {
		return entries, nil
	}

	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})

	return entries[:sampleSize], nil
}
