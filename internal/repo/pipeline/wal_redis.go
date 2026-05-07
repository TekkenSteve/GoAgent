package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

const (
	walStreamPrefix      = "wal:run:"
	walStreamMaxLen      = 1000
	walEntryTTL          = 3600 * time.Second
	walLocalBufferSize   = 100
	walMaxLocalRunBuffer = 50
)

// WriteType categorizes WAL entries for the batch writer.
type WriteType string

const (
	WriteTypeMessage    WriteType = "message"
	WriteTypeToolResult WriteType = "tool_result"
)

// WALEntry is a single write-ahead log entry persisted to Redis Stream.
type WALEntry struct {
	EntryID     string    `json:"entry_id"`
	RunID       string    `json:"run_id"`
	WriteType   WriteType `json:"write_type"`
	Data        string    `json:"data"`
	CreatedAt   float64   `json:"created_at"`
	Attempts    int       `json:"attempts"`
	LastAttempt float64   `json:"last_attempt,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
}

// WriteAheadLog provides a Redis Stream-backed write-ahead log for agent persistence.
// It wraps a Redis client and maintains a local buffer as fallback for Redis outages.
type WriteAheadLog struct {
	client goredis.Cmdable

	mu          sync.Mutex
	localBuffer map[string][]WALEntry // runID -> entries (FIFO)
}

// NewWriteAheadLog creates a WAL backed by the given Redis client.
func NewWriteAheadLog(client goredis.Cmdable) *WriteAheadLog {
	return &WriteAheadLog{
		client:      client,
		localBuffer: make(map[string][]WALEntry),
	}
}

// Append writes an entry to the WAL (Redis Stream first, local buffer fallback).
func (w *WriteAheadLog) Append(ctx context.Context, runID string, writeType WriteType, data string) (string, error) {
	entry := WALEntry{
		EntryID:   uuid.New().String(),
		RunID:     runID,
		WriteType: writeType,
		Data:      data,
		CreatedAt: float64(time.Now().UnixNano()) / 1e9,
	}

	payload, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("WAL - Append - marshal: %w", err)
	}

	streamKey := walStreamPrefix + runID

	err = w.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"payload": string(payload)},
		MaxLen: walStreamMaxLen,
		Approx: true,
	}).Err()
	if err == nil {
		w.client.Expire(ctx, streamKey, walEntryTTL)
		return entry.EntryID, nil
	}

	// Fallback: local buffer
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.localBuffer[runID]; !ok {
		if len(w.localBuffer) >= walMaxLocalRunBuffer {
			for k := range w.localBuffer {
				delete(w.localBuffer, k)
				break
			}
		}
		w.localBuffer[runID] = make([]WALEntry, 0, walLocalBufferSize)
	}

	buf := w.localBuffer[runID]
	if len(buf) >= walLocalBufferSize {
		buf = buf[1:]
	}
	w.localBuffer[runID] = append(buf, entry)

	return entry.EntryID, nil
}

// GetPending returns all pending WAL entries for a run (Redis + local buffer).
func (w *WriteAheadLog) GetPending(ctx context.Context, runID string) ([]WALEntry, error) {
	streamKey := walStreamPrefix + runID
	var entries []WALEntry

	results, err := w.client.XRange(ctx, streamKey, "-", "+").Result()
	if err == nil {
		for _, msg := range results {
			payload, ok := msg.Values["payload"].(string)
			if !ok {
				continue
			}
			var entry WALEntry
			if err := json.Unmarshal([]byte(payload), &entry); err != nil {
				continue
			}
			entries = append(entries, entry)
		}
	}

	w.mu.Lock()
	localEntries := make([]WALEntry, 0)
	if buf, ok := w.localBuffer[runID]; ok {
		seen := make(map[string]bool, len(entries))
		for _, e := range entries {
			seen[e.EntryID] = true
		}
		for _, e := range buf {
			if !seen[e.EntryID] {
				localEntries = append(localEntries, e)
			}
		}
	}
	w.mu.Unlock()

	entries = append(entries, localEntries...)
	return entries, nil
}

// MarkCompleted removes entries from the WAL after successful flush.
func (w *WriteAheadLog) MarkCompleted(ctx context.Context, runID string, entryIDs []string) (int, error) {
	if len(entryIDs) == 0 {
		return 0, nil
	}

	streamKey := walStreamPrefix + runID
	completed := 0

	results, err := w.client.XRange(ctx, streamKey, "-", "+").Result()
	if err == nil {
		toDelete := make([]string, 0)
		entrySet := make(map[string]struct{}, len(entryIDs))
		for _, id := range entryIDs {
			entrySet[id] = struct{}{}
		}

		for _, msg := range results {
			payload, ok := msg.Values["payload"].(string)
			if !ok {
				continue
			}
			var entry WALEntry
			if json.Unmarshal([]byte(payload), &entry) != nil {
				continue
			}
			if _, ok := entrySet[entry.EntryID]; ok {
				toDelete = append(toDelete, msg.ID)
			}
		}

		if len(toDelete) > 0 {
			if err := w.client.XDel(ctx, streamKey, toDelete...).Err(); err == nil {
				completed += len(toDelete)
			}
		}
	}

	w.mu.Lock()
	if buf, ok := w.localBuffer[runID]; ok {
		entrySet := make(map[string]struct{}, len(entryIDs))
		for _, id := range entryIDs {
			entrySet[id] = struct{}{}
		}
		filtered := make([]WALEntry, 0, len(buf))
		for _, e := range buf {
			if _, ok := entrySet[e.EntryID]; !ok {
				filtered = append(filtered, e)
			}
		}
		completed += len(buf) - len(filtered)
		w.localBuffer[runID] = filtered
	}
	w.mu.Unlock()

	return completed, nil
}

// MarkFailed increments the attempt count and records the error.
func (w *WriteAheadLog) MarkFailed(ctx context.Context, runID string, entryID string, errStr string) error {
	streamKey := walStreamPrefix + runID

	results, err := w.client.XRange(ctx, streamKey, "-", "+").Result()
	if err == nil {
		for _, msg := range results {
			payload, ok := msg.Values["payload"].(string)
			if !ok {
				continue
			}
			var entry WALEntry
			if json.Unmarshal([]byte(payload), &entry) != nil {
				continue
			}
			if entry.EntryID != entryID {
				continue
			}

			entry.Attempts++
			entry.LastAttempt = float64(time.Now().UnixNano()) / 1e9
			entry.LastError = errStr

			newPayload, _ := json.Marshal(entry)
			w.client.XDel(ctx, streamKey, msg.ID)
			w.client.XAdd(ctx, &goredis.XAddArgs{
				Stream: streamKey,
				Values: map[string]any{"payload": string(newPayload)},
				MaxLen: walStreamMaxLen,
				Approx: true,
			})
			w.client.Expire(ctx, streamKey, walEntryTTL)
			return nil
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if buf, ok := w.localBuffer[runID]; ok {
		for i, e := range buf {
			if e.EntryID == entryID {
				e.Attempts++
				e.LastAttempt = float64(time.Now().UnixNano()) / 1e9
				e.LastError = errStr
				buf[i] = e
				return nil
			}
		}
	}

	return fmt.Errorf("WAL - MarkFailed - entry %s not found in run %s", entryID, runID)
}

// PendingRuns returns the run IDs with entries in the local buffer.
func (w *WriteAheadLog) PendingRuns() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	runIDs := make([]string, 0, len(w.localBuffer))
	for runID := range w.localBuffer {
		runIDs = append(runIDs, runID)
	}
	return runIDs
}

// CleanupRun removes all WAL entries for a run.
func (w *WriteAheadLog) CleanupRun(ctx context.Context, runID string) error {
	streamKey := walStreamPrefix + runID
	w.client.Del(ctx, streamKey)

	w.mu.Lock()
	delete(w.localBuffer, runID)
	w.mu.Unlock()
	return nil
}

// AppendMessage implements repo.WALAppender by encoding the record to JSON and appending to the WAL.
func (w *WriteAheadLog) AppendMessage(ctx context.Context, runID string, record entity.MessageRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("WriteAheadLog - AppendMessage - marshal: %w", err)
	}
	_, err = w.Append(ctx, runID, WriteTypeMessage, string(data))
	if err != nil {
		return fmt.Errorf("WriteAheadLog - AppendMessage: %w", err)
	}
	return nil
}

// AppendToolResult implements repo.WALAppender by encoding the record to JSON and appending to the WAL.
func (w *WriteAheadLog) AppendToolResult(ctx context.Context, runID string, record entity.ToolResultRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("WriteAheadLog - AppendToolResult - marshal: %w", err)
	}
	_, err = w.Append(ctx, runID, WriteTypeToolResult, string(data))
	if err != nil {
		return fmt.Errorf("WriteAheadLog - AppendToolResult: %w", err)
	}
	return nil
}
