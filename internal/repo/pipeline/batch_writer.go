package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var ErrUnknownWriteType = errors.New("unknown write type")

const (
	defaultFlushInterval            = 5 * time.Second
	defaultBatchSize                = 50
	defaultMaxRetries               = 3
	defaultBatchBaseDelay           = 100 * time.Millisecond
	batchMaxDelayMultiplier         = 5
	nanosPerSecond          float64 = 1e9
)

// PostgresWriter is the interface for writing to Postgres (implemented by MessageRepo).
type PostgresWriter interface {
	PersistMessage(ctx context.Context, record entity.MessageRecord) (string, error)
	PersistToolResult(ctx context.Context, record entity.ToolResultRecord) (string, error)
}

// BatchWriter asynchronously flushes WAL entries to Postgres.
// Periodic flush from WAL to Postgres, with retry and DLQ escalation for failed entries.
type BatchWriter struct {
	wal           *WriteAheadLog
	dlq           *DeadLetterQueue
	pg            PostgresWriter
	log           *logger.Logger
	retryPolicy   RetryPolicy
	flushInterval time.Duration
	batchSize     int

	notify chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// BatchWriterOption configures the BatchWriter.
type BatchWriterOption func(*BatchWriter)

func WithFlushInterval(d time.Duration) BatchWriterOption {
	return func(b *BatchWriter) {
		b.flushInterval = d
	}
}

func WithBatchSize(n int) BatchWriterOption {
	return func(b *BatchWriter) {
		b.batchSize = n
	}
}

// NewBatchWriter creates a BatchWriter.
func NewBatchWriter(
	wal *WriteAheadLog,
	dlq *DeadLetterQueue,
	pg PostgresWriter,
	log *logger.Logger,
	opts ...BatchWriterOption,
) *BatchWriter {
	ctx, cancel := context.WithCancel(context.Background())

	b := &BatchWriter{
		wal:           wal,
		dlq:           dlq,
		pg:            pg,
		log:           log,
		flushInterval: defaultFlushInterval,
		batchSize:     defaultBatchSize,
		notify:        make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		retryPolicy: &ExponentialBackoff{
			BaseDelay: defaultBatchBaseDelay,
			MaxDelay:  defaultBatchBaseDelay * batchMaxDelayMultiplier,
			MaxRetry:  defaultMaxRetries,
		},
	}

	for _, opt := range opts {
		opt(b)
	}

	return b
}

// Start begins the background flush loop.
func (b *BatchWriter) Start() {
	b.wg.Add(1)

	go b.loop()

	b.log.Info("[BatchWriter] started (interval: %s, batch: %d)", b.flushInterval, b.batchSize)
}

// Stop gracefully stops the background flush loop.
func (b *BatchWriter) Stop() {
	b.cancel()
	b.wg.Wait()
	b.log.Info("[BatchWriter] stopped")
}

// FlushNow triggers an immediate flush (non-blocking).
func (b *BatchWriter) FlushNow() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *BatchWriter) loop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.flushAllRuns()
		case <-b.notify:
			b.flushAllRuns()
		}
	}
}

func (b *BatchWriter) flushAllRuns() {
	runIDs := b.wal.PendingRuns()

	for _, runID := range runIDs {
		if b.ctx.Err() != nil {
			return
		}

		if err := b.flushRun(runID); err != nil {
			b.log.Warn("[BatchWriter] flush run %s: %v", runID, err)
		}
	}
}

func (b *BatchWriter) flushRun(runID string) error {
	entries, err := b.wal.GetPending(b.ctx, runID)
	if err != nil {
		return fmt.Errorf("wal.GetPending: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	var completedIDs []string

	for _, entry := range entries {
		if b.ctx.Err() != nil {
			return b.ctx.Err()
		}

		id, err := b.flushEntry(&entry)
		if err == nil {
			completedIDs = append(completedIDs, id)
		} else {
			b.handleFailure(&entry, err.Error())
		}
	}

	if len(completedIDs) > 0 {
		if _, err := b.wal.MarkCompleted(b.ctx, runID, completedIDs); err != nil {
			b.log.Warn("[BatchWriter] MarkCompleted: %v", err)
		}
	}

	return nil
}

func (b *BatchWriter) flushMessage(entry *WALEntry) (string, error) {
	var record entity.MessageRecord
	if err := json.Unmarshal([]byte(entry.Data), &record); err != nil {
		return entry.EntryID, fmt.Errorf("flushMessage - unmarshal: %w", err)
	}

	return entry.EntryID, b.flushWithRetry(func(ctx context.Context) error {
		_, err := b.pg.PersistMessage(ctx, record)

		return err
	}, "flushMessage")
}

func (b *BatchWriter) flushToolResult(entry *WALEntry) (string, error) {
	var record entity.ToolResultRecord
	if err := json.Unmarshal([]byte(entry.Data), &record); err != nil {
		return entry.EntryID, fmt.Errorf("flushToolResult - unmarshal: %w", err)
	}

	return entry.EntryID, b.flushWithRetry(func(ctx context.Context) error {
		_, err := b.pg.PersistToolResult(ctx, record)

		return err
	}, "flushToolResult")
}

func (b *BatchWriter) flushWithRetry(fn func(ctx context.Context) error, name string) error {
	err := RetryFn(b.ctx, fn, b.retryPolicy)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	return nil
}

func (b *BatchWriter) flushEntry(entry *WALEntry) (string, error) {
	switch entry.WriteType {
	case WriteTypeMessage:
		return b.flushMessage(entry)
	case WriteTypeToolResult:
		return b.flushToolResult(entry)
	default:
		return entry.EntryID, fmt.Errorf("%w: %s", ErrUnknownWriteType, entry.WriteType)
	}
}

func (b *BatchWriter) handleFailure(entry *WALEntry, errStr string) {
	entry.Attempts++
	entry.LastAttempt = float64(time.Now().UnixNano()) / nanosPerSecond
	entry.LastError = errStr

	if entry.Attempts >= defaultMaxRetries {
		b.sendToDLQ(entry, errStr)
	} else {
		if err := b.wal.MarkFailed(b.ctx, entry.RunID, entry.EntryID, errStr); err != nil {
			b.log.Warn("[BatchWriter] MarkFailed: %v", err)
		}
	}
}

func (b *BatchWriter) sendToDLQ(entry *WALEntry, errStr string) {
	b.log.Warn("[BatchWriter] sending to DLQ: run=%s entry=%s attempts=%d err=%s",
		entry.RunID, entry.EntryID, entry.Attempts, errStr)

	if err := b.dlq.Send(b.ctx, &DLQEntry{
		EntryID:   entry.EntryID,
		RunID:     entry.RunID,
		WriteType: string(entry.WriteType),
		Data:      entry.Data,
		Error:     errStr,
		Attempts:  entry.Attempts,
		CreatedAt: entry.CreatedAt,
	}); err != nil {
		b.log.Error("[BatchWriter] dlq.Send failed: %v", err)
	}

	if _, err := b.wal.MarkCompleted(b.ctx, entry.RunID, []string{entry.EntryID}); err != nil {
		b.log.Warn("[BatchWriter] MarkCompleted handleFailure: %v", err)
	}
}
