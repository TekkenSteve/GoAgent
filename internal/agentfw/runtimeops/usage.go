package runtimeops

import (
	"context"
	"time"
)

// UsageRecord is auditable model/tool usage payload.
type UsageRecord struct {
	RunID            string
	ThreadRunID      string
	IdempotencyKey   string
	ModelID          string
	ToolName         string
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
}

// BillingSink delivers usage records to billing backend.
type BillingSink interface {
	Deliver(ctx context.Context, record *UsageRecord) error
}

// UsageEmitter retries billing delivery with bounded backoff.
type UsageEmitter struct {
	Sink        BillingSink
	MaxAttempts int
	Backoff     time.Duration
}

// Emit delivers usage record with at-least-once retries.
func (e UsageEmitter) Emit(ctx context.Context, record *UsageRecord) error {
	attempts := e.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}

	for i := 0; i < attempts; i++ {
		err := e.Sink.Deliver(ctx, record)
		if err == nil {
			return nil
		}

		if i == attempts-1 {
			return err
		}

		if e.Backoff > 0 {
			select {
			case <-time.After(e.Backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}
