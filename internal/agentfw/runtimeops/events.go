package runtimeops

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EventType identifies stream event categories.
type EventType string

const (
	EventTypeStatus  EventType = "status"
	EventTypeChunk   EventType = "chunk"
	EventTypeTool    EventType = "tool"
	EventTypeError   EventType = "error"
	EventTypeMetrics EventType = "metrics"
	EventTypeAudit   EventType = "audit"
)

// StreamEvent is the runtime event contract.
type StreamEvent struct {
	EventSchemaVersion string
	RunID              string
	Sequence           int64
	Timestamp          time.Time
	Type               EventType
	CorrelationID      string
	Payload            map[string]any
}

// EventPublisher publishes runtime events.
type EventPublisher interface {
	Publish(ctx context.Context, event StreamEvent) error
}

// Sequencer assigns monotonic sequence numbers per run.
type Sequencer struct {
	mu  sync.Mutex
	seq map[string]int64
}

// NewSequencer creates a per-run sequence generator.
func NewSequencer() *Sequencer {
	return &Sequencer{seq: map[string]int64{}}
}

// Next returns the next sequence value for a run id.
func (s *Sequencer) Next(runID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq[runID]++
	return s.seq[runID]
}

// FailOpenPublisher swallows publish failures and records them through callback.
type FailOpenPublisher struct {
	Inner          EventPublisher
	OnPublishError func(error)
}

// Publish attempts publish and suppresses transient channel failures.
func (p FailOpenPublisher) Publish(ctx context.Context, event StreamEvent) error {
	if p.Inner == nil {
		return nil
	}
	if err := p.Inner.Publish(ctx, event); err != nil {
		if p.OnPublishError != nil {
			p.OnPublishError(err)
		}
		return nil
	}
	return nil
}

// IsNMinusOneCompatible checks if consumer schema can read producer event schema
// with N-1 backward compatibility.
func IsNMinusOneCompatible(producerSchema, consumerSchema string) (bool, error) {
	producerN, err := parseSchemaVersion(producerSchema)
	if err != nil {
		return false, err
	}
	consumerN, err := parseSchemaVersion(consumerSchema)
	if err != nil {
		return false, err
	}

	// Consumer can read same version or one version behind producer.
	return producerN == consumerN || producerN == consumerN+1, nil
}

func parseSchemaVersion(v string) (int, error) {
	raw := strings.TrimPrefix(strings.ToLower(v), "v")
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid schema version %q", v)
	}
	return n, nil
}
