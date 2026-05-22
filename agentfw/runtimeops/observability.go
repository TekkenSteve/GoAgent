package runtimeops

import (
	"context"
	"sync"
	"time"
)

type ctxKey string

const correlationIDKey ctxKey = "agentfw_correlation_id"

// WithCorrelationID adds run correlation id to context.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

// CorrelationID returns correlation id from context.
func CorrelationID(ctx context.Context) string {
	v, ok := ctx.Value(correlationIDKey).(string)
	if !ok {
		return ""
	}

	return v
}

// MetricsRecorder stores counters and durations for baseline observability.
type MetricsRecorder struct {
	mu        sync.Mutex
	counters  map[string]int64
	durations map[string][]time.Duration
}

// NewMetricsRecorder creates in-memory metrics sink.
func NewMetricsRecorder() *MetricsRecorder {
	return &MetricsRecorder{
		counters:  map[string]int64{},
		durations: map[string][]time.Duration{},
	}
}

// Inc increments a named counter.
func (m *MetricsRecorder) Inc(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.counters[name]++
}

// Observe records a latency sample.
func (m *MetricsRecorder) Observe(name string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.durations[name] = append(m.durations[name], d)
}

// Counter returns current counter value.
func (m *MetricsRecorder) Counter(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.counters[name]
}
