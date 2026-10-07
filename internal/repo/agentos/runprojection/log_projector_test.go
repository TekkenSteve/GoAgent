package runprojection

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
)

// Test-side static errors: err113 rejects dynamic errors even in tests, and a
// sentinel is what a caller would match on anyway.
var (
	errStoreDown  = errors.New("store down")
	errTestFailed = errors.New("test setup failed")
)

// fakeStore is the in-memory durable sink the projector writes to.
type fakeStore struct {
	mu      sync.Mutex
	rows    []*agentoscore.Event
	failOn  map[string]error
	appends int
}

func (s *fakeStore) AppendRunEvent(_ context.Context, ev *agentoscore.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.appends++

	if err, ok := s.failOn[ev.RunID]; ok {
		return err
	}

	s.rows = append(s.rows, ev)

	return nil
}

func (s *fakeStore) stored() []*agentoscore.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*agentoscore.Event(nil), s.rows...)
}

// fakeBatch is one delivered batch: a fixed record slice and an ack flag.
type fakeBatch struct {
	records []eventlog.ConsumedRecord
	acked   bool
}

func (b *fakeBatch) Records() []eventlog.ConsumedRecord { return b.records }

func (b *fakeBatch) Ack(context.Context) error {
	b.acked = true

	return nil
}

// fakeReader hands out one prepared batch per poll, then blocks like an empty
// log would.
type fakeReader struct {
	mu     sync.Mutex
	batch  *fakeBatch
	blocks chan struct{}
}

func (r *fakeReader) Poll(context.Context) (eventlog.Batch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.batch != nil {
		batch := r.batch
		r.batch = nil

		return batch, nil
	}

	<-r.blocks

	return nil, context.Canceled
}

func (r *fakeReader) queue(records []eventlog.ConsumedRecord) *fakeBatch {
	r.mu.Lock()
	defer r.mu.Unlock()

	batch := &fakeBatch{records: records}
	r.batch = batch

	return batch
}

// runFactRecord wraps one durable run fact in the wire form the drainer
// publishes: the envelope's payload is the domain event verbatim, and the
// event's own (run_id, sequence) is the idempotency key.
func runFactRecord(t *testing.T, ev *agentoscore.Event, sequence uint64) eventlog.ConsumedRecord {
	t.Helper()

	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("%v: marshal fact: %v", errTestFailed, err)
	}

	envelope := eventlog.Envelope{
		EventID:   ev.RunID + ":" + itoa(ev.Sequence),
		EventType: string(ev.EventType),
		Entity:    eventlog.EntityRef{Kind: "agentos_run", ID: ev.RunID},
		Payload:   body,
	}

	value, err := envelope.Encode()
	if err != nil {
		t.Fatalf("%v: encode envelope: %v", errTestFailed, err)
	}

	return eventlog.ConsumedRecord{
		Domain:   eventlog.DomainRunTimeline,
		Sequence: sequence,
		Value:    value,
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}

	digits := make([]byte, 0, 20)

	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	return string(digits)
}

// milestone is one fact-shaped milestone the projector must land.
func milestone(runID string, sequence int64, eventType agentoscore.EventType) *agentoscore.Event {
	return &agentoscore.Event{
		RunID:     runID,
		Sequence:  sequence,
		EventType: eventType,
		ThreadID:  "thread-" + runID,
		Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// TestLogProjectorReplaysFactsToDurableTimeline is the writer↔log round trip:
// the facts a writer made durable, published to the log in the drainer's wire
// form, come back through the projection as the same rows — the consumer side
// of the ruling that facts flow Postgres → outbox → log → projection.
func TestLogProjectorReplaysFactsToDurableTimeline(t *testing.T) {
	t.Parallel()

	facts := []*agentoscore.Event{
		milestone("run-1", 1, agentoscore.EventRunStarted),
		milestone("run-1", 2, agentoscore.EventAgentMessageCompleted),
		milestone("run-1", 3, agentoscore.EventToolCallCompleted),
		milestone("run-1", 4, agentoscore.EventRunCompleted),
	}

	records := make([]eventlog.ConsumedRecord, 0, len(facts))
	for i, fact := range facts {
		records = append(records, runFactRecord(t, fact, uint64(i+1)))
	}

	reader := &fakeReader{blocks: make(chan struct{})}
	batch := reader.queue(records)

	store := &fakeStore{}

	projector, err := NewLogProjector(LogConfig{Reader: reader, Store: store})
	if err != nil {
		t.Fatalf("NewLogProjector: %v", err)
	}

	result, err := projector.ProjectBatch(t.Context())
	if err != nil {
		t.Fatalf("ProjectBatch: %v", err)
	}

	if !batch.acked {
		t.Fatal("batch was not acknowledged after every fact landed")
	}

	if result.Projected != len(facts) || result.Skipped != 0 {
		t.Fatalf("result = %+v, want %d projected / 0 skipped", result, len(facts))
	}

	requireRowsMatch(t, facts, store.stored())
}

// requireRowsMatch asserts the replayed rows are the facts the writer made
// durable, field for field.
func requireRowsMatch(t *testing.T, facts, stored []*agentoscore.Event) {
	t.Helper()

	if len(stored) != len(facts) {
		t.Fatalf("stored %d rows, want %d", len(stored), len(facts))
	}

	for i := range facts {
		want, err := json.Marshal(facts[i])
		if err != nil {
			t.Fatalf("%v: marshal want row %d: %v", errTestFailed, i, err)
		}

		got, err := json.Marshal(stored[i])
		if err != nil {
			t.Fatalf("%v: marshal stored row %d: %v", errTestFailed, i, err)
		}

		if !bytes.Equal(want, got) {
			t.Fatalf("row %d diverges:\nwant %s\ngot  %s", i, want, got)
		}
	}
}

// TestLogProjectorSkipsUndecodableRecords locks the strand-policy: a record
// this consumer cannot use is acknowledged, not parked — the log keeps what
// the projection did not use, and a poison record cannot wedge the timeline.
func TestLogProjectorSkipsUndecodableRecords(t *testing.T) {
	t.Parallel()

	good := milestone("run-2", 1, agentoscore.EventRunStarted)
	records := []eventlog.ConsumedRecord{
		{Domain: eventlog.DomainRunTimeline, Sequence: 1, Value: []byte("not an envelope")},
		runFactRecord(t, good, 2),
		{Domain: eventlog.DomainRunTimeline, Sequence: 3, Value: []byte(`{"event_id":"x","schema":1,"payload":"{}"}`)},
		runFactRecord(t, milestone("run-3", 0, agentoscore.EventRunCompleted), 4),
	}

	reader := &fakeReader{blocks: make(chan struct{})}
	batch := reader.queue(records)

	store := &fakeStore{}

	projector, err := NewLogProjector(LogConfig{Reader: reader, Store: store})
	if err != nil {
		t.Fatalf("NewLogProjector: %v", err)
	}

	result, err := projector.ProjectBatch(t.Context())
	if err != nil {
		t.Fatalf("ProjectBatch: %v", err)
	}

	if !batch.acked {
		t.Fatal("batch with undecodable records was not acknowledged")
	}

	if result.Skipped != 3 {
		t.Fatalf("skipped %d records, want 3 (bad envelope, bad payload, identityless)", result.Skipped)
	}

	if result.Projected != 1 || len(store.stored()) != 1 {
		t.Fatalf("projected %+v / stored %d, want the one decodable fact", result, len(store.stored()))
	}
}

// TestLogProjectorLeavesBatchUnackedOnPersistFailure locks the delivery
// contract: the batch is acknowledged only after the store took it, so a
// crash between land and ack redelivers — and the idempotent append makes the
// redelivery free.
func TestLogProjectorLeavesBatchUnackedOnPersistFailure(t *testing.T) {
	t.Parallel()

	persistErr := errStoreDown
	records := []eventlog.ConsumedRecord{
		runFactRecord(t, milestone("run-4", 1, agentoscore.EventRunStarted), 1),
		runFactRecord(t, milestone("run-5", 1, agentoscore.EventRunStarted), 2),
	}

	reader := &fakeReader{blocks: make(chan struct{})}
	batch := reader.queue(records)

	store := &fakeStore{failOn: map[string]error{"run-5": persistErr}}

	projector, err := NewLogProjector(LogConfig{Reader: reader, Store: store})
	if err != nil {
		t.Fatalf("NewLogProjector: %v", err)
	}

	if _, err := projector.ProjectBatch(t.Context()); !errors.Is(err, persistErr) {
		t.Fatalf("ProjectBatch error = %v, want the persist failure", err)
	}

	if batch.acked {
		t.Fatal("batch was acknowledged despite a persist failure")
	}

	// The fact that landed before the failure is stored; the failed one is
	// redelivered with the batch on retry.
	if len(store.stored()) != 1 {
		t.Fatalf("stored %d rows before the failure, want 1", len(store.stored()))
	}
}

func TestNewLogProjectorRequiresReaderAndStore(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{blocks: make(chan struct{})}

	if _, err := NewLogProjector(LogConfig{Store: &fakeStore{}}); !errors.Is(err, ErrLogReaderRequired) {
		t.Fatalf("error = %v, want ErrLogReaderRequired", err)
	}

	if _, err := NewLogProjector(LogConfig{Reader: reader}); !errors.Is(err, ErrLogStoreRequired) {
		t.Fatalf("error = %v, want ErrLogStoreRequired", err)
	}
}
