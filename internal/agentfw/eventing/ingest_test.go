package eventing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func TestServiceIngestAppendsNormalizedEvent(t *testing.T) {
	store := &fakeEventStore{sequence: 12}
	dedupe := &fakeDedupeStore{claimed: true}
	service := newTestService(t, store, dedupe)
	service.now = func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) }

	result, err := service.Ingest(context.Background(), IngestEvent{
		EventID:   "evt-1",
		RunID:     "run-1",
		ThreadID:  "thread-1",
		EventType: agentos.EventAgentMessageDelta,
		Source:    "langgraph",
		Payload: map[string]any{
			"text": "hello",
		},
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if result.Sequence != 12 || result.Duplicate {
		t.Fatalf("unexpected result: %#v", result)
	}
	if store.sessionID != "thread-1" || store.runID != "run-1" {
		t.Fatalf("unexpected store route: session=%q run=%q", store.sessionID, store.runID)
	}

	event, ok := store.event.(entity.AgentOSEvent)
	if !ok {
		t.Fatalf("event type = %T", store.event)
	}
	if event.EventType() != "agent.message.delta" ||
		event.BaseEvent.EventID != "evt-1" ||
		event.BaseEvent.Source != "langgraph" ||
		event.Payload["text"] != "hello" {
		t.Fatalf("unexpected normalized event: %#v", event)
	}
	if dedupe.runID != "run-1" || dedupe.eventID != "evt-1" {
		t.Fatalf("unexpected dedupe key: %#v", dedupe)
	}
}

func TestServiceIngestIgnoresDuplicateEvent(t *testing.T) {
	store := &fakeEventStore{sequence: 12}
	service := newTestService(t, store, &fakeDedupeStore{claimed: false})

	result, err := service.Ingest(context.Background(), IngestEvent{
		EventID:   "evt-1",
		RunID:     "run-1",
		EventType: agentos.EventRunStarted,
		Source:    "python-agent",
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if !result.Duplicate || result.Sequence != duplicateEventSequence {
		t.Fatalf("unexpected duplicate result: %#v", result)
	}
	if store.event != nil {
		t.Fatalf("duplicate event was appended: %#v", store.event)
	}
}

func TestServiceIngestRejectsUnknownEventType(t *testing.T) {
	service := newTestService(t, &fakeEventStore{}, nil)

	_, err := service.Ingest(context.Background(), IngestEvent{
		EventID:   "evt-1",
		RunID:     "run-1",
		EventType: "backend.random",
		Source:    "external",
	})
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("err = %v", err)
	}
}

func newTestService(t *testing.T, store *fakeEventStore, dedupe DedupeStore) *Service {
	t.Helper()

	service, err := NewService(store, dedupe)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	return service
}

type fakeEventStore struct {
	sessionID string
	runID     string
	event     entity.StreamEvent
	sequence  int64
}

func (s *fakeEventStore) Append(_ context.Context, sessionID, runID string, event entity.StreamEvent) (int64, error) {
	s.sessionID = sessionID
	s.runID = runID
	s.event = event

	return s.sequence, nil
}

type fakeDedupeStore struct {
	runID   string
	eventID string
	claimed bool
}

func (s *fakeDedupeStore) ClaimEvent(_ context.Context, runID, eventID string) (bool, error) {
	s.runID = runID
	s.eventID = eventID

	return s.claimed, nil
}
