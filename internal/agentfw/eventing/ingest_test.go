package eventing

import (
	"context"
	"errors"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

const (
	Run1 = "run-1"
	Evt1 = "evt-1"
)

func TestServiceIngestPublishesNormalizedEvent(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	dedupe := &fakeDedupeStore{claimed: true}
	service := newTestService(t, publisher, dedupe)
	service.now = func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) }

	input := IngestEvent{
		EventID:   "evt-1",
		RunID:     "run-1",
		ThreadID:  "thread-1",
		EventType: agentoscore.EventAgentMessageDelta,
		Source:    "langgraph",
		Payload: map[string]any{
			"text": "hello",
		},
	}

	result, err := service.Ingest(context.Background(), &input)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	assertIngestResult(t, &result)
	assertPublishRoute(t, publisher)
	assertNormalizedEvent(t, publisher)
	assertDedupeKey(t, dedupe)
}

func assertIngestResult(t *testing.T, result *IngestResult) {
	t.Helper()

	if result.Sequence != duplicateEventSequence || result.Duplicate {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func assertPublishRoute(t *testing.T, publisher *fakePublisher) {
	t.Helper()

	want := "agentos:run:thread-1:run-1"
	if publisher.handle == nil || publisher.handle.Channel != want {
		t.Fatalf("publish route = %v, want %s", publisher.handle, want)
	}
}

func assertNormalizedEvent(t *testing.T, publisher *fakePublisher) {
	t.Helper()

	if publisher.event == nil {
		t.Fatal("no event published")
	}

	if publisher.event.Type != stream.EventCustom {
		t.Fatalf("event type = %q, want CUSTOM", publisher.event.Type)
	}

	if publisher.event.Payload[stream.FieldName] != agentOSEventCustomName {
		t.Fatalf("custom name = %v, want %s", publisher.event.Payload[stream.FieldName], agentOSEventCustomName)
	}

	event, ok := publisher.event.Payload["event"].(*entity.AgentOSEvent)
	if !ok {
		t.Fatalf("payload event type = %T", publisher.event.Payload["event"])
	}

	if event.EventType() != "agent.message.delta" ||
		event.EventID != Evt1 ||
		event.Source != "langgraph" ||
		event.Payload["text"] != "hello" {
		t.Fatalf("unexpected normalized event: %#v", event)
	}
}

func assertDedupeKey(t *testing.T, dedupe *fakeDedupeStore) {
	t.Helper()

	if dedupe.runID != "run-1" || dedupe.eventID != "evt-1" {
		t.Fatalf("unexpected dedupe key: %#v", dedupe)
	}
}

func TestServiceIngestIgnoresDuplicateEvent(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	service := newTestService(t, publisher, &fakeDedupeStore{claimed: false})

	input := IngestEvent{
		EventID:   "evt-1",
		RunID:     "run-1",
		EventType: agentoscore.EventRunStarted,
		Source:    "python-agent",
	}

	result, err := service.Ingest(context.Background(), &input)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if !result.Duplicate || result.Sequence != duplicateEventSequence {
		t.Fatalf("unexpected duplicate result: %#v", result)
	}

	if publisher.event != nil {
		t.Fatalf("duplicate event was published: %#v", publisher.event)
	}
}

func TestServiceIngestRejectsUnknownEventType(t *testing.T) {
	t.Parallel()
	service := newTestService(t, &fakePublisher{}, nil)

	input := IngestEvent{
		EventID:   "evt-1",
		RunID:     "run-1",
		EventType: "backend.random",
		Source:    "external",
	}

	_, err := service.Ingest(context.Background(), &input)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("err = %v", err)
	}
}

func newTestService(t *testing.T, publisher *fakePublisher, dedupe DedupeStore) *Service {
	t.Helper()

	service, err := NewService(publisher, dedupe)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	return service
}

type fakePublisher struct {
	handle *stream.Handle
	event  *stream.Event
}

func (p *fakePublisher) Publish(_ context.Context, handle *stream.Handle, ev *stream.Event) error {
	p.handle = handle
	p.event = ev

	return nil
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
