// Package eventing ingests external backend events into the normalized
// AgentOS event stream.
package eventing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
)

const (
	duplicateEventSequence int64 = 0

	// agentOSEventCustomName is the namespaced CUSTOM name carrying a normalized
	// external AgentOS event on the run timeline. The control plane owns the
	// agentos.* namespace; these events ride the bus so run-channel subscribers
	// see them, and their authoritative copy remains the control plane's store.
	agentOSEventCustomName = "agentos.event"
)

var (
	// ErrInvalidEvent is returned when an ingested event is missing a
	// required field or has an unsupported event type.
	ErrInvalidEvent       = errors.New("agentos event ingest: invalid event")
	errIngestNilPublisher = errors.New("agentos event ingest: nil event publisher")
)

// Publisher publishes normalized AgentOS events onto a run's data-plane
// channel. agentos/stream.Publisher satisfies it.
type Publisher interface {
	Publish(ctx context.Context, handle *stream.Handle, ev *stream.Event) error
}

// DedupeStore claims backend-provided event IDs before publish.
type DedupeStore interface {
	ClaimEvent(ctx context.Context, runID, eventID string) (bool, error)
}

// Service ingests external backend events into the normalized AgentOS stream.
type Service struct {
	publisher Publisher
	dedupe    DedupeStore
	now       func() time.Time
}

// NewService creates an event ingest service.
func NewService(publisher Publisher, dedupe DedupeStore) (*Service, error) {
	if publisher == nil {
		return nil, errIngestNilPublisher
	}

	return &Service{
		publisher: publisher,
		dedupe:    dedupe,
		now:       func() time.Time { return time.Now().UTC() },
	}, nil
}

// Ingest validates, normalizes, deduplicates, and publishes one external event
// to the run's live channel. The durable copy of these events is owned by the
// control plane's store; the bus carries only the live tail.
func (s *Service) Ingest(ctx context.Context, input *IngestEvent) (IngestResult, error) {
	event, sessionID, err := s.normalize(input)
	if err != nil {
		return IngestResult{}, err
	}

	if s.dedupe != nil && event.EventID != "" {
		claimed, err := s.dedupe.ClaimEvent(ctx, event.RunID, event.EventID)
		if err != nil {
			return IngestResult{}, err
		}

		if !claimed {
			return IngestResult{
				RunID:     event.RunID,
				EventID:   event.EventID,
				Duplicate: true,
				Sequence:  duplicateEventSequence,
			}, nil
		}
	}

	wire := stream.NewCustom(sessionID, event.RunID, agentOSEventCustomName)
	wire.Set("event", event)

	if err := s.publisher.Publish(ctx, streamadapter.HandleForRun(sessionID, event.RunID), wire); err != nil {
		return IngestResult{}, fmt.Errorf("agentos event ingest: publish: %w", err)
	}

	return IngestResult{
		RunID:    event.RunID,
		EventID:  event.EventID,
		Sequence: duplicateEventSequence,
	}, nil
}

func (s *Service) normalize(input *IngestEvent) (*entity.AgentOSEvent, string, error) {
	if input.RunID == "" {
		return nil, "", fmt.Errorf("%w: run_id is required", ErrInvalidEvent)
	}

	if input.EventID == "" {
		return nil, "", fmt.Errorf("%w: event_id is required", ErrInvalidEvent)
	}

	if !entity.IsAgentOSStandardEventType(string(input.EventType)) {
		return nil, "", fmt.Errorf("%w: unsupported event_type %q", ErrInvalidEvent, input.EventType)
	}

	if input.Source == "" {
		return nil, "", fmt.Errorf("%w: source is required", ErrInvalidEvent)
	}

	timestamp := input.Timestamp
	if timestamp.IsZero() {
		timestamp = s.now()
	}

	sessionID := input.ThreadID
	if sessionID == "" {
		sessionID = input.RunID
	}

	return &entity.AgentOSEvent{
		BaseEvent: entity.BaseEvent{
			EventType: string(input.EventType),
			Source:    entity.EventSource(input.Source),
			SessionID: sessionID,
			RunID:     input.RunID,
			EventID:   input.EventID,
			Timestamp: timestamp,
			TraceID:   input.TraceID,
			Tags:      input.Tags,
		},
		Payload: input.Payload,
	}, sessionID, nil
}
