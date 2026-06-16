package eventing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

const duplicateEventSequence int64 = 0

var (
	ErrInvalidEvent = errors.New("agentos event ingest: invalid event")
)

// EventStore appends normalized AgentOS events to the shared event log.
type EventStore interface {
	Append(ctx context.Context, sessionID, runID string, event entity.StreamEvent) (int64, error)
}

// DedupeStore claims backend-provided event IDs before append.
type DedupeStore interface {
	ClaimEvent(ctx context.Context, runID, eventID string) (bool, error)
}

// Service ingests external backend events into the normalized AgentOS stream.
type Service struct {
	eventStore EventStore
	dedupe     DedupeStore
	now        func() time.Time
}

// NewService creates an event ingest service.
func NewService(eventStore EventStore, dedupe DedupeStore) (*Service, error) {
	if eventStore == nil {
		return nil, errors.New("agentos event ingest: nil event store")
	}

	return &Service{
		eventStore: eventStore,
		dedupe:     dedupe,
		now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

// Ingest validates, normalizes, deduplicates, and persists one external event.
func (s *Service) Ingest(ctx context.Context, input IngestEvent) (IngestResult, error) {
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

	seq, err := s.eventStore.Append(ctx, sessionID, event.RunID, event)
	if err != nil {
		return IngestResult{}, fmt.Errorf("agentos event ingest: append: %w", err)
	}

	return IngestResult{
		RunID:    event.RunID,
		EventID:  event.EventID,
		Sequence: seq,
	}, nil
}

func (s *Service) normalize(input IngestEvent) (entity.AgentOSEvent, string, error) {
	if input.RunID == "" {
		return entity.AgentOSEvent{}, "", fmt.Errorf("%w: run_id is required", ErrInvalidEvent)
	}
	if input.EventID == "" {
		return entity.AgentOSEvent{}, "", fmt.Errorf("%w: event_id is required", ErrInvalidEvent)
	}
	if !entity.IsAgentOSStandardEventType(string(input.EventType)) {
		return entity.AgentOSEvent{}, "", fmt.Errorf("%w: unsupported event_type %q", ErrInvalidEvent, input.EventType)
	}
	if input.Source == "" {
		return entity.AgentOSEvent{}, "", fmt.Errorf("%w: source is required", ErrInvalidEvent)
	}

	timestamp := input.Timestamp
	if timestamp.IsZero() {
		timestamp = s.now()
	}
	sessionID := input.ThreadID
	if sessionID == "" {
		sessionID = input.RunID
	}

	return entity.AgentOSEvent{
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
