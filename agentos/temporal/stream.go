package temporal

import (
	"encoding/json"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwstream "github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type subscription struct {
	events <-chan agentos.Event
	close  func() error
}

func (s *subscription) Events() <-chan agentos.Event {
	return s.events
}

func (s *subscription) Close() error {
	if s.close == nil {
		return nil
	}

	return s.close()
}

func newSubscription(internalSub *agentfwstream.Subscription) agentos.Subscription {
	out := make(chan agentos.Event)
	go func() {
		defer close(out)
		for stored := range internalSub.C {
			out <- eventFromStored(stored)
		}
	}()

	return &subscription{
		events: out,
		close: func() error {
			internalSub.Close()

			return nil
		},
	}
}

func eventFromStored(stored agentfwstream.StoredEvent) agentos.Event {
	base := stored.Event.Base()

	return agentos.Event{
		EventID:   base.EventID,
		EventType: stored.Event.EventType(),
		RunID:     base.RunID,
		ThreadID:  base.SessionID,
		Sequence:  stored.Sequence,
		Timestamp: base.Timestamp,
		Source:    string(base.Source),
		Payload:   payloadFromStreamEvent(stored.Event),
	}
}

func payloadFromStreamEvent(event entity.StreamEvent) map[string]any {
	data, err := entity.MarshalEvent(event)
	if err != nil {
		return map[string]any{}
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return map[string]any{}
	}

	return payload
}
