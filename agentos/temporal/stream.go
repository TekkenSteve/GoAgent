package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwstream "github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
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

func newPlanReplaySubscription(planEvents []agentos.PlanEvent) agentos.Subscription {
	out := make(chan agentos.Event, len(planEvents))
	for _, planEvent := range planEvents {
		out <- eventFromPlanEvent(planEvent)
	}
	close(out)

	return &subscription{events: out}
}

func newPlanReplayThenLiveSubscription(planEvents []agentos.PlanEvent, live agentos.Subscription) agentos.Subscription {
	if live == nil {
		return newPlanReplaySubscription(planEvents)
	}

	out := make(chan agentos.Event, len(planEvents))
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(out)
		for _, planEvent := range planEvents {
			select {
			case <-done:
				return
			case out <- eventFromPlanEvent(planEvent):
			}
		}
		for {
			select {
			case <-done:
				return
			case event, ok := <-live.Events():
				if !ok {
					return
				}
				select {
				case <-done:
					return
				case out <- event:
				}
			}
		}
	}()

	return &subscription{
		events: out,
		close: func() error {
			var err error
			once.Do(func() {
				close(done)
				err = live.Close()
			})

			return err
		},
	}
}

func eventFromPlanEvent(planEvent agentos.PlanEvent) agentos.Event {
	event := planEvent.Event
	payload := make(map[string]any, len(event.Payload)+2)
	for key, value := range event.Payload {
		payload[key] = value
	}
	payload["plan_id"] = planEvent.PlanID
	if planEvent.NodeID != "" {
		payload["node_id"] = planEvent.NodeID
	}
	event.Payload = payload

	return event
}

func lastPlanEventSequence(afterSequence int64, planEvents []agentos.PlanEvent) int64 {
	last := afterSequence
	for _, planEvent := range planEvents {
		if planEvent.Sequence > last {
			last = planEvent.Sequence
		}
	}

	return last
}

type agentOSSubscriber struct {
	subscriber *repostream.RedisSubscriber
}

func newAgentOSSubscriber(subscriber *repostream.RedisSubscriber) *agentOSSubscriber {
	if subscriber == nil {
		return nil
	}

	return &agentOSSubscriber{subscriber: subscriber}
}

func (s *agentOSSubscriber) SubscribeAgentOS(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	if s == nil || s.subscriber == nil {
		return nil, errors.New("agentos temporal subscriber: redis subscriber is not configured")
	}

	sessionID := scope.ThreadID
	if sessionID == "" {
		sessionID = scope.RunID
	}
	if sessionID == "" {
		return nil, fmt.Errorf("%w: run id or thread id is required", agentos.ErrInvalidStreamScope)
	}

	sub, err := s.subscriber.Subscribe(ctx, sessionID, scope.AfterSequence)
	if err != nil {
		return nil, err
	}

	return newSubscription(sub), nil
}

func eventFromStored(stored agentfwstream.StoredEvent) agentos.Event {
	base := stored.Event.Base()

	return agentos.Event{
		EventID:   base.EventID,
		EventType: agentos.EventType(stored.Event.EventType()),
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
