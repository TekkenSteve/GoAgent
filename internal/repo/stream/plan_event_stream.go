package stream

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	goredis "github.com/redis/go-redis/v9"
)

const planEventSubscriberBufferSize = 256

type planEventRedisClient interface {
	StreamAddWithID(ctx context.Context, stream, id string, values map[string]any, maxLen int) (string, error)
	StreamRange(ctx context.Context, stream, start, end string, count int) ([]goredis.XMessage, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
}

// RedisPlanEventStream publishes and subscribes public AgentOS PlanEvents in a
// Redis live stream. Durable replay remains owned by Postgres PlanEventStore.
type RedisPlanEventStream struct {
	rdb planEventRedisClient
	hub *redis.StreamHub
}

// NewRedisPlanEventStream creates a Redis-backed live PlanEvent stream.
func NewRedisPlanEventStream(rdb *redis.Redis) *RedisPlanEventStream {
	if rdb == nil {
		return nil
	}

	return newRedisPlanEventStream(rdb, rdb.Hub())
}

func newRedisPlanEventStream(rdb planEventRedisClient, hub *redis.StreamHub) *RedisPlanEventStream {
	return &RedisPlanEventStream{rdb: rdb, hub: hub}
}

// PublishPlanEvent writes a sequenced PlanEvent to the live Redis stream.
func (s *RedisPlanEventStream) PublishPlanEvent(ctx context.Context, event agentos.PlanEvent) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: redis plan event stream is not configured", agentos.ErrInvalidPlanEvent)
	}
	if event.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidPlanEvent)
	}
	if event.Sequence <= 0 {
		return fmt.Errorf("%w: plan event sequence is required", agentos.ErrInvalidPlanEvent)
	}
	data, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		return err
	}

	streamKey := planEventStreamKey(event.PlanID)
	entryID := planEventEntryID(event.Sequence)
	existing, exists, err := s.planEventAt(ctx, streamKey, entryID)
	if err != nil {
		return err
	}
	if exists {
		return ensureSamePlanEvent(existing, event)
	}

	_, err = s.rdb.StreamAddWithID(ctx, streamKey, entryID, map[string]any{
		"data":       string(data),
		"event_type": string(event.EventType),
		"sequence":   event.Sequence,
	}, DefaultEventStoreMaxLen)
	if err != nil {
		existing, exists, lookupErr := s.planEventAt(ctx, streamKey, entryID)
		if lookupErr != nil {
			return lookupErr
		}
		if exists {
			return ensureSamePlanEvent(existing, event)
		}

		return fmt.Errorf("plan_event_stream: publish: %w", err)
	}
	if _, err := s.rdb.Expire(ctx, streamKey, DefaultEventStoreTTL); err != nil {
		return fmt.Errorf("plan_event_stream: expire: %w", err)
	}

	return nil
}

// SubscribePlanEvents subscribes to live PlanEvents after scope.AfterSequence.
func (s *RedisPlanEventStream) SubscribePlanEvents(_ context.Context, scope agentos.PlanStreamScope) (agentos.Subscription, error) {
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return nil, err
	}
	if s == nil || s.hub == nil {
		return nil, fmt.Errorf("%w: redis plan event subscriber is not configured", agentos.ErrInvalidStreamScope)
	}

	hubSub := s.hub.Subscribe(planEventStreamKey(scope.PlanID), planEventEntryID(scope.AfterSequence))
	out := make(chan agentos.Event, planEventSubscriberBufferSize)
	go func() {
		defer close(out)
		for entry := range hubSub.C {
			planEvent, err := planEventFromStreamEntry(entry)
			if err != nil || !planEventMatchesScope(planEvent, scope) {
				continue
			}

			out <- eventFromPlanEvent(planEvent)
		}
	}()

	return &planLiveSubscription{
		events: out,
		close: func() error {
			hubSub.Close()

			return nil
		},
	}, nil
}

func (s *RedisPlanEventStream) planEventAt(ctx context.Context, streamKey string, entryID string) (agentos.PlanEvent, bool, error) {
	entries, err := s.rdb.StreamRange(ctx, streamKey, entryID, entryID, 1)
	if err != nil {
		return agentos.PlanEvent{}, false, fmt.Errorf("plan_event_stream: lookup: %w", err)
	}
	if len(entries) == 0 {
		return agentos.PlanEvent{}, false, nil
	}
	event, err := planEventFromValues(entries[0].Values)
	if err != nil {
		return agentos.PlanEvent{}, false, err
	}

	return event, true, nil
}

type planLiveSubscription struct {
	events <-chan agentos.Event
	close  func() error
}

func (s *planLiveSubscription) Events() <-chan agentos.Event {
	return s.events
}

func (s *planLiveSubscription) Close() error {
	if s.close == nil {
		return nil
	}

	return s.close()
}

func planEventFromStreamEntry(entry redis.XStreamEntry) (agentos.PlanEvent, error) {
	return planEventFromStringValues(entry.Values)
}

func planEventFromValues(values map[string]any) (agentos.PlanEvent, error) {
	data, ok := values["data"]
	if !ok {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event stream entry has no data", agentos.ErrInvalidPlanEvent)
	}

	return decodePlanEventStreamValue(data)
}

func planEventFromStringValues(values map[string]string) (agentos.PlanEvent, error) {
	data, ok := values["data"]
	if !ok {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event stream entry has no data", agentos.ErrInvalidPlanEvent)
	}

	return decodePlanEventStreamValue(data)
}

func decodePlanEventStreamValue(value any) (agentos.PlanEvent, error) {
	switch typed := value.(type) {
	case string:
		return agentos.UnmarshalPlanEvent([]byte(typed))
	case []byte:
		return agentos.UnmarshalPlanEvent(typed)
	default:
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event stream data has type %T", agentos.ErrInvalidPlanEvent, value)
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

func planEventMatchesScope(event agentos.PlanEvent, scope agentos.PlanStreamScope) bool {
	if event.PlanID != scope.PlanID {
		return false
	}
	if scope.NodeID != "" && event.NodeID != scope.NodeID {
		return false
	}
	if scope.RunID != "" && event.RunID != scope.RunID {
		return false
	}
	if event.Sequence <= scope.AfterSequence {
		return false
	}

	return true
}

func ensureSamePlanEvent(existing agentos.PlanEvent, expected agentos.PlanEvent) error {
	existingData, existingErr := agentos.MarshalPlanEvent(existing)
	expectedData, expectedErr := agentos.MarshalPlanEvent(expected)
	if existingErr == nil && expectedErr == nil && bytes.Equal(existingData, expectedData) {
		return nil
	}

	return fmt.Errorf("%w: plan event sequence %d already belongs to event %q", agentos.ErrInvalidPlanEvent, expected.Sequence, existing.EventID)
}

func planEventStreamKey(planID string) string {
	return "agentos:plan:events:" + planID
}

func planEventEntryID(sequence int64) string {
	if sequence <= 0 {
		return "0-0"
	}

	return strconv.FormatInt(sequence, 10) + "-0"
}
