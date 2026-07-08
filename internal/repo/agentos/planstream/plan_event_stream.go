package planstream

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	goredis "github.com/redis/go-redis/v9"
)

const (
	planEventStreamMaxLen = 2000
	planEventStreamTTL    = 2 * time.Hour
	subscriberBufferSize  = 256
	livePlanEventStartID  = "$"
)

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
func (s *RedisPlanEventStream) PublishPlanEvent(ctx context.Context, event *agentos.PlanEvent) error {
	if err := validateRedisPlanEventStream(s, event); err != nil {
		return err
	}

	data, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		return err
	}

	streamKey := planEventStreamKey(planEventRef(event))
	entryID := planEventEntryID(event.Sequence)

	return s.publishPlanEventEntry(ctx, streamKey, entryID, event, data)
}

func validateRedisPlanEventStream(stream *RedisPlanEventStream, event *agentos.PlanEvent) error {
	if stream == nil || stream.rdb == nil {
		return fmt.Errorf("%w: redis plan event stream is not configured", agentoscore.ErrInvalidPlanEvent)
	}

	if event.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidPlanEvent)
	}

	if event.Sequence <= 0 {
		return fmt.Errorf("%w: plan event sequence is required", agentoscore.ErrInvalidPlanEvent)
	}

	return nil
}

func (s *RedisPlanEventStream) publishPlanEventEntry(ctx context.Context, streamKey, entryID string, event *agentos.PlanEvent, data []byte) error {
	existing, exists, err := s.planEventAt(ctx, streamKey, entryID)
	if err != nil {
		return err
	}

	if exists {
		return ensureSamePlanEvent(&existing, event)
	}

	_, err = s.rdb.StreamAddWithID(ctx, streamKey, entryID, map[string]any{
		"data":       string(data),
		"event_type": string(event.EventType),
		"sequence":   event.Sequence,
	}, planEventStreamMaxLen)
	if err != nil {
		existing, exists, lookupErr := s.planEventAt(ctx, streamKey, entryID)
		if lookupErr != nil {
			return lookupErr
		}

		if exists {
			return ensureSamePlanEvent(&existing, event)
		}

		return fmt.Errorf("plan_event_stream: publish: %w", err)
	}

	if _, err := s.rdb.Expire(ctx, streamKey, planEventStreamTTL); err != nil {
		return fmt.Errorf("plan_event_stream: expire: %w", err)
	}

	return nil
}

// SubscribePlanEvents subscribes to live PlanEvents. Durable replay and
// catch-up are owned by the Postgres PlanEventStore.
func (s *RedisPlanEventStream) SubscribePlanEvents(_ context.Context, scope *agentos.PlanStreamScope) (agentosplan.PlanEventSubscription, error) {
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return nil, err
	}

	if s == nil || s.hub == nil {
		return nil, fmt.Errorf("%w: redis plan event subscriber is not configured", agentoscore.ErrInvalidStreamScope)
	}

	hubSub := s.hub.Subscribe(planEventStreamKey(planStreamRef(scope)), planEventSubscriptionStartID(*scope))
	out := make(chan agentos.PlanEvent, subscriberBufferSize)

	go func() {
		defer close(out)

		for entry := range hubSub.C {
			planEvent, err := planEventFromStreamEntry(entry)
			if err != nil || !planEventMatchesScope(&planEvent, scope) {
				continue
			}

			out <- planEvent
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

func planEventSubscriptionStartID(agentos.PlanStreamScope) string {
	return livePlanEventStartID
}

func (s *RedisPlanEventStream) planEventAt(ctx context.Context, streamKey, entryID string) (agentos.PlanEvent, bool, error) {
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
	events <-chan agentos.PlanEvent
	close  func() error
}

func (s *planLiveSubscription) Events() <-chan agentos.PlanEvent {
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
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event stream entry has no data", agentoscore.ErrInvalidPlanEvent)
	}

	return decodePlanEventStreamValue(data)
}

func planEventFromStringValues(values map[string]string) (agentos.PlanEvent, error) {
	data, ok := values["data"]
	if !ok {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event stream entry has no data", agentoscore.ErrInvalidPlanEvent)
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
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event stream data has type %T", agentoscore.ErrInvalidPlanEvent, value)
	}
}

func planEventMatchesScope(event *agentos.PlanEvent, scope *agentos.PlanStreamScope) bool {
	if event.PlanID != scope.PlanID {
		return false
	}

	if event.AccountID != scope.AccountID {
		return false
	}

	if event.ProjectID != scope.ProjectID {
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

func ensureSamePlanEvent(existing, expected *agentos.PlanEvent) error {
	existingData, existingErr := agentos.MarshalPlanEvent(existing)

	expectedData, expectedErr := agentos.MarshalPlanEvent(expected)
	if existingErr == nil && expectedErr == nil && bytes.Equal(existingData, expectedData) {
		return nil
	}

	return fmt.Errorf("%w: plan event sequence %d already belongs to event %q", agentoscore.ErrInvalidPlanEvent, expected.Sequence, existing.EventID)
}

func planEventStreamKey(ref agentos.PlanRef) string {
	return "agentos:plan:events:" +
		url.PathEscape(ref.AccountID) + ":" +
		url.PathEscape(ref.ProjectID) + ":" +
		url.PathEscape(ref.PlanID)
}

func planEventRef(event *agentos.PlanEvent) agentos.PlanRef {
	return agentos.PlanRef{
		PlanID:    event.PlanID,
		AccountID: event.AccountID,
		ProjectID: event.ProjectID,
	}
}

func planStreamRef(scope *agentos.PlanStreamScope) agentos.PlanRef {
	return agentos.PlanRef{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}
}

func planEventEntryID(sequence int64) string {
	if sequence <= 0 {
		return "0-0"
	}

	return strconv.FormatInt(sequence, 10) + "-0"
}
