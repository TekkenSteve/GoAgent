package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	goredis "github.com/redis/go-redis/v9"
)

func TestRedisPlanEventStreamPublishesPlanEvent(t *testing.T) {
	fake := newFakePlanEventRedis()
	stream := newRedisPlanEventStream(fake, nil)
	event := testPlanEvent("evt-1", 7)

	if err := stream.PublishPlanEvent(context.Background(), event); err != nil {
		t.Fatalf("PublishPlanEvent: %v", err)
	}

	entry, ok := fake.entry(planEventStreamKey("plan-1"), "7-0")
	if !ok {
		t.Fatalf("missing redis entry: %#v", fake.entries)
	}
	stored, err := planEventFromValues(entry.Values)
	if err != nil {
		t.Fatalf("decode stored event: %v", err)
	}
	if stored.EventID != event.EventID || stored.PlanID != event.PlanID || stored.Sequence != event.Sequence {
		t.Fatalf("stored = %#v, want %#v", stored, event)
	}
	if fake.expiredKey != planEventStreamKey("plan-1") {
		t.Fatalf("expired key = %q", fake.expiredKey)
	}
}

func TestRedisPlanEventStreamPublishIsIdempotent(t *testing.T) {
	fake := newFakePlanEventRedis()
	stream := newRedisPlanEventStream(fake, nil)
	event := testPlanEvent("evt-1", 7)
	fake.store(planEventStreamKey("plan-1"), "7-0", event)

	if err := stream.PublishPlanEvent(context.Background(), event); err != nil {
		t.Fatalf("PublishPlanEvent duplicate: %v", err)
	}
	if fake.addCalls != 0 {
		t.Fatalf("duplicate publish called XADD %d times", fake.addCalls)
	}
}

func TestRedisPlanEventStreamRejectsSequenceCollision(t *testing.T) {
	fake := newFakePlanEventRedis()
	stream := newRedisPlanEventStream(fake, nil)
	fake.store(planEventStreamKey("plan-1"), "7-0", testPlanEvent("evt-existing", 7))

	err := stream.PublishPlanEvent(context.Background(), testPlanEvent("evt-new", 7))
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("PublishPlanEvent error = %v, want ErrInvalidPlanEvent", err)
	}
}

func testPlanEvent(eventID string, sequence int64) agentos.PlanEvent {
	return agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   eventID,
			EventType: agentos.EventPlanNodeStarted,
			RunID:     "run-1",
			Sequence:  sequence,
			Timestamp: time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC),
			Payload:   map[string]any{"ok": true},
		},
		PlanID: "plan-1",
		NodeID: "node-1",
	}
}

type fakePlanEventRedis struct {
	entries    map[string]map[string]goredis.XMessage
	addCalls   int
	expiredKey string
}

func newFakePlanEventRedis() *fakePlanEventRedis {
	return &fakePlanEventRedis{
		entries: make(map[string]map[string]goredis.XMessage),
	}
}

func (r *fakePlanEventRedis) StreamAddWithID(_ context.Context, stream string, id string, values map[string]any, _ int) (string, error) {
	r.addCalls++
	if r.entries[stream] == nil {
		r.entries[stream] = make(map[string]goredis.XMessage)
	}
	if _, exists := r.entries[stream][id]; exists {
		return "", errors.New("duplicate stream id")
	}
	r.entries[stream][id] = goredis.XMessage{ID: id, Values: values}

	return id, nil
}

func (r *fakePlanEventRedis) StreamRange(_ context.Context, stream string, start string, _ string, _ int) ([]goredis.XMessage, error) {
	entry, ok := r.entry(stream, start)
	if !ok {
		return nil, nil
	}

	return []goredis.XMessage{entry}, nil
}

func (r *fakePlanEventRedis) Expire(_ context.Context, key string, _ time.Duration) (bool, error) {
	r.expiredKey = key

	return true, nil
}

func (r *fakePlanEventRedis) store(stream string, id string, event agentos.PlanEvent) {
	data, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		panic(err)
	}
	if r.entries[stream] == nil {
		r.entries[stream] = make(map[string]goredis.XMessage)
	}
	r.entries[stream][id] = goredis.XMessage{
		ID: id,
		Values: map[string]any{
			"data": string(data),
		},
	}
}

func (r *fakePlanEventRedis) entry(stream string, id string) (goredis.XMessage, bool) {
	entries := r.entries[stream]
	if entries == nil {
		return goredis.XMessage{}, false
	}
	entry, ok := entries[id]

	return entry, ok
}
