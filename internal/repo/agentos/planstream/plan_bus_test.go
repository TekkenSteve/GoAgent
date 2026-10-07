package planstream

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
)

// TestPlanEventStreamRoundTrip locks the bus publish→subscribe→decode path: a
// PlanEvent published on the plan channel is decoded back to the identical
// typed event on the live subscription.
func TestPlanEventStreamRoundTrip(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	stream := New(bus, bus)

	scope := agentos.PlanStreamScope{
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	// Subscribe before publishing: the bus subscription is live-only (PG owns
	// replay), so only events after this point are delivered.
	sub, err := stream.SubscribePlanEvents(t.Context(), &scope)
	if err != nil {
		t.Fatalf("SubscribePlanEvents: %v", err)
	}

	defer sub.Close()

	event := testPlanEvent("evt-1")
	if err := stream.PublishPlanEvent(t.Context(), &event); err != nil {
		t.Fatalf("PublishPlanEvent: %v", err)
	}

	select {
	case got, ok := <-sub.Events():
		if !ok {
			t.Fatal("subscription closed before the plan event")
		}

		assertPlanEventMatches(t, &got, &event)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the plan event")
	}
}

func assertPlanEventMatches(t *testing.T, got, want *agentos.PlanEvent) {
	t.Helper()

	if got.EventID != want.EventID || got.PlanID != want.PlanID ||
		got.AccountID != want.AccountID || got.ProjectID != want.ProjectID ||
		got.Sequence != want.Sequence || got.NodeID != want.NodeID {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

// TestPlanEventStreamFiltersMismatchedScope locks the per-channel scope
// filtering: an event for a different tenant on the same bus never reaches the
// subscription.
func TestPlanEventStreamFiltersMismatchedScope(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	stream := New(bus, bus)

	scope := agentos.PlanStreamScope{
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	sub, err := stream.SubscribePlanEvents(t.Context(), &scope)
	if err != nil {
		t.Fatalf("SubscribePlanEvents: %v", err)
	}

	defer sub.Close()

	// Same bus, different plan channel: the subscriber filter must reject it.
	other := testPlanEvent("evt-other")
	other.PlanID = "plan-other"

	if err := stream.PublishPlanEvent(t.Context(), &other); err != nil {
		t.Fatalf("PublishPlanEvent: %v", err)
	}

	select {
	case got, ok := <-sub.Events():
		if ok {
			t.Fatalf("mismatched plan event delivered: %#v", got)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

// TestPlanEventStreamSubscribeRequiresTenantScope locks scope validation: a
// live subscription without the full tenant boundary is rejected.
func TestPlanEventStreamSubscribeRequiresTenantScope(t *testing.T) {
	t.Parallel()

	stream := New(memstream.New(), memstream.New())
	for _, scope := range []agentos.PlanStreamScope{
		{PlanID: "plan-1"},
		{PlanID: "plan-1", AccountID: "acct-1"},
		{PlanID: "plan-1", ProjectID: "proj-1"},
	} {
		sub, err := stream.SubscribePlanEvents(t.Context(), &scope)
		if sub != nil {
			t.Fatalf("SubscribePlanEvents scope %#v returned subscription", scope)
		}

		if !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
			t.Fatalf("SubscribePlanEvents scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}
}

// TestPlanEventStreamPublishRequiresTenantScope locks publish-side validation:
// MarshalPlanEvent rejects an event that lacks the tenant boundary.
func TestPlanEventStreamPublishRequiresTenantScope(t *testing.T) {
	t.Parallel()

	stream := New(memstream.New(), memstream.New())
	valid := testPlanEvent("evt-1")
	cases := map[string]agentos.PlanEvent{
		"account id": func() agentos.PlanEvent {
			event := valid
			event.AccountID = ""

			return event
		}(),
		"project id": func() agentos.PlanEvent {
			event := valid
			event.ProjectID = ""

			return event
		}(),
	}

	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := stream.PublishPlanEvent(t.Context(), &event)
			if !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
				t.Fatalf("PublishPlanEvent error = %v, want ErrInvalidPlanEvent", err)
			}
		})
	}
}

// TestPlanEventStreamNotConfigured locks the fail-degraded states: a nil
// publisher/subscriber surfaces a typed error instead of a panic.
func TestPlanEventStreamNotConfigured(t *testing.T) {
	t.Parallel()

	var stream *PlanEventStream

	event := testPlanEvent("evt-1")
	if err := stream.PublishPlanEvent(t.Context(), &event); !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("PublishPlanEvent nil stream error = %v, want ErrInvalidPlanEvent", err)
	}

	scope := agentos.PlanStreamScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}

	sub, err := stream.SubscribePlanEvents(t.Context(), &scope)
	if sub != nil {
		t.Fatal("SubscribePlanEvents nil stream returned subscription")
	}

	if !errors.Is(err, agentoscore.ErrInvalidPlanEvent) && !errors.Is(err, agentoscore.ErrInvalidStreamScope) {
		t.Fatalf("SubscribePlanEvents nil stream error = %v, want ErrInvalidPlanEvent or ErrInvalidStreamScope", err)
	}
}

func TestPlanEventMatchesScopeRejectsTenantMismatch(t *testing.T) {
	t.Parallel()

	event := testPlanEvent("evt-1")
	for _, scope := range []agentos.PlanStreamScope{
		{PlanID: event.PlanID, AccountID: "acct-other", ProjectID: event.ProjectID},
		{PlanID: event.PlanID, AccountID: event.AccountID, ProjectID: "proj-other"},
	} {
		if planEventMatchesScope(&event, &scope) {
			t.Fatalf("planEventMatchesScope(%#v, %#v) = true, want false", event, scope)
		}
	}

	scope := agentos.PlanStreamScope{
		PlanID:    event.PlanID,
		AccountID: event.AccountID,
		ProjectID: event.ProjectID,
	}
	if !planEventMatchesScope(&event, &scope) {
		t.Fatal("planEventMatchesScope rejected matching tenant scope")
	}
}

func TestPlanEventMatchesScopeAppliesCursor(t *testing.T) {
	t.Parallel()

	event := testPlanEvent("evt-1")

	scope := agentos.PlanStreamScope{
		PlanID:        event.PlanID,
		AccountID:     event.AccountID,
		ProjectID:     event.ProjectID,
		AfterSequence: event.Sequence,
	}
	if planEventMatchesScope(&event, &scope) {
		t.Fatal("planEventMatchesScope accepted event at or below the cursor")
	}

	scope.AfterSequence = event.Sequence - 1
	if !planEventMatchesScope(&event, &scope) {
		t.Fatal("planEventMatchesScope rejected event above the cursor")
	}
}

func testPlanEvent(eventID string) agentos.PlanEvent {
	return agentos.PlanEvent{
		Event: agentoscore.Event{
			EventID:   eventID,
			EventType: agentoscore.EventPlanNodeStarted,
			RunID:     "run-1",
			Sequence:  7,
			Timestamp: time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC),
			Payload:   map[string]any{"ok": true},
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		NodeID:    "node-1",
	}
}
