package temporal

import (
	"errors"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
)

// TestSubscribeAgentOSReducesToMilestones locks the run-subscribe reduction: a
// milestone survives ProjectToCore and is stamped with the bus offset, while a
// transient byte delta on the same channel is filtered before the caller sees
// it — the subscribe API and the durable projection agree on what survives.
func TestSubscribeAgentOSReducesToMilestones(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	agentSub := newAgentOSSubscriber(bus)
	runID := "run-1"
	handle := streamadapter.HandleForRun("", runID)

	if err := bus.Publish(t.Context(), handle, agentosstream.NewRunStarted("thread-1", runID)); err != nil {
		t.Fatalf("publish run started: %v", err)
	}

	if err := bus.Publish(t.Context(), handle, agentosstream.NewTextMessageContent("thread-1", runID, "m-1", "hello")); err != nil {
		t.Fatalf("publish text delta: %v", err)
	}

	sub, err := agentSub.SubscribeAgentOS(t.Context(), agentoscore.StreamScope{RunID: runID})
	if err != nil {
		t.Fatalf("SubscribeAgentOS: %v", err)
	}

	defer sub.Close()

	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatal("subscription closed before the run started milestone")
		}

		assertRunStartedMilestone(t, &ev, runID, handle)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the run started milestone")
	}

	// The transient delta was replayed on the same channel but filtered by
	// ProjectToCore, so no further event arrives before close.
	select {
	case ev, ok := <-sub.Events():
		if ok {
			t.Fatalf("transient delta should not project: %#v", ev)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func assertRunStartedMilestone(t *testing.T, ev *agentoscore.Event, runID string, handle *agentosstream.Handle) {
	t.Helper()

	if ev.EventType != agentoscore.EventRunStarted {
		t.Fatalf("event type = %q, want %q", ev.EventType, agentoscore.EventRunStarted)
	}

	if ev.RunID != runID || ev.ThreadID != "thread-1" {
		t.Fatalf("scope = %s/%s, want %s/thread-1", ev.RunID, ev.ThreadID, runID)
	}

	if ev.Sequence != 1 {
		t.Fatalf("sequence = %d, want 1", ev.Sequence)
	}

	if ev.EventID != runID+":1" {
		t.Fatalf("event id = %q, want %q", ev.EventID, runID+":1")
	}

	if ev.Source != handle.Channel {
		t.Fatalf("source = %q, want %q", ev.Source, handle.Channel)
	}
}

func TestSubscribeAgentOSRequiresRunID(t *testing.T) {
	t.Parallel()

	agentSub := newAgentOSSubscriber(memstream.New())

	_, err := agentSub.SubscribeAgentOS(t.Context(), agentoscore.StreamScope{})
	if !errors.Is(err, agentoscore.ErrInvalidStreamScope) {
		t.Fatalf("SubscribeAgentOS empty run id error = %v, want %v", err, agentoscore.ErrInvalidStreamScope)
	}
}

func TestSubscribeAgentOSNotConfigured(t *testing.T) {
	t.Parallel()

	var agentSub *agentOSSubscriber

	_, err := agentSub.SubscribeAgentOS(t.Context(), agentoscore.StreamScope{RunID: "run-1"})
	if !errors.Is(err, errAgentOSSubscriberNotConfigured) {
		t.Fatalf("SubscribeAgentOS nil subscriber error = %v, want %v", err, errAgentOSSubscriberNotConfigured)
	}
}
