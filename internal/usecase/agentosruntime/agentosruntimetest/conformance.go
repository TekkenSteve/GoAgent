package agentosruntimetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

// BackendConformanceCase describes a backend implementation under test.
type BackendConformanceCase struct {
	Name            string
	Backend         agentosruntime.AgentBackend
	Ref             agentos.BackendRef
	RunID           string
	StatusState     string
	SubscriberProbe *SubscriberProbe
}

// RunBackendConformance verifies the shared AgentBackend contract.
func RunBackendConformance(t *testing.T, tc BackendConformanceCase) {
	t.Helper()
	if tc.Backend == nil {
		t.Fatal("backend is required")
	}
	if tc.Ref.Kind == "" || tc.Ref.Name == "" {
		t.Fatal("backend ref is required")
	}
	if tc.RunID == "" {
		tc.RunID = "agentos-conformance-run"
	}
	if tc.StatusState == "" {
		tc.StatusState = "running"
	}

	t.Run(tc.Name+"/start-status-signal-control-subscribe", func(t *testing.T) {
		ctx := context.Background()
		status, err := tc.Backend.Start(ctx, agentos.RunSpec{
			RunID:       tc.RunID,
			ThreadID:    "agentos-conformance-thread",
			AccountID:   "agentos-conformance-account",
			UserMessage: "hello",
			Backend:     tc.Ref,
			Input: map[string]any{
				"purpose": "conformance",
			},
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if status.RunID != tc.RunID {
			t.Fatalf("Start status run id = %q, want %q", status.RunID, tc.RunID)
		}

		if err := tc.Backend.Signal(ctx, tc.RunID, agentos.Signal{
			Type:           agentos.SignalUserMessage,
			IdempotencyKey: "agentos-conformance-signal",
			Payload: map[string]any{
				"content": "continue",
			},
			SentAt: time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
		}); err != nil {
			t.Fatalf("Signal user.message: %v", err)
		}

		if err := tc.Backend.Control(ctx, tc.RunID, agentos.ControlRequest{Operation: agentos.ControlCancel}); err != nil {
			t.Fatalf("Control cancel: %v", err)
		}

		current, err := tc.Backend.Status(ctx, tc.RunID)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if current.RunID != tc.RunID {
			t.Fatalf("Status run id = %q, want %q", current.RunID, tc.RunID)
		}
		if current.LifecycleState != tc.StatusState {
			t.Fatalf("Status lifecycle = %q, want %q", current.LifecycleState, tc.StatusState)
		}

		if tc.SubscriberProbe == nil {
			return
		}

		subscription, err := tc.Backend.Subscribe(ctx, agentos.StreamScope{
			RunID:         tc.RunID,
			ThreadID:      "agentos-conformance-thread",
			AfterSequence: 41,
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		defer subscription.Close()

		if got := tc.SubscriberProbe.LastScope(); got.RunID != tc.RunID || got.AfterSequence != 41 {
			t.Fatalf("Subscribe scope = %#v", got)
		}
	})
}

// SubscriberProbe records SubscribeAgentOS calls for backend conformance tests.
type SubscriberProbe struct {
	scope agentos.StreamScope
}

// SubscribeAgentOS implements EventSubscriber.
func (s *SubscriberProbe) SubscribeAgentOS(_ context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	s.scope = scope

	return &probeSubscription{events: make(chan agentos.Event)}, nil
}

// LastScope returns the last stream scope received by the probe.
func (s *SubscriberProbe) LastScope() agentos.StreamScope {
	return s.scope
}

type probeSubscription struct {
	events chan agentos.Event
	closed bool
}

func (s *probeSubscription) Events() <-chan agentos.Event {
	return s.events
}

func (s *probeSubscription) Close() error {
	if s.closed {
		return fmt.Errorf("subscription already closed")
	}
	s.closed = true
	close(s.events)

	return nil
}
