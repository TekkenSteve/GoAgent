// Package agentosruntimetest provides conformance helpers for AgentOS runtime implementations.
package agentosruntimetest

import (
	"context"
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

var errProbeSubscriptionClosed = errors.New("subscription already closed")

const conformanceAfterSequence = 41

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
func RunBackendConformance(t *testing.T, tc *BackendConformanceCase) {
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
		spec := agentos.RunSpec{RunID: tc.RunID, ThreadID: "agentos-conformance-thread", AccountID: "agentos-conformance-account", ProjectID: "agentos-conformance-project", UserMessage: "hello", Backend: tc.Ref, Input: map[string]any{"purpose": "conformance"}}

		assertBackendStart(ctx, t, tc.Backend, &spec, tc.RunID)
		assertBackendSignal(ctx, t, tc.Backend, tc.RunID)
		assertBackendControl(ctx, t, tc.Backend, tc.RunID)
		assertBackendStatus(ctx, t, tc.Backend, tc.RunID, tc.StatusState)
		assertBackendSubscribe(ctx, t, tc)
	})
}

func assertBackendStart(ctx context.Context, t *testing.T, backend agentosruntime.AgentBackend, spec *agentos.RunSpec, runID string) {
	t.Helper()

	status, err := backend.Start(ctx, spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if status.RunID != runID {
		t.Fatalf("Start status run id = %q, want %q", status.RunID, runID)
	}
}

func assertBackendSignal(ctx context.Context, t *testing.T, backend agentosruntime.AgentBackend, runID string) {
	t.Helper()

	signal := agentoscore.Signal{Type: agentoscore.SignalUserMessage, IdempotencyKey: "agentos-conformance-signal", Payload: map[string]any{"content": "continue"}, SentAt: time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)}
	if err := backend.Signal(ctx, runID, &signal); err != nil {
		t.Fatalf("Signal user.message: %v", err)
	}
}

func assertBackendControl(ctx context.Context, t *testing.T, backend agentosruntime.AgentBackend, runID string) {
	t.Helper()

	control := agentoscore.ControlRequest{Operation: agentoscore.ControlCancel}
	if err := backend.Control(ctx, runID, &control); err != nil {
		t.Fatalf("Control cancel: %v", err)
	}
}

func assertBackendStatus(ctx context.Context, t *testing.T, backend agentosruntime.AgentBackend, runID, statusState string) {
	t.Helper()

	current, err := backend.Status(ctx, runID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if current.RunID != runID {
		t.Fatalf("Status run id = %q, want %q", current.RunID, runID)
	}

	if current.LifecycleState != statusState {
		t.Fatalf("Status lifecycle = %q, want %q", current.LifecycleState, statusState)
	}
}

func assertBackendSubscribe(ctx context.Context, t *testing.T, tc *BackendConformanceCase) {
	t.Helper()

	if tc.SubscriberProbe == nil {
		return
	}

	subscription, err := tc.Backend.Subscribe(ctx, agentoscore.StreamScope{RunID: tc.RunID, ThreadID: "agentos-conformance-thread", AfterSequence: conformanceAfterSequence})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	t.Cleanup(func() {
		if err := subscription.Close(); err != nil {
			t.Errorf("close subscription: %v", err)
		}
	})

	if got := tc.SubscriberProbe.LastScope(); got.RunID != tc.RunID || got.AfterSequence != conformanceAfterSequence {
		t.Fatalf("Subscribe scope = %#v", got)
	}
}

// SubscriberProbe records SubscribeAgentOS calls for backend conformance tests.
type SubscriberProbe struct {
	scope agentoscore.StreamScope
}

// LifecycleProbe records LifecyclePublisher calls for backend wiring tests —
// the write-side twin of SubscriberProbe. It captures what a backend forwards
// to its data-plane lifecycle adapter (Start → PublishStarted, Status →
// PublishStatus), so a test can verify the wiring without a real bus.
type LifecycleProbe struct {
	startedSpec   agentos.RunSpec
	startedStatus agentos.RunStatus
	statuses      []agentos.RunStatus
}

// PublishStarted implements LifecyclePublisher.
func (p *LifecycleProbe) PublishStarted(_ context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) {
	if spec != nil {
		p.startedSpec = *spec
	}

	if status != nil {
		p.startedStatus = *status
	}
}

// PublishStatus implements LifecyclePublisher.
func (p *LifecycleProbe) PublishStatus(_ context.Context, _ string, status *agentos.RunStatus) {
	if status != nil {
		p.statuses = append(p.statuses, *status)
	}
}

// LastStartedSpec returns the run spec of the last PublishStarted call.
func (p *LifecycleProbe) LastStartedSpec() agentos.RunSpec {
	return p.startedSpec
}

// LastStartedStatus returns the status of the last PublishStarted call.
func (p *LifecycleProbe) LastStartedStatus() agentos.RunStatus {
	return p.startedStatus
}

// Statuses returns every status forwarded by PublishStatus calls, in order.
func (p *LifecycleProbe) Statuses() []agentos.RunStatus {
	return p.statuses
}

// SubscribeAgentOS implements EventSubscriber.
func (s *SubscriberProbe) SubscribeAgentOS(_ context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	s.scope = scope

	return &probeSubscription{events: make(chan agentoscore.Event)}, nil
}

// LastScope returns the last stream scope received by the probe.
func (s *SubscriberProbe) LastScope() agentoscore.StreamScope {
	return s.scope
}

type probeSubscription struct {
	events chan agentoscore.Event
	closed bool
}

func (s *probeSubscription) Events() <-chan agentoscore.Event {
	return s.events
}

func (s *probeSubscription) Close() error {
	if s.closed {
		return errProbeSubscriptionClosed
	}

	s.closed = true
	close(s.events)

	return nil
}
