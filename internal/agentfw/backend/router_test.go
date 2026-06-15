package backend

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRouterRoutesRunOperationsToRegisteredBackend(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	router, err := NewRouter(registry, NewMemoryRunBackendIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.Start(ctx, agentos.RunSpec{
		RunID:       "run-1",
		UserMessage: "hello",
		Backend:     ref,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if status.RunID != "run-1" {
		t.Fatalf("status run id = %q", status.RunID)
	}

	if _, err := router.Status(ctx, "run-1"); err != nil {
		t.Fatalf("status: %v", err)
	}
	if err := router.Control(ctx, "run-1", agentos.ControlPause); err != nil {
		t.Fatalf("control: %v", err)
	}
	if err := router.Signal(ctx, "run-1", agentos.Signal{Type: agentos.SignalUserMessage}); err != nil {
		t.Fatalf("signal: %v", err)
	}

	if stub.statusRunID != "run-1" || stub.controlRunID != "run-1" || stub.signalRunID != "run-1" {
		t.Fatalf("router did not route run operations: %#v", stub)
	}
}

func TestRouterRequiresExplicitBackendRef(t *testing.T) {
	router, err := NewRouter(NewRegistry(), NewMemoryRunBackendIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	_, err = router.Start(context.Background(), agentos.RunSpec{RunID: "run-1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

type stubBackend struct {
	statusRunID  string
	controlRunID string
	signalRunID  string
}

func (b *stubBackend) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created", UpdatedAt: time.Now()}, nil
}

func (b *stubBackend) Signal(_ context.Context, runID string, _ agentos.Signal) error {
	b.signalRunID = runID

	return nil
}

func (b *stubBackend) Control(_ context.Context, runID string, _ agentos.ControlOperation) error {
	b.controlRunID = runID

	return nil
}

func (b *stubBackend) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	b.statusRunID = runID

	return agentos.RunStatus{RunID: runID}, nil
}

func (b *stubBackend) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}
