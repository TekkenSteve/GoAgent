package agentosruntime

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
	router, err := NewRouter(registry, newStubRunIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.Start(ctx, agentos.RunSpec{
		RunID:          "run-1",
		UserMessage:    "hello",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
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
	if err := router.Control(ctx, "run-1", agentos.ControlRequest{Operation: agentos.ControlPause}); err != nil {
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
	router, err := NewRouter(NewRegistry(), newStubRunIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	_, err = router.Start(context.Background(), agentos.RunSpec{RunID: "run-1", IdempotencyKey: "run-start-1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRouterStartRequiresIdempotencyKey(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	router, err := NewRouter(registry, newStubRunIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	_, err = router.Start(context.Background(), agentos.RunSpec{RunID: "run-1", Backend: ref})
	if err == nil {
		t.Fatal("expected error")
	}
	if stub.startBackend != (agentos.BackendRef{}) {
		t.Fatalf("backend was started before idempotency validation: %#v", stub.startBackend)
	}
}

func TestRouterSelectsBackendWhenSpecOmitsBackend(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"}
	stub := &stubBackend{}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	selector, err := NewRuleBackendSelector([]BackendSelectionRule{
		{
			Backend: ref,
			Input: map[string]any{
				"task_type": "research_report",
			},
		},
	})
	if err != nil {
		t.Fatalf("new selector: %v", err)
	}
	index := newStubRunIndex()
	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	router.WithBackendSelector(selector)

	status, err := router.Start(ctx, agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Input: map[string]any{
			"task_type": "research_report",
		},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if status.RunID != "run-1" {
		t.Fatalf("status run id = %q", status.RunID)
	}
	if got := index.routes["run-1"]; got != ref {
		t.Fatalf("route = %#v, want %#v", got, ref)
	}
	if stub.startBackend != ref {
		t.Fatalf("backend passed to start = %#v, want %#v", stub.startBackend, ref)
	}
}

func TestRouterStartPlanNodeBindsPlanOwnership(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	index := newStubRunIndex()
	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.StartPlanNode(ctx, "plan-1", "node-1", agentos.RunSpec{
		RunID:          "run-1",
		Backend:        ref,
		IdempotencyKey: "node-start-1",
	})
	if err != nil {
		t.Fatalf("StartPlanNode: %v", err)
	}
	if status.RunID != "run-1" {
		t.Fatalf("status run id = %q", status.RunID)
	}
	record := index.planRoutes["run-1"]
	if record.PlanID != "plan-1" || record.NodeID != "node-1" || record.Backend != ref {
		t.Fatalf("plan route = %#v", record)
	}
	if _, ok := index.routes["run-1"]; ok {
		t.Fatalf("standalone route was written for plan node")
	}
}

func TestRouterRoutesMixedBackendRunsByOwnership(t *testing.T) {
	ctx := context.Background()
	refs := []agentos.BackendRef{
		{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
		{Kind: agentos.BackendKindTemporalExternal, Name: "langgraph"},
		{Kind: agentos.BackendKindHTTP, Name: "claude-code"},
		{Kind: agentos.BackendKindGRPC, Name: "grpc-agent"},
	}
	registry := NewRegistry()
	backends := make(map[agentos.BackendRef]*stubBackend, len(refs))
	for _, ref := range refs {
		backend := &stubBackend{}
		backends[ref] = backend
		if err := registry.Register(ref, backend); err != nil {
			t.Fatalf("register %s/%s: %v", ref.Kind, ref.Name, err)
		}
	}
	router, err := NewRouter(registry, newStubRunIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	for i, ref := range refs {
		runID := "run-" + string(ref.Kind)
		if ref.Kind == agentos.BackendKindNative {
			runID = "run-native"
		}
		_, err := router.Start(ctx, agentos.RunSpec{
			RunID:          runID,
			Backend:        ref,
			IdempotencyKey: "run-start-" + runID,
			Input: map[string]any{
				"ordinal": i,
			},
		})
		if err != nil {
			t.Fatalf("start %s/%s: %v", ref.Kind, ref.Name, err)
		}
		if _, err := router.Status(ctx, runID); err != nil {
			t.Fatalf("status %s: %v", runID, err)
		}
		if err := router.Control(ctx, runID, agentos.ControlRequest{Operation: agentos.ControlCancel}); err != nil {
			t.Fatalf("control %s: %v", runID, err)
		}
		if err := router.Signal(ctx, runID, agentos.Signal{Type: agentos.SignalUserMessage}); err != nil {
			t.Fatalf("signal %s: %v", runID, err)
		}

		backend := backends[ref]
		if backend.startBackend != ref || backend.statusRunID != runID || backend.controlRunID != runID || backend.signalRunID != runID {
			t.Fatalf("backend %s/%s calls = %#v, runID=%s", ref.Kind, ref.Name, backend, runID)
		}
	}
}

func TestRuleBackendSelectorRequiresMatchingRule(t *testing.T) {
	selector, err := NewRuleBackendSelector([]BackendSelectionRule{
		{
			Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"},
			Input: map[string]any{
				"task_type": "research_report",
			},
		},
	})
	if err != nil {
		t.Fatalf("new selector: %v", err)
	}

	_, err = selector.Select(context.Background(), agentos.RunSpec{
		RunID: "run-1",
		Input: map[string]any{
			"task_type": "main",
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRuleBackendSelectorRejectsRuleWithoutCriteria(t *testing.T) {
	_, err := NewRuleBackendSelector([]BackendSelectionRule{
		{
			Name:    "default",
			Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

type stubBackend struct {
	statusRunID  string
	controlRunID string
	signalRunID  string
	startBackend agentos.BackendRef
}

type stubRunIndex struct {
	routes     map[string]agentos.BackendRef
	planRoutes map[string]stubPlanRoute
}

type stubPlanRoute struct {
	PlanID  string
	NodeID  string
	Backend agentos.BackendRef
}

func newStubRunIndex() *stubRunIndex {
	return &stubRunIndex{
		routes:     make(map[string]agentos.BackendRef),
		planRoutes: make(map[string]stubPlanRoute),
	}
}

func (i *stubRunIndex) Bind(_ context.Context, spec agentos.RunSpec) error {
	i.routes[spec.RunID] = spec.Backend

	return nil
}

func (i *stubRunIndex) BindPlanNode(_ context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error {
	runID := status.RunID
	if runID == "" {
		runID = spec.RunID
	}
	i.planRoutes[runID] = stubPlanRoute{
		PlanID:  planID,
		NodeID:  nodeID,
		Backend: spec.Backend,
	}

	return nil
}

func (i *stubRunIndex) Resolve(_ context.Context, runID string) (agentos.BackendRef, error) {
	if route, ok := i.planRoutes[runID]; ok {
		return route.Backend, nil
	}

	return i.routes[runID], nil
}

func (b *stubBackend) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	b.startBackend = spec.Backend

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created", UpdatedAt: time.Now()}, nil
}

func (b *stubBackend) Signal(_ context.Context, runID string, _ agentos.Signal) error {
	b.signalRunID = runID

	return nil
}

func (b *stubBackend) Control(_ context.Context, runID string, _ agentos.ControlRequest) error {
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
