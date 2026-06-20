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

func TestRouterStartClaimsOwnershipBeforeBackendStart(t *testing.T) {
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

	if _, err := router.Start(ctx, agentos.RunSpec{
		RunID:          "run-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if len(index.routeLifecycle) != 2 ||
		index.routeLifecycle[0] != RunBackendLifecycleClaiming ||
		index.routeLifecycle[1] != "created" {
		t.Fatalf("route lifecycle = %#v, want claim before completion", index.routeLifecycle)
	}
	if stub.startCount != 1 {
		t.Fatalf("start count = %d, want 1", stub.startCount)
	}
}

func TestRouterStartReusesCompletedOwnership(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	index := newStubRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}
	if err := index.Bind(ctx, spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.Start(ctx, spec)
	if err != nil {
		t.Fatalf("Start replay: %v", err)
	}
	if status.RunID != spec.RunID {
		t.Fatalf("status run id = %q, want %q", status.RunID, spec.RunID)
	}
	if stub.startCount != 0 {
		t.Fatalf("backend start count = %d, want 0 for completed ownership replay", stub.startCount)
	}
	if stub.statusRunID != spec.RunID {
		t.Fatalf("status run id = %q, want %q", stub.statusRunID, spec.RunID)
	}
}

func TestRouterStartRejectsBackendRunIDDrift(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{startStatus: agentos.RunStatus{RunID: "backend-run", LifecycleState: "running"}}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	router, err := NewRouter(registry, newStubRunIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	_, err = router.Start(ctx, agentos.RunSpec{
		RunID:          "run-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	})
	if err == nil {
		t.Fatal("Start succeeded, want run id drift error")
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

func TestRouterStartPlanNodeClaimsOwnershipBeforeBackendStart(t *testing.T) {
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

	if _, err := router.StartPlanNode(ctx, "plan-1", "node-1", agentos.RunSpec{
		RunID:          "run-1",
		Backend:        ref,
		IdempotencyKey: "node-start-1",
	}); err != nil {
		t.Fatalf("StartPlanNode: %v", err)
	}

	if len(index.planRouteLifecycle) != 2 ||
		index.planRouteLifecycle[0] != RunBackendLifecycleClaiming ||
		index.planRouteLifecycle[1] != "created" {
		t.Fatalf("plan route lifecycle = %#v, want claim before completion", index.planRouteLifecycle)
	}
	if stub.startCount != 1 {
		t.Fatalf("start count = %d, want 1", stub.startCount)
	}
}

func TestRouterStartPlanNodeReusesCompletedOwnership(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{}
	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}
	index := newStubRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		Backend:        ref,
		IdempotencyKey: "node-start-1",
	}
	if err := index.BindPlanNode(ctx, "plan-1", "node-1", spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}); err != nil {
		t.Fatalf("BindPlanNode: %v", err)
	}
	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.StartPlanNode(ctx, "plan-1", "node-1", spec)
	if err != nil {
		t.Fatalf("StartPlanNode replay: %v", err)
	}
	if status.RunID != spec.RunID {
		t.Fatalf("status run id = %q, want %q", status.RunID, spec.RunID)
	}
	if stub.startCount != 0 {
		t.Fatalf("backend start count = %d, want 0 for completed ownership replay", stub.startCount)
	}
	if stub.statusRunID != spec.RunID {
		t.Fatalf("status run id = %q, want %q", stub.statusRunID, spec.RunID)
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
	startCount   int
	startStatus  agentos.RunStatus
}

type stubRunIndex struct {
	routes             map[string]agentos.BackendRef
	routeKeys          map[string]string
	routeLifecycle     []string
	planRoutes         map[string]stubPlanRoute
	planRouteLifecycle []string
}

type stubPlanRoute struct {
	PlanID         string
	NodeID         string
	Backend        agentos.BackendRef
	IdempotencyKey string
	LifecycleState string
}

func newStubRunIndex() *stubRunIndex {
	return &stubRunIndex{
		routes:     make(map[string]agentos.BackendRef),
		routeKeys:  make(map[string]string),
		planRoutes: make(map[string]stubPlanRoute),
	}
}

func (i *stubRunIndex) Bind(_ context.Context, spec agentos.RunSpec, status agentos.RunStatus) error {
	i.routes[spec.RunID] = spec.Backend
	i.routeKeys[spec.RunID] = spec.IdempotencyKey
	i.routeLifecycle = append(i.routeLifecycle, status.LifecycleState)

	return nil
}

func (i *stubRunIndex) BindPlanNode(_ context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error {
	runID := status.RunID
	if runID == "" {
		runID = spec.RunID
	}
	i.planRoutes[runID] = stubPlanRoute{
		PlanID:         planID,
		NodeID:         nodeID,
		Backend:        spec.Backend,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: status.LifecycleState,
	}
	i.planRouteLifecycle = append(i.planRouteLifecycle, status.LifecycleState)

	return nil
}

func (i *stubRunIndex) GetRunBackend(_ context.Context, runID string) (agentos.RunBackendOwnership, bool, error) {
	if route, ok := i.routes[runID]; ok {
		lifecycle := ""
		if len(i.routeLifecycle) > 0 {
			lifecycle = i.routeLifecycle[len(i.routeLifecycle)-1]
		}

		return agentos.RunBackendOwnership{
			RunID:          runID,
			Backend:        route,
			IdempotencyKey: i.routeKeys[runID],
			LifecycleState: lifecycle,
		}, true, nil
	}
	route, ok := i.planRoutes[runID]
	if !ok {
		return agentos.RunBackendOwnership{}, false, nil
	}

	return agentos.RunBackendOwnership{
		RunID:          runID,
		PlanID:         route.PlanID,
		NodeID:         route.NodeID,
		Backend:        route.Backend,
		IdempotencyKey: route.IdempotencyKey,
		LifecycleState: route.LifecycleState,
	}, true, nil
}

func (i *stubRunIndex) Resolve(_ context.Context, runID string) (agentos.BackendRef, error) {
	if route, ok := i.planRoutes[runID]; ok {
		return route.Backend, nil
	}

	return i.routes[runID], nil
}

func (b *stubBackend) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	b.startBackend = spec.Backend
	b.startCount++
	if b.startStatus.RunID != "" || b.startStatus.LifecycleState != "" {
		return b.startStatus, nil
	}

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
