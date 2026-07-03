package agentosruntime

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const Run1 = "run-1"

func TestRouterRoutesRunOperationsToRegisteredBackend(t *testing.T) {
	t.Parallel()

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

	spec := agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		UserMessage:    "hello",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}

	status, err := router.Start(ctx, &spec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if status.RunID != Run1 {
		t.Fatalf("status run id = %q", status.RunID)
	}

	assertRoutesRunOperations(ctx, t, router, stub, ref, Run1)
}

func TestRouterRequiresExplicitBackendRef(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(NewRegistry(), newStubRunIndex())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	_, err = startRun(context.Background(), router, &agentos.RunSpec{RunID: "run-1", AccountID: "acct-1", ProjectID: "proj-1", IdempotencyKey: "run-start-1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRouterStartRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

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

	_, err = startRun(context.Background(), router, &agentos.RunSpec{RunID: "run-1", Backend: ref})
	if err == nil {
		t.Fatal("expected error")
	}

	if stub.startBackend != (agentos.BackendRef{}) {
		t.Fatalf("backend was started before idempotency validation: %#v", stub.startBackend)
	}
}

func TestRouterStartRequiresTenantScope(t *testing.T) {
	t.Parallel()

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

	_, err = startRun(context.Background(), router, &agentos.RunSpec{
		RunID:          "run-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if stub.startCount != 0 {
		t.Fatalf("backend start count = %d, want 0 before tenant validation", stub.startCount)
	}
}

func TestRouterStartClaimsOwnershipBeforeBackendStart(t *testing.T) {
	t.Parallel()

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

	if _, err := startRun(ctx, router, &agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if len(index.routeLifecycle) != 2 ||
		index.routeLifecycle[0] != RunBackendLifecycleClaiming ||
		index.routeLifecycle[1] != RunBackendLifecycleCreated {
		t.Fatalf("route lifecycle = %#v, want claim before completion", index.routeLifecycle)
	}

	if stub.startCount != 1 {
		t.Fatalf("start count = %d, want 1", stub.startCount)
	}
}

func TestRouterStartReusesCompletedOwnership(t *testing.T) {
	t.Parallel()

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
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}

	boundStatus := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.Bind(ctx, &spec, &boundStatus); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.Start(ctx, &spec)
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
	t.Parallel()

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

	_, err = startRun(ctx, router, &agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	})
	if err == nil {
		t.Fatal("Start succeeded, want run id drift error")
	}
}

func TestRouterStatusNormalizesBackendRunID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{statusStatus: agentos.RunStatus{LifecycleState: "running"}}

	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}

	index := newStubRunIndex()

	spec := agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}

	boundStatus := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.Bind(ctx, &spec, &boundStatus); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.Status(ctx, spec.RunID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if status.RunID != spec.RunID {
		t.Fatalf("status run id = %q, want %q", status.RunID, spec.RunID)
	}
}

func TestRouterStatusRejectsBackendRunIDDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	stub := &stubBackend{statusStatus: agentos.RunStatus{RunID: "backend-run", LifecycleState: "running"}}

	registry := NewRegistry()
	if err := registry.Register(ref, stub); err != nil {
		t.Fatalf("register backend: %v", err)
	}

	index := newStubRunIndex()

	spec := agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}

	boundStatus := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.Bind(ctx, &spec, &boundStatus); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	_, err = router.Status(ctx, spec.RunID)
	if err == nil {
		t.Fatal("Status succeeded, want run id drift error")
	}
}

func TestRouterSelectsBackendWhenSpecOmitsBackend(t *testing.T) {
	t.Parallel()

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

	spec := agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "run-start-1",
		Input: map[string]any{
			"task_type": "research_report",
		},
	}

	status, err := startRun(ctx, router, &spec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if status.RunID != Run1 {
		t.Fatalf("status run id = %q", status.RunID)
	}

	if got := index.routes["run-1"]; got != ref {
		t.Fatalf("route = %#v, want %#v", got, ref)
	}

	if stub.startBackend != ref {
		t.Fatalf("backend passed to start = %#v, want %#v", stub.startBackend, ref)
	}

	if spec.Backend != (agentos.BackendRef{}) {
		t.Fatalf("caller spec backend = %#v, want zero value", spec.Backend)
	}
}

func TestRouterSignalAndControlDoNotShareCallerOwnedData(t *testing.T) {
	t.Parallel()

	const (
		mutatedValue = "mutated"
		storedValue  = "stored"
	)

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
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-1",
	}

	status := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.Bind(ctx, &spec, &status); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	signal := agentos.Signal{Type: agentos.SignalUserMessage, Payload: map[string]any{"message": storedValue}}
	if err := router.Signal(ctx, spec.RunID, &signal); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	control := agentos.ControlRequest{Operation: agentos.ControlCancel, Metadata: map[string]string{"reason": storedValue}}
	if err := router.Control(ctx, spec.RunID, &control); err != nil {
		t.Fatalf("Control: %v", err)
	}

	signal.Payload["message"] = mutatedValue
	control.Metadata["reason"] = mutatedValue

	if stub.signal.Payload["message"] != storedValue {
		t.Fatalf("backend signal payload = %#v", stub.signal.Payload)
	}

	if stub.control.Metadata["reason"] != storedValue {
		t.Fatalf("backend control metadata = %#v", stub.control.Metadata)
	}
}

func TestRouterStartPlanNodeBindsPlanOwnership(t *testing.T) {
	t.Parallel()

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

	status, err := startPlanNode(ctx, router, "plan-1", "node-1", &agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "node-start-1",
	})
	if err != nil {
		t.Fatalf("StartPlanNode: %v", err)
	}

	if status.RunID != Run1 {
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
	t.Parallel()

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

	if _, err := startPlanNode(ctx, router, "plan-1", "node-1", &agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
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

func TestRouterStartPlanNodeRequiresTenantScope(t *testing.T) {
	t.Parallel()

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

	_, err = startPlanNode(ctx, router, "plan-1", "node-1", &agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		Backend:        ref,
		IdempotencyKey: "node-start-1",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if stub.startCount != 0 {
		t.Fatalf("backend start count = %d, want 0 before tenant validation", stub.startCount)
	}
}

func TestRouterStartPlanNodeReusesCompletedOwnership(t *testing.T) {
	t.Parallel()

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
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "node-start-1",
	}

	boundStatus := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.BindPlanNode(ctx, "plan-1", "node-1", &spec, &boundStatus); err != nil {
		t.Fatalf("BindPlanNode: %v", err)
	}

	router, err := NewRouter(registry, index)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	status, err := router.StartPlanNode(ctx, "plan-1", "node-1", &spec)
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
	t.Parallel()

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
		assertRoutesBackendRunByOwnership(ctx, t, router, backends[ref], ref, i)
	}
}

func assertRoutesBackendRunByOwnership(
	ctx context.Context,
	t *testing.T,
	router *Router,
	backend *stubBackend,
	ref agentos.BackendRef,
	ordinal int,
) {
	t.Helper()

	runID := runIDForBackendRef(ref)

	_, err := startRun(ctx, router, &agentos.RunSpec{
		RunID:          runID,
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		Backend:        ref,
		IdempotencyKey: "run-start-" + runID,
		Input: map[string]any{
			"ordinal": ordinal,
		},
	})
	if err != nil {
		t.Fatalf("start %s/%s: %v", ref.Kind, ref.Name, err)
	}

	assertRoutesRunOperations(ctx, t, router, backend, ref, runID)
}

func runIDForBackendRef(ref agentos.BackendRef) string {
	if ref.Kind == agentos.BackendKindNative {
		return "run-native"
	}

	return "run-" + string(ref.Kind)
}

func assertRoutesRunOperations(
	ctx context.Context,
	t *testing.T,
	router *Router,
	backend *stubBackend,
	ref agentos.BackendRef,
	runID string,
) {
	t.Helper()

	if _, err := router.Status(ctx, runID); err != nil {
		t.Fatalf("status %s: %v", runID, err)
	}

	if err := controlRun(ctx, router, runID, agentos.ControlCancel); err != nil {
		t.Fatalf("control %s: %v", runID, err)
	}

	if err := signalRun(ctx, router, runID, agentos.SignalUserMessage); err != nil {
		t.Fatalf("signal %s: %v", runID, err)
	}

	if backend.startBackend != ref || backend.statusRunID != runID || backend.controlRunID != runID || backend.signalRunID != runID {
		t.Fatalf("backend %s/%s calls = %#v, runID=%s", ref.Kind, ref.Name, backend, runID)
	}
}

func TestRuleBackendSelectorRequiresMatchingRule(t *testing.T) {
	t.Parallel()

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

	_, err = selector.Select(context.Background(), &agentos.RunSpec{
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
	t.Parallel()

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
	signal       agentos.Signal
	control      agentos.ControlRequest
	startBackend agentos.BackendRef
	startCount   int
	startStatus  agentos.RunStatus
	statusStatus agentos.RunStatus
}

func startRun(ctx context.Context, router *Router, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	return router.Start(ctx, spec)
}

func startPlanNode(
	ctx context.Context,
	router *Router,
	planID string,
	nodeID string,
	spec *agentos.RunSpec,
) (agentos.RunStatus, error) {
	return router.StartPlanNode(ctx, planID, nodeID, spec)
}

func controlRun(ctx context.Context, router *Router, runID string, operation agentos.ControlOperation) error {
	control := agentos.ControlRequest{Operation: operation}

	return router.Control(ctx, runID, &control)
}

func signalRun(ctx context.Context, router *Router, runID string, signalType agentos.SignalType) error {
	signal := agentos.Signal{Type: signalType}

	return router.Signal(ctx, runID, &signal)
}

type stubRunIndex struct {
	routes             map[string]agentos.BackendRef
	routeKeys          map[string]string
	routeRecords       map[string]agentos.RunBackendOwnership
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
		routes:       make(map[string]agentos.BackendRef),
		routeKeys:    make(map[string]string),
		routeRecords: make(map[string]agentos.RunBackendOwnership),
		planRoutes:   make(map[string]stubPlanRoute),
	}
}

func (i *stubRunIndex) Bind(_ context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) error {
	i.routes[spec.RunID] = spec.Backend
	i.routeKeys[spec.RunID] = spec.IdempotencyKey
	i.routeRecords[spec.RunID] = agentos.RunBackendOwnership{
		RunID:          spec.RunID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Backend:        spec.Backend,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: status.LifecycleState,
	}
	i.routeLifecycle = append(i.routeLifecycle, status.LifecycleState)

	return nil
}

func (i *stubRunIndex) BindPlanNode(_ context.Context, planID, nodeID string, spec *agentos.RunSpec, status *agentos.RunStatus) error {
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
	i.routeRecords[runID] = agentos.RunBackendOwnership{
		RunID:          runID,
		PlanID:         planID,
		NodeID:         nodeID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Backend:        spec.Backend,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: status.LifecycleState,
	}
	i.planRouteLifecycle = append(i.planRouteLifecycle, status.LifecycleState)

	return nil
}

func (i *stubRunIndex) GetRunBackend(_ context.Context, runID string) (agentos.RunBackendOwnership, bool, error) {
	if record, ok := i.routeRecords[runID]; ok {
		return record, true, nil
	}

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

func (b *stubBackend) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	b.startBackend = spec.Backend

	b.startCount++
	if b.startStatus.RunID != "" || b.startStatus.LifecycleState != "" {
		return b.startStatus, nil
	}

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created", UpdatedAt: time.Now()}, nil
}

func (b *stubBackend) Signal(_ context.Context, runID string, signal *agentos.Signal) error {
	b.signalRunID = runID
	if signal != nil {
		b.signal = *signal
	}

	return nil
}

func (b *stubBackend) Control(_ context.Context, runID string, control *agentos.ControlRequest) error {
	b.controlRunID = runID
	if control != nil {
		b.control = *control
	}

	return nil
}

func (b *stubBackend) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	b.statusRunID = runID
	if b.statusStatus.RunID != "" || b.statusStatus.LifecycleState != "" {
		return b.statusStatus, nil
	}

	return agentos.RunStatus{RunID: runID}, nil
}

func (b *stubBackend) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}
