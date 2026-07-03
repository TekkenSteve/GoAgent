package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/client"
)

var (
	errTestTemporalUnavailable    = errors.New("temporal unavailable")
	errTestShouldNotStartWorkflow = errors.New("should not start workflow")
	errTestShouldNotSignal        = errors.New("should not signal")
	errTestAuditUnavailable       = errors.New("audit unavailable")
	errTestUnexpectedCreatePlan   = errors.New("unexpected CreatePlan")
	errTestUnexpectedGetPlan      = errors.New("unexpected plan-only GetPlan")
)

const testPostgresURL = "postgres://localhost:5432/testdb"

func TestPlanRuntimeRequiresConfig(t *testing.T) {
	t.Parallel()

	_, err := NewPlanRuntime(t.Context(), nil)
	if !errors.Is(err, errPlanRuntimeConfigRequired) {
		t.Fatalf("NewPlanRuntime nil config error = %v, want %v", err, errPlanRuntimeConfigRequired)
	}

	_, err = NewPlanRuntimeWithClient(t.Context(), nil, nil)
	if !errors.Is(err, errPlanRuntimeConfigRequired) {
		t.Fatalf("NewPlanRuntimeWithClient nil config error = %v, want %v", err, errPlanRuntimeConfigRequired)
	}
}

func TestPlanRuntimeSignalPlanValidatesSignalBeforeAudit(t *testing.T) {
	t.Parallel()

	rt := &planRuntime{}
	ref := agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}

	err := rt.SignalPlan(t.Context(), ref, &agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "message-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidSignal", err)
	}

	err = rt.SignalPlan(t.Context(), ref, &agentos.Signal{
		Type:           agentos.SignalPlanNodeRetry,
		IdempotencyKey: "retry-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan retry error = %v, want ErrInvalidSignal", err)
	}
}

func TestPlanRuntimeStatusPlanReadsDurableIndex(t *testing.T) {
	t.Parallel()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "node-1",
				Run: agentos.RunSpec{
					RunID:   "run-1",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"},
				},
			},
		},
	}

	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleSucceeded,
		UpdatedAt:      time.Now().UTC(),
	}
	if _, _, err := store.CreatePlan(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	rt := &planRuntime{planIndex: store}

	got, err := rt.StatusPlan(t.Context(), agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID})
	if err != nil {
		t.Fatalf("StatusPlan: %v", err)
	}

	if got.PlanID != spec.PlanID || got.LifecycleState != agentos.PlanLifecycleSucceeded {
		t.Fatalf("status = %#v", got)
	}
}

func TestPlanRuntimeStatusPlanUsesScopedPlanIndex(t *testing.T) {
	t.Parallel()

	ref := agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	index := &scopedOnlyPlanIndex{
		ref: ref,
		spec: agentos.RunPlanSpec{
			PlanID:    ref.PlanID,
			AccountID: ref.AccountID,
			ProjectID: ref.ProjectID,
		},
		status: agentos.RunPlanStatus{
			PlanID:         ref.PlanID,
			LifecycleState: agentos.PlanLifecycleRunning,
		},
	}
	rt := &planRuntime{planIndex: index}

	got, err := rt.StatusPlan(t.Context(), ref)
	if err != nil {
		t.Fatalf("StatusPlan: %v", err)
	}

	if got.PlanID != ref.PlanID || got.LifecycleState != agentos.PlanLifecycleRunning {
		t.Fatalf("status = %#v", got)
	}

	if index.planOnlyCalled {
		t.Fatal("StatusPlan used plan-only GetPlan instead of scoped GetPlanByRef")
	}
}

func TestPlanRuntimeStartPlanRequiresTenantScope(t *testing.T) {
	t.Parallel()

	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StartPlan(t.Context(), &agentos.RunPlanSpec{
		PlanID:         "plan-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("StartPlan missing account error = %v, want ErrInvalidPlanScope", err)
	}

	_, err = rt.StartPlan(t.Context(), &agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("StartPlan missing project error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestPlanRuntimeRequiresDurableStoresAtConstruction(t *testing.T) {
	t.Parallel()

	_, err := NewPlanRuntime(t.Context(), &RuntimeConfig{TemporalTaskQueue: "agentos-test"})
	if !errors.Is(err, ErrPlanRuntimePostgresURLRequired) {
		t.Fatalf("NewPlanRuntime missing postgres error = %v, want %v", err, ErrPlanRuntimePostgresURLRequired)
	}

	_, err = newPlanRuntimeWithClient(t.Context(), &RuntimeConfig{TemporalTaskQueue: "agentos-test"}, &fakePlanTemporalClient{}, false)
	if !errors.Is(err, ErrPlanRuntimePostgresURLRequired) {
		t.Fatalf("newPlanRuntimeWithClient missing postgres error = %v, want %v", err, ErrPlanRuntimePostgresURLRequired)
	}

	_, err = newPlanRuntimeWithClient(t.Context(), &RuntimeConfig{
		TemporalTaskQueue: "agentos-test",
		PostgresURL:       testPostgresURL,
	}, &fakePlanTemporalClient{}, false)
	if !errors.Is(err, ErrPlanRuntimeArtifactStoreBackendRequired) {
		t.Fatalf("newPlanRuntimeWithClient missing artifact backend error = %v, want %v", err, ErrPlanRuntimeArtifactStoreBackendRequired)
	}

	_, err = newPlanRuntimeWithClient(t.Context(), &RuntimeConfig{
		TemporalTaskQueue: "agentos-test",
		PostgresURL:       testPostgresURL,
		ArtifactStore: ArtifactStoreConfig{
			Backend: ArtifactStoreBackendLocal,
		},
	}, &fakePlanTemporalClient{}, false)
	if !errors.Is(err, ErrPlanRuntimeArtifactStoreLocalRootRequired) {
		t.Fatalf("newPlanRuntimeWithClient missing local root error = %v, want %v", err, ErrPlanRuntimeArtifactStoreLocalRootRequired)
	}
}

func TestPlanRuntimeNilDurableStoresFailMethods(t *testing.T) {
	t.Parallel()

	rt := &planRuntime{}

	_, err := rt.StartPlan(t.Context(), &agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, errPlanRuntimePlanIndexRequired) {
		t.Fatalf("StartPlan error = %v, want missing durable plan index", err)
	}

	_, err = rt.SubscribePlan(t.Context(), &agentos.PlanStreamScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"})
	if !errors.Is(err, errPlanRuntimePlanEventStoreRequired) {
		t.Fatalf("SubscribePlan error = %v, want missing durable plan event store", err)
	}

	_, err = rt.ListPlanEvents(t.Context(), &agentos.PlanEventScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"})
	if !errors.Is(err, errPlanRuntimePlanEventStoreRequired) {
		t.Fatalf("ListPlanEvents error = %v, want missing durable plan event store", err)
	}
}

func TestPlanRuntimeStartPlanDoesNotOverwriteWorkflowOwnedState(t *testing.T) {
	t.Parallel()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "research",
				Run: agentos.RunSpec{
					RunID:   runResearch,
					Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-agent"},
				},
			},
		},
	}
	workflowStatus := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{
				NodeID:         "research",
				RunID:          runResearch,
				Backend:        agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-agent"},
				LifecycleState: agentos.PlanNodeRunning,
				Attempts:       1,
			},
		},
		UpdatedAt: time.Now().UTC(),
	}
	temporalClient := &fakePlanTemporalClient{
		executeFunc: func(ctx context.Context, _ *client.StartWorkflowOptions, _ any, _ ...any) error {
			return store.SavePlanState(ctx, &agentosplan.PlanStateSnapshot{
				Spec:   spec,
				Status: workflowStatus,
			})
		},
	}
	rt := &planRuntime{temporalClient: temporalClient, taskQueue: "agentos-test", planIndex: store, commandStore: store, auditStore: store}

	if _, err := rt.StartPlan(t.Context(), &spec); err != nil {
		t.Fatalf("StartPlan: %v", err)
	}

	_, got, exists, err := store.GetPlan(t.Context(), spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("GetPlan exists=%v err=%v", exists, err)
	}

	if len(got.Nodes) != 1 ||
		got.Nodes[0].NodeID != RESEARCH ||
		got.Nodes[0].LifecycleState != agentos.PlanNodeRunning ||
		got.Nodes[0].RunID != runResearch {
		t.Fatalf("plan status was overwritten by runtime start path: %#v", got)
	}
}

func TestPlanRuntimeStartPlanDoesNotAuditFailedDelivery(t *testing.T) {
	t.Parallel()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
	}
	temporalClient := &fakePlanTemporalClient{executeErr: errTestTemporalUnavailable}
	rt := &planRuntime{temporalClient: temporalClient, taskQueue: "agentos-test", planIndex: store, commandStore: store, auditStore: store}

	_, err := rt.StartPlan(t.Context(), &spec)
	if err == nil {
		t.Fatal("StartPlan succeeded, want delivery error")
	}

	if temporalClient.executeCount != 1 {
		t.Fatalf("execute count = %d, want 1", temporalClient.executeCount)
	}

	ref := agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
	if _, exists, lookupErr := store.GetAuditRecord(t.Context(), planAuditRef(ref, spec.IdempotencyKey)); lookupErr != nil || exists {
		t.Fatalf("audit exists=%v err=%v, want no audit", exists, lookupErr)
	}

	command, exists, lookupErr := store.GetPlanCommand(t.Context(), planCommandRef(ref, spec.IdempotencyKey))
	if lookupErr != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, lookupErr)
	}

	if command.Status != agentosplan.PlanCommandFailed || command.FailureReason == "" {
		t.Fatalf("command = %#v, want failed with reason", command)
	}
}

func TestPlanRuntimeStartPlanRetriesFailedCommand(t *testing.T) {
	t.Parallel()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
	}
	temporalClient := &fakePlanTemporalClient{executeErr: errTestTemporalUnavailable}
	rt := &planRuntime{temporalClient: temporalClient, taskQueue: "agentos-test", planIndex: store, commandStore: store, auditStore: store}

	if _, err := rt.StartPlan(t.Context(), &spec); err == nil {
		t.Fatal("StartPlan succeeded, want delivery error")
	}

	temporalClient.executeErr = nil

	if _, err := rt.StartPlan(t.Context(), &spec); err != nil {
		t.Fatalf("StartPlan retry: %v", err)
	}

	if temporalClient.executeCount != 2 {
		t.Fatalf("execute count = %d, want retry delivery", temporalClient.executeCount)
	}

	ref := agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}

	command, exists, err := store.GetPlanCommand(t.Context(), planCommandRef(ref, spec.IdempotencyKey))
	if err != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, err)
	}

	if command.Status != agentosplan.PlanCommandDelivered {
		t.Fatalf("command = %#v, want delivered", command)
	}

	if _, exists, err := store.GetAuditRecord(t.Context(), planAuditRef(ref, spec.IdempotencyKey)); err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
}

func TestPlanRuntimeStartPlanSkipsDeliveryWhenCommandDelivered(t *testing.T) {
	t.Parallel()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
	}

	status := agentosplan.NewState(&spec, time.Now().UTC()).Status
	if _, _, err := store.CreatePlan(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	command, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planStartAuditRecord(&spec)))
	if err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	commandAudit := agentosplan.AuditRecordFromPlanCommand(&command)
	if _, _, err := store.RecordAudit(t.Context(), &commandAudit); err != nil {
		t.Fatalf("RecordAudit for command: %v", err)
	}

	if _, err := store.MarkPlanCommandDelivered(t.Context(), agentosplan.PlanCommandRefFromRecord(&command)); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{executeErr: errTestShouldNotStartWorkflow}
	rt := &planRuntime{temporalClient: temporalClient, taskQueue: "agentos-test", planIndex: store, commandStore: store, auditStore: store}

	if _, err := rt.StartPlan(t.Context(), &spec); err != nil {
		t.Fatalf("StartPlan: %v", err)
	}

	if temporalClient.executeCount != 0 {
		t.Fatalf("execute count = %d, want 0", temporalClient.executeCount)
	}

	ref := agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
	if _, exists, err := store.GetAuditRecord(t.Context(), planAuditRef(ref, spec.IdempotencyKey)); err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
}

func TestPlanRuntimeSignalPlanRequiresCommandStore(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	rt := &planRuntime{temporalClient: &fakePlanTemporalClient{}, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, &agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, errPlanRuntimeCommandStoreRequired) {
		t.Fatalf("SignalPlan error = %v, want missing command store", err)
	}
}

func TestPlanRuntimeStatusPlanReportsMissingDurablePlan(t *testing.T) {
	t.Parallel()

	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StatusPlan(t.Context(), agentos.PlanRef{PlanID: "missing-plan", AccountID: "acct-1", ProjectID: "proj-1"})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeStatusPlanRejectsTenantMismatch(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	rt := &planRuntime{planIndex: store}

	ref.AccountID = "acct-other"

	_, err := rt.StatusPlan(t.Context(), ref)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeSubscribePlanEnforcesTenantScope(t *testing.T) {
	t.Parallel()

	store, ref := newPlanRuntimeTestStore(t)
	if _, err := store.AppendPlanEvent(t.Context(), &agentos.PlanEvent{Event: agentos.Event{EventType: agentos.EventPlanStarted}, PlanID: ref.PlanID}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	rt := &planRuntime{planIndex: store, planEvents: store}

	_, err := rt.SubscribePlan(t.Context(), &agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SubscribePlan mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	sub, err := rt.SubscribePlan(t.Context(), &agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID})
	if err != nil {
		t.Fatalf("SubscribePlan scoped: %v", err)
	}
	defer sub.Close()

	event := <-sub.Events()
	if event.EventType != agentos.EventPlanStarted {
		t.Fatalf("event = %#v", event)
	}
}

func TestPlanRuntimeSubscribePlanCatchesUpDurableEventsAfterLiveSubscribe(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	if _, err := store.AppendPlanEvent(t.Context(), &agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: ref.PlanID,
	}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent initial: %v", err)
	}

	live := &catchUpPlanEventSubscriber{
		append: func(ctx context.Context, scope *agentos.PlanStreamScope) error {
			_, err := store.AppendPlanEvent(ctx, &agentos.PlanEvent{
				Event:  agentos.Event{EventType: agentos.EventPlanNodeStarted, RunID: "run-draft"},
				PlanID: scope.PlanID,
				NodeID: "draft",
			}, "event-2")

			return err
		},
	}
	rt := &planRuntime{planIndex: store, planEvents: store, planLiveEvents: live}

	sub, err := rt.SubscribePlan(t.Context(), &agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID})
	if err != nil {
		t.Fatalf("SubscribePlan: %v", err)
	}
	defer sub.Close()

	first := <-sub.Events()

	second := <-sub.Events()
	if first.EventType != agentos.EventPlanStarted || second.EventType != agentos.EventPlanNodeStarted {
		t.Fatalf("events = %#v, %#v", first, second)
	}

	if live.scope.AfterSequence != 1 {
		t.Fatalf("live after sequence = %d, want 1", live.scope.AfterSequence)
	}
}

func TestPlanRuntimeListPlanEventsEnforcesTenantScopeAndFilters(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	events := []agentos.PlanEvent{
		{
			Event:  agentos.Event{EventType: agentos.EventPlanStarted},
			PlanID: ref.PlanID,
		},
		{
			Event:  agentos.Event{EventType: agentos.EventPlanNodeStarted, RunID: runResearch},
			PlanID: ref.PlanID,
			NodeID: "research",
		},
		{
			Event:  agentos.Event{EventType: agentos.EventPlanNodeSucceeded, RunID: runResearch},
			PlanID: ref.PlanID,
			NodeID: "research",
		},
		{
			Event:  agentos.Event{EventType: agentos.EventPlanNodeStarted, RunID: "run-verify"},
			PlanID: ref.PlanID,
			NodeID: "verify",
		},
	}

	keys := []string{"event-1", "event-2", "event-3", "event-4"}
	for i, event := range events {
		if _, err := store.AppendPlanEvent(t.Context(), &event, keys[i]); err != nil {
			t.Fatalf("AppendPlanEvent %d: %v", i, err)
		}
	}

	rt := &planRuntime{planIndex: store, planEvents: store}

	_, err := rt.ListPlanEvents(t.Context(), &agentos.PlanEventScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanEvents mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	got, err := rt.ListPlanEvents(t.Context(), &agentos.PlanEventScope{
		PlanID:        ref.PlanID,
		AccountID:     ref.AccountID,
		ProjectID:     ref.ProjectID,
		NodeID:        "research",
		RunID:         runResearch,
		AfterSequence: 1,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("ListPlanEvents scoped: %v", err)
	}

	if len(got) != 1 || got[0].EventType != agentos.EventPlanNodeStarted || got[0].NodeID != "research" || got[0].RunID != runResearch {
		t.Fatalf("events = %#v", got)
	}
}

func TestPlanRuntimeListPlanDebugTracesProjectsDurableEvents(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	appendDebugTraceEvents(t, store, ref)
	rt := &planRuntime{planIndex: store, planEvents: store}

	_, err := rt.ListPlanDebugTraces(t.Context(), &agentos.PlanDebugTraceScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanDebugTraces mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	traces, err := rt.ListPlanDebugTraces(t.Context(), &agentos.PlanDebugTraceScope{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID, NodeID: "research", RunID: runResearch, AfterSequence: 1, Limit: 1})
	if err != nil {
		t.Fatalf("ListPlanDebugTraces: %v", err)
	}

	assertProjectedDebugTrace(t, traces)
}

func TestPlanRuntimeListPlanAuditsEnforcesTenantScope(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	audit := agentosplan.AuditRecord{
		AuditID:        "audit-1",
		PlanID:         ref.PlanID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
		CreatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	}
	if _, _, err := store.RecordAudit(t.Context(), &audit); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	rt := &planRuntime{planIndex: store, auditStore: store}

	_, err := rt.ListPlanAudits(t.Context(), &agentos.PlanAuditScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanAudits mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	records, err := rt.ListPlanAudits(t.Context(), &agentos.PlanAuditScope{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Action:    agentos.PlanAuditActionControl,
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("ListPlanAudits: %v", err)
	}

	if len(records) != 1 || records[0].AuditID != "audit-1" || records[0].Action != agentos.PlanAuditActionControl {
		t.Fatalf("records = %#v", records)
	}
}

func TestPlanRuntimeListPlanArtifactsEnforcesTenantScopeAndFilters(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	artifactStore := agentosplan.NewMemoryArtifactStore()

	_, err := putArtifact(t.Context(), artifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-research",
		PlanID:     ref.PlanID,
		NodeID:     "research",
		RunID:      runResearch,
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"summary": "ok"}, "artifact-research")
	if err != nil {
		t.Fatalf("Put artifact: %v", err)
	}

	_, err = putArtifact(t.Context(), artifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-other",
		PlanID:     ref.PlanID,
		NodeID:     "verify",
		RunID:      "run-verify",
		Name:       "report",
		Kind:       agentos.ArtifactKindReport,
	}, map[string]any{"report": "ok"}, "artifact-other")
	if err != nil {
		t.Fatalf("Put other artifact: %v", err)
	}

	rt := &planRuntime{planIndex: store, artifactStore: artifactStore}

	_, err = rt.ListPlanArtifacts(t.Context(), &agentos.PlanArtifactScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanArtifacts mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	refs, err := rt.ListPlanArtifacts(t.Context(), &agentos.PlanArtifactScope{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		NodeID:    "research",
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("ListPlanArtifacts scoped: %v", err)
	}

	if len(refs) != 1 || refs[0].ArtifactID != "artifact-research" {
		t.Fatalf("refs = %#v", refs)
	}
}

func TestPlanRuntimeGetPlanArtifactEnforcesPlanOwnership(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	artifactStore := agentosplan.NewMemoryArtifactStore()

	_, err := putArtifact(t.Context(), artifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     ref.PlanID,
		NodeID:     "research",
		RunID:      runResearch,
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"summary": "ok"}, "artifact-1")
	if err != nil {
		t.Fatalf("Put artifact: %v", err)
	}

	_, err = putArtifact(t.Context(), artifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-other-plan",
		PlanID:     "plan-other",
		NodeID:     "research",
		RunID:      "run-other",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"summary": "other"}, "artifact-other-plan")
	if err != nil {
		t.Fatalf("Put other artifact: %v", err)
	}

	rt := &planRuntime{planIndex: store, artifactStore: artifactStore}

	artifact, err := rt.GetPlanArtifact(t.Context(), &agentos.PlanArtifactScope{
		PlanID:     ref.PlanID,
		AccountID:  ref.AccountID,
		ProjectID:  ref.ProjectID,
		ArtifactID: "artifact-1",
	})
	if err != nil {
		t.Fatalf("GetPlanArtifact: %v", err)
	}

	payload, ok := artifact.Payload.(map[string]any)
	if !ok || payload["summary"] != "ok" {
		t.Fatalf("artifact = %#v", artifact)
	}

	_, err = rt.GetPlanArtifact(t.Context(), &agentos.PlanArtifactScope{
		PlanID:     ref.PlanID,
		AccountID:  ref.AccountID,
		ProjectID:  ref.ProjectID,
		ArtifactID: "artifact-other-plan",
	})
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("GetPlanArtifact other plan error = %v, want ErrArtifactNotFound", err)
	}
}

func TestPlanRuntimeSignalPlanDoesNotAuditFailedDelivery(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{signalErr: errTestTemporalUnavailable}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, &agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	})
	if err == nil {
		t.Fatal("SignalPlan succeeded, want delivery error")
	}

	if temporalClient.signalCount != 1 {
		t.Fatalf("signal count = %d, want 1", temporalClient.signalCount)
	}

	if _, exists, lookupErr := store.GetAuditRecord(t.Context(), planAuditRef(ref, "approve-1")); lookupErr != nil || exists {
		t.Fatalf("audit exists=%v err=%v, want no audit", exists, lookupErr)
	}

	command, exists, lookupErr := store.GetPlanCommand(t.Context(), planCommandRef(ref, "approve-1"))
	if lookupErr != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, lookupErr)
	}

	if command.Status != agentosplan.PlanCommandFailed || command.FailureReason == "" {
		t.Fatalf("command = %#v, want failed with reason", command)
	}
}

func TestPlanRuntimeSignalPlanRetriesFailedCommand(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{signalErr: errTestTemporalUnavailable}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}

	if err := rt.SignalPlan(t.Context(), ref, &signal); err == nil {
		t.Fatal("SignalPlan succeeded, want delivery error")
	}

	temporalClient.signalErr = nil

	if err := rt.SignalPlan(t.Context(), ref, &signal); err != nil {
		t.Fatalf("SignalPlan retry: %v", err)
	}

	if temporalClient.signalCount != 2 {
		t.Fatalf("signal count = %d, want retry delivery", temporalClient.signalCount)
	}

	command, exists, err := store.GetPlanCommand(t.Context(), planCommandRef(ref, signal.IdempotencyKey))
	if err != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, err)
	}

	if command.Status != agentosplan.PlanCommandDelivered {
		t.Fatalf("command = %#v, want delivered", command)
	}

	if _, exists, err := store.GetAuditRecord(t.Context(), planAuditRef(ref, signal.IdempotencyKey)); err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
}

func TestPlanRuntimeSignalPlanKeepsCommandRecoverableWhenAuditFails(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{}
	rt := &planRuntime{
		temporalClient: temporalClient,
		commandStore:   store,
		auditStore:     failingAuditStore{AuditStore: store, err: errTestAuditUnavailable},
		planIndex:      store,
	}
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}

	err := rt.SignalPlan(t.Context(), ref, &signal)
	if err == nil {
		t.Fatal("SignalPlan succeeded, want audit error")
	}

	if temporalClient.signalCount != 1 {
		t.Fatalf("signal count = %d, want 1", temporalClient.signalCount)
	}

	command, exists, lookupErr := store.GetPlanCommand(t.Context(), planCommandRef(ref, signal.IdempotencyKey))
	if lookupErr != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, lookupErr)
	}

	if command.Status != agentosplan.PlanCommandPending {
		t.Fatalf("command = %#v, want pending for recovery", command)
	}
}

type failingAuditStore struct {
	agentosplan.AuditStore
	err error
}

func (s failingAuditStore) RecordAudit(context.Context, *agentosplan.AuditRecord) (agentosplan.AuditRecord, bool, error) {
	return agentosplan.AuditRecord{}, false, s.err
}

func TestPlanRuntimeSignalPlanSkipsDeliveryWhenCommandDelivered(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}

	command, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planSignalAuditRecord(ref, &signal)))
	if err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	commandAudit := agentosplan.AuditRecordFromPlanCommand(&command)
	if _, _, err := store.RecordAudit(t.Context(), &commandAudit); err != nil {
		t.Fatalf("RecordAudit for command: %v", err)
	}

	if _, err := store.MarkPlanCommandDelivered(t.Context(), agentosplan.PlanCommandRefFromRecord(&command)); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{signalErr: errTestShouldNotSignal}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	if err := rt.SignalPlan(t.Context(), ref, &signal); err != nil {
		t.Fatalf("SignalPlan: %v", err)
	}

	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}

	if _, exists, err := store.GetAuditRecord(t.Context(), planAuditRef(ref, "approve-1")); err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
}

func TestPlanRuntimeSignalPlanRejectsCommandKeyReuseWithDifferentSignal(t *testing.T) {
	t.Parallel()

	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planSignalAuditRecord(ref, &agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
	}))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{signalErr: errTestShouldNotSignal}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, &agentos.Signal{
		Type:           agentos.SignalPlanReject,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidRunPlan", err)
	}

	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func TestPlanRuntimeSignalPlanRejectsCommandKeyReuseWithDifferentSentAt(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	sentAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planSignalAuditRecord(ref, &agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
		SentAt:         sentAt,
	}))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{signalErr: errTestShouldNotSignal}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, &agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
		SentAt:         sentAt.Add(time.Second),
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidRunPlan", err)
	}

	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func TestPlanRuntimeControlPlanAuditsAfterDelivery(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, &agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "cancel-1",
		ActorID:        "operator-1",
	})
	if err != nil {
		t.Fatalf("ControlPlan: %v", err)
	}

	assertControlCommandDelivered(t, temporalClient, store, ref, "cancel-1")
}

func TestPlanRuntimeControlPlanRejectsCommandKeyReuseWithDifferentControl(t *testing.T) {
	t.Parallel()

	store, ref := newPlanRuntimeTestStore(t)

	audit := agentosplan.AuditRecord{
		PlanID:         "plan-1",
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload: map[string]any{
			"operation": agentos.ControlCancel,
			"metadata":  map[string]string(nil),
		},
	}
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(&audit)); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{signalErr: errTestShouldNotSignal}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, &agentos.ControlRequest{
		Operation:      agentos.ControlPause,
		IdempotencyKey: "control-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ControlPlan error = %v, want ErrInvalidRunPlan", err)
	}

	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func TestPlanRuntimeControlPlanRejectsCommandKeyReuseWithDifferentRequestedAt(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	requestedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planControlAuditRecord(ref, &agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "control-1",
		RequestedAt:    requestedAt,
		ActorID:        "operator-1",
	}))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{signalErr: errTestShouldNotSignal}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, &agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "control-1",
		RequestedAt:    requestedAt.Add(time.Second),
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ControlPlan error = %v, want ErrInvalidRunPlan", err)
	}

	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func newPlanRuntimeTestStore(t *testing.T) (*agentosplan.MemoryPlanStore, agentos.PlanRef) {
	t.Helper()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
	}

	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Now().UTC(),
	}
	if _, _, err := store.CreatePlan(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	return store, agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
}

func planCommandRef(ref agentos.PlanRef, idempotencyKey string) agentosplan.PlanCommandRef {
	return agentosplan.PlanCommandRef{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: idempotencyKey,
	}
}

func planAuditRef(ref agentos.PlanRef, idempotencyKey string) agentosplan.AuditRef {
	return agentosplan.AuditRef{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: idempotencyKey,
	}
}

type scopedOnlyPlanIndex struct {
	ref            agentos.PlanRef
	spec           agentos.RunPlanSpec
	status         agentos.RunPlanStatus
	planOnlyCalled bool
}

func (i *scopedOnlyPlanIndex) CreatePlan(context.Context, *agentos.RunPlanSpec, *agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error) {
	return agentos.RunPlanStatus{}, false, errTestUnexpectedCreatePlan
}

func (i *scopedOnlyPlanIndex) GetPlan(context.Context, string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	i.planOnlyCalled = true

	return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, errTestUnexpectedGetPlan
}

func (i *scopedOnlyPlanIndex) GetPlanByRef(_ context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if ref != i.ref {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
	}

	return i.spec, i.status, true, nil
}

func appendDebugTraceEvents(t *testing.T, store agentosplan.PlanEventStore, ref agentos.PlanRef) {
	t.Helper()

	debugSpec := agentos.RunPlanSpec{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}
	debugStatus := agentos.RunPlanStatus{PlanID: ref.PlanID, LifecycleState: agentos.PlanLifecycleRunning}
	debugEvent := debugTracePlanEvent(t, &debugSpec, &debugStatus)
	events := []agentos.PlanEvent{
		{Event: agentos.Event{EventType: agentos.EventPlanStarted}, PlanID: ref.PlanID},
		{Event: agentos.Event{EventType: agentos.EventPlanNodeStarted, RunID: runResearch}, PlanID: ref.PlanID, NodeID: "research"},
		debugEvent,
	}

	for i := range events {
		if _, err := store.AppendPlanEvent(t.Context(), &events[i], []string{"debug-event-1", "debug-event-2", "debug-event-3"}[i]); err != nil {
			t.Fatalf("AppendPlanEvent %d: %v", i, err)
		}
	}
}

func debugTracePlanEvent(t *testing.T, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) agentos.PlanEvent {
	t.Helper()

	stateEvent := agentosplan.StateEvent{
		Kind:   agentosplan.EventNodeInputResolved,
		NodeID: "research",
		RunID:  runResearch,
		InputTrace: agentosplan.InputResolutionTrace{
			InputDigest:  "digest-1",
			InputKeys:    []string{"topic"},
			MappingCount: 1,
			Mappings:     []agentosplan.InputMappingTrace{{Target: "topic", SourceArtifact: "summary", Required: true}},
		},
		PreviousLifecycleState: agentos.PlanNodeReady,
		NextLifecycleState:     agentos.PlanNodeRunning,
		At:                     time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	}

	event, _, err := agentosplan.PlanEventFromStateEvent(spec, status, &stateEvent)
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}

	return event
}

func assertProjectedDebugTrace(t *testing.T, traces []agentos.PlanDebugTrace) {
	t.Helper()

	if len(traces) != 1 {
		t.Fatalf("traces = %#v, want one trace", traces)
	}

	trace := traces[0]
	if trace.EventType != agentos.EventNodeInputResolved {
		t.Fatalf("trace event type = %q, want %q", trace.EventType, agentos.EventNodeInputResolved)
	}

	if trace.InputResolution == nil || trace.InputResolution.MappingCount != 1 {
		t.Fatalf("trace input resolution = %#v", trace.InputResolution)
	}

	if trace.Transition == nil || trace.Transition.NextLifecycleState != agentos.PlanNodeRunning {
		t.Fatalf("trace transition = %#v", trace.Transition)
	}
}

func assertControlCommandDelivered(t *testing.T, temporalClient *fakePlanTemporalClient, store interface {
	agentosplan.AuditStore
	agentosplan.PlanCommandStore
}, ref agentos.PlanRef, idempotencyKey string,
) {
	t.Helper()

	if temporalClient.signalName != PlanControlSignalName || temporalClient.signalCount != 1 {
		t.Fatalf("signal name=%q count=%d", temporalClient.signalName, temporalClient.signalCount)
	}

	record, exists, err := store.GetAuditRecord(t.Context(), planAuditRef(ref, idempotencyKey))
	if err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}

	if record.ActorID != "operator-1" || record.Action != agentosplan.AuditActionPlanControl {
		t.Fatalf("audit = %#v", record)
	}

	command, exists, err := store.GetPlanCommand(t.Context(), planCommandRef(ref, idempotencyKey))
	if err != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, err)
	}

	if command.Status != agentosplan.PlanCommandDelivered {
		t.Fatalf("command = %#v, want delivered", command)
	}
}

type fakePlanTemporalClient struct {
	executeFunc       func(context.Context, *client.StartWorkflowOptions, any, ...any) error
	executeErr        error
	executeWorkflowID string
	executeTaskQueue  string
	executeCount      int
	signalWorkflowID  string
	signalName        string
	signalPayload     any
	signalCount       int
	signalErr         error
}

func (c *fakePlanTemporalClient) ExecuteWorkflow(ctx context.Context, options *client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	c.executeWorkflowID = options.ID
	c.executeTaskQueue = options.TaskQueue

	c.executeCount++
	if c.executeFunc != nil {
		if err := c.executeFunc(ctx, options, workflow, args...); err != nil {
			return nil, err
		}
	}

	return nil, c.executeErr
}

func (c *fakePlanTemporalClient) SignalWorkflow(_ context.Context, workflowID, _, signalName string, arg any) error {
	c.signalWorkflowID = workflowID
	c.signalName = signalName
	c.signalPayload = arg
	c.signalCount++

	return c.signalErr
}

func (c *fakePlanTemporalClient) Close() {}

type catchUpPlanEventSubscriber struct {
	scope  *agentos.PlanStreamScope
	append func(context.Context, *agentos.PlanStreamScope) error
}

func (s *catchUpPlanEventSubscriber) SubscribePlanEvents(ctx context.Context, scope *agentos.PlanStreamScope) (agentosplan.PlanEventSubscription, error) {
	s.scope = scope
	if s.append != nil {
		if err := s.append(ctx, scope); err != nil {
			return nil, err
		}
	}

	return &fakePlanEventSubscription{events: make(chan agentos.PlanEvent)}, nil
}
