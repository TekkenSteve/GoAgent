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

func TestPlanRuntimeSignalPlanValidatesSignalBeforeAudit(t *testing.T) {
	rt := &planRuntime{}
	ref := agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1"}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "message-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidSignal", err)
	}

	err = rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalPlanNodeRetry,
		IdempotencyKey: "retry-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan retry error = %v, want ErrInvalidSignal", err)
	}
}

func TestPlanRuntimeStatusPlanReadsDurableIndex(t *testing.T) {
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
	if _, _, err := store.CreatePlan(t.Context(), spec, status); err != nil {
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

func TestPlanRuntimeStartPlanRequiresAccountScope(t *testing.T) {
	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StartPlan(t.Context(), agentos.RunPlanSpec{
		PlanID:         "plan-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("StartPlan error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestPlanRuntimeRequiresDurableStoresAtConstruction(t *testing.T) {
	_, err := NewPlanRuntime(t.Context(), RuntimeConfig{TemporalTaskQueue: "agentos-test"})
	if !errors.Is(err, ErrPlanRuntimePostgresURLRequired) {
		t.Fatalf("NewPlanRuntime missing postgres error = %v, want %v", err, ErrPlanRuntimePostgresURLRequired)
	}

	_, err = newPlanRuntimeWithClient(t.Context(), RuntimeConfig{TemporalTaskQueue: "agentos-test"}, &fakePlanTemporalClient{}, false)
	if !errors.Is(err, ErrPlanRuntimePostgresURLRequired) {
		t.Fatalf("newPlanRuntimeWithClient missing postgres error = %v, want %v", err, ErrPlanRuntimePostgresURLRequired)
	}

	_, err = newPlanRuntimeWithClient(t.Context(), RuntimeConfig{
		TemporalTaskQueue: "agentos-test",
		PostgresURL:       "postgres://user:pass@localhost:5432/db",
	}, &fakePlanTemporalClient{}, false)
	if !errors.Is(err, ErrPlanRuntimeArtifactStoreBackendRequired) {
		t.Fatalf("newPlanRuntimeWithClient missing artifact backend error = %v, want %v", err, ErrPlanRuntimeArtifactStoreBackendRequired)
	}

	_, err = newPlanRuntimeWithClient(t.Context(), RuntimeConfig{
		TemporalTaskQueue: "agentos-test",
		PostgresURL:       "postgres://user:pass@localhost:5432/db",
		ArtifactStore: ArtifactStoreConfig{
			Backend: ArtifactStoreBackendLocal,
		},
	}, &fakePlanTemporalClient{}, false)
	if !errors.Is(err, ErrPlanRuntimeArtifactStoreLocalRootRequired) {
		t.Fatalf("newPlanRuntimeWithClient missing local root error = %v, want %v", err, ErrPlanRuntimeArtifactStoreLocalRootRequired)
	}
}

func TestPlanRuntimeNilDurableStoresFailMethods(t *testing.T) {
	rt := &planRuntime{}

	_, err := rt.StartPlan(t.Context(), agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, errPlanRuntimePlanIndexRequired) {
		t.Fatalf("StartPlan error = %v, want missing durable plan index", err)
	}
	_, err = rt.SubscribePlan(t.Context(), agentos.PlanStreamScope{PlanID: "plan-1", AccountID: "acct-1"})
	if !errors.Is(err, errPlanRuntimePlanEventStoreRequired) {
		t.Fatalf("SubscribePlan error = %v, want missing durable plan event store", err)
	}
	_, err = rt.ListPlanEvents(t.Context(), agentos.PlanEventScope{PlanID: "plan-1", AccountID: "acct-1"})
	if !errors.Is(err, errPlanRuntimePlanEventStoreRequired) {
		t.Fatalf("ListPlanEvents error = %v, want missing durable plan event store", err)
	}
}

func TestPlanRuntimeSignalPlanRequiresCommandStore(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	rt := &planRuntime{temporalClient: &fakePlanTemporalClient{}, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, errPlanRuntimeCommandStoreRequired) {
		t.Fatalf("SignalPlan error = %v, want missing command store", err)
	}
}

func TestPlanRuntimeStatusPlanReportsMissingDurablePlan(t *testing.T) {
	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StatusPlan(t.Context(), agentos.PlanRef{PlanID: "missing-plan", AccountID: "acct-1"})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeStatusPlanRejectsTenantMismatch(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	rt := &planRuntime{planIndex: store}

	ref.AccountID = "acct-other"
	_, err := rt.StatusPlan(t.Context(), ref)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeSubscribePlanEnforcesTenantScope(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, err := store.AppendPlanEvent(t.Context(), agentos.PlanEvent{Event: agentos.Event{EventType: agentos.EventPlanStarted}, PlanID: ref.PlanID}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}
	rt := &planRuntime{planIndex: store, planEvents: store}

	_, err := rt.SubscribePlan(t.Context(), agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SubscribePlan mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	sub, err := rt.SubscribePlan(t.Context(), agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID})
	if err != nil {
		t.Fatalf("SubscribePlan scoped: %v", err)
	}
	defer sub.Close()
	event := <-sub.Events()
	if event.EventType != agentos.EventPlanStarted {
		t.Fatalf("event = %#v", event)
	}
}

func TestPlanRuntimeListPlanEventsEnforcesTenantScopeAndFilters(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	events := []agentos.PlanEvent{
		{
			Event:  agentos.Event{EventType: agentos.EventPlanStarted},
			PlanID: ref.PlanID,
		},
		{
			Event:  agentos.Event{EventType: agentos.EventPlanNodeStarted, RunID: "run-research"},
			PlanID: ref.PlanID,
			NodeID: "research",
		},
		{
			Event:  agentos.Event{EventType: agentos.EventPlanNodeSucceeded, RunID: "run-research"},
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
		if _, err := store.AppendPlanEvent(t.Context(), event, keys[i]); err != nil {
			t.Fatalf("AppendPlanEvent %d: %v", i, err)
		}
	}
	rt := &planRuntime{planIndex: store, planEvents: store}

	_, err := rt.ListPlanEvents(t.Context(), agentos.PlanEventScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanEvents mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	got, err := rt.ListPlanEvents(t.Context(), agentos.PlanEventScope{
		PlanID:        ref.PlanID,
		AccountID:     ref.AccountID,
		ProjectID:     ref.ProjectID,
		NodeID:        "research",
		RunID:         "run-research",
		AfterSequence: 1,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("ListPlanEvents scoped: %v", err)
	}
	if len(got) != 1 || got[0].EventType != agentos.EventPlanNodeStarted || got[0].NodeID != "research" || got[0].RunID != "run-research" {
		t.Fatalf("events = %#v", got)
	}
}

func TestPlanRuntimeListPlanAuditsEnforcesTenantScope(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := store.RecordAudit(t.Context(), agentosplan.AuditRecord{
		AuditID:        "audit-1",
		PlanID:         ref.PlanID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
		CreatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	rt := &planRuntime{planIndex: store, auditStore: store}

	_, err := rt.ListPlanAudits(t.Context(), agentos.PlanAuditScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanAudits mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	records, err := rt.ListPlanAudits(t.Context(), agentos.PlanAuditScope{
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
	store, ref := newPlanRuntimeTestStore(t)
	artifactStore := agentosplan.NewMemoryArtifactStore()
	_, err := artifactStore.Put(t.Context(), agentos.ArtifactRef{
		ArtifactID: "artifact-research",
		PlanID:     ref.PlanID,
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"summary": "ok"}, "artifact-research")
	if err != nil {
		t.Fatalf("Put artifact: %v", err)
	}
	_, err = artifactStore.Put(t.Context(), agentos.ArtifactRef{
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
	_, err = rt.ListPlanArtifacts(t.Context(), agentos.PlanArtifactScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanArtifacts mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	refs, err := rt.ListPlanArtifacts(t.Context(), agentos.PlanArtifactScope{
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
	store, ref := newPlanRuntimeTestStore(t)
	artifactStore := agentosplan.NewMemoryArtifactStore()
	_, err := artifactStore.Put(t.Context(), agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     ref.PlanID,
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"summary": "ok"}, "artifact-1")
	if err != nil {
		t.Fatalf("Put artifact: %v", err)
	}
	_, err = artifactStore.Put(t.Context(), agentos.ArtifactRef{
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
	artifact, err := rt.GetPlanArtifact(t.Context(), agentos.PlanArtifactScope{
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

	_, err = rt.GetPlanArtifact(t.Context(), agentos.PlanArtifactScope{
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
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("temporal unavailable")}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
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
	if _, exists, lookupErr := store.GetAuditRecord(t.Context(), "approve-1"); lookupErr != nil || exists {
		t.Fatalf("audit exists=%v err=%v, want no audit", exists, lookupErr)
	}
	command, exists, lookupErr := store.GetPlanCommand(t.Context(), "approve-1")
	if lookupErr != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, lookupErr)
	}
	if command.Status != agentosplan.PlanCommandFailed || command.FailureReason == "" {
		t.Fatalf("command = %#v, want failed with reason", command)
	}
}

func TestPlanRuntimeSignalPlanRetriesFailedCommand(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("temporal unavailable")}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}

	if err := rt.SignalPlan(t.Context(), ref, signal); err == nil {
		t.Fatal("SignalPlan succeeded, want delivery error")
	}
	temporalClient.signalErr = nil
	if err := rt.SignalPlan(t.Context(), ref, signal); err != nil {
		t.Fatalf("SignalPlan retry: %v", err)
	}

	if temporalClient.signalCount != 2 {
		t.Fatalf("signal count = %d, want retry delivery", temporalClient.signalCount)
	}
	command, exists, err := store.GetPlanCommand(t.Context(), signal.IdempotencyKey)
	if err != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, err)
	}
	if command.Status != agentosplan.PlanCommandDelivered {
		t.Fatalf("command = %#v, want delivered", command)
	}
	if _, exists, err := store.GetAuditRecord(t.Context(), signal.IdempotencyKey); err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
}

func TestPlanRuntimeSignalPlanSkipsDeliveryWhenCommandDelivered(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}
	command, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(planSignalAuditRecord("plan-1", signal)))
	if err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}
	if _, err := store.MarkPlanCommandDelivered(t.Context(), command.IdempotencyKey); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	if err := rt.SignalPlan(t.Context(), ref, signal); err != nil {
		t.Fatalf("SignalPlan: %v", err)
	}
	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
	if _, exists, err := store.GetAuditRecord(t.Context(), "approve-1"); err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
}

func TestPlanRuntimeSignalPlanRejectsCommandKeyReuseWithDifferentSignal(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(planSignalAuditRecord("plan-1", agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
	}))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
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

func TestPlanRuntimeControlPlanAuditsAfterDelivery(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "cancel-1",
		ActorID:        "operator-1",
	})
	if err != nil {
		t.Fatalf("ControlPlan: %v", err)
	}
	if temporalClient.signalName != PlanControlSignalName || temporalClient.signalCount != 1 {
		t.Fatalf("signal name=%q count=%d", temporalClient.signalName, temporalClient.signalCount)
	}
	record, exists, err := store.GetAuditRecord(t.Context(), "cancel-1")
	if err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
	if record.ActorID != "operator-1" || record.Action != agentosplan.AuditActionPlanControl {
		t.Fatalf("audit = %#v", record)
	}
	command, exists, err := store.GetPlanCommand(t.Context(), "cancel-1")
	if err != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, err)
	}
	if command.Status != agentosplan.PlanCommandDelivered {
		t.Fatalf("command = %#v, want delivered", command)
	}
}

func TestPlanRuntimeControlPlanRejectsCommandKeyReuseWithDifferentControl(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(agentosplan.AuditRecord{
		PlanID:         "plan-1",
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload: map[string]any{
			"operation": agentos.ControlCancel,
			"metadata":  map[string]string(nil),
		},
	})); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, agentos.ControlRequest{
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
	if _, _, err := store.CreatePlan(t.Context(), spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	return store, agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
}

type fakePlanTemporalClient struct {
	signalWorkflowID string
	signalName       string
	signalPayload    any
	signalCount      int
	signalErr        error
}

func (c *fakePlanTemporalClient) ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
	return nil, nil
}

func (c *fakePlanTemporalClient) SignalWorkflow(_ context.Context, workflowID string, _ string, signalName string, arg interface{}) error {
	c.signalWorkflowID = workflowID
	c.signalName = signalName
	c.signalPayload = arg
	c.signalCount++

	return c.signalErr
}

func (c *fakePlanTemporalClient) Close() {}
