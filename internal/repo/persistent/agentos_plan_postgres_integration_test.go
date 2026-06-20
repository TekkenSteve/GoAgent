//go:build postgres_integration

package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	artifactblob "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func TestAgentOSPlanPostgresDurablePersistence(t *testing.T) {
	pgURL := os.Getenv("GOAGENT_POSTGRES_TEST_URL")
	if pgURL == "" {
		t.Fatal("GOAGENT_POSTGRES_TEST_URL is required for postgres_integration tests")
	}

	pg, err := postgres.New(pgURL, postgres.MaxPoolSize(1), postgres.ConnAttempts(1), postgres.ConnTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	defer pg.Close()
	waitForPostgres(t, pg)
	applyAgentOSPlanMigrations(t, pg)

	ctx := t.Context()
	planRepo := NewAgentOSPlanRepo(pg)
	routeIndex := NewRunBackendIndexRepo(pg)
	blobStore, err := artifactblob.NewLocalBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBlobStore: %v", err)
	}
	artifactStore := NewAgentOSArtifactRepo(pg, blobStore)
	capabilityCatalog := NewAgentOSCapabilityCatalogRepo(pg)
	artifactSchemaCatalog := NewAgentOSArtifactSchemaCatalogRepo(pg)

	suffix := time.Now().UTC().Format("20060102150405.000000000")
	spec := postgresIntegrationPlanSpec("plan-"+suffix, "plan-start-"+suffix)
	status := agentosplan.NewState(spec, time.Now().UTC()).Status

	first, created, err := planRepo.CreatePlan(ctx, spec, status)
	if err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}
	if !created {
		t.Fatal("CreatePlan first was not created")
	}
	second, created, err := planRepo.CreatePlan(ctx, spec, agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleFailed})
	if err != nil {
		t.Fatalf("CreatePlan replay: %v", err)
	}
	if created || second.LifecycleState != first.LifecycleState {
		t.Fatalf("CreatePlan replay = %#v created=%v, want %#v created=false", second, created, first)
	}

	changedSpec := spec
	changedSpec.Nodes[0].NodeID = "changed"
	if _, _, err := planRepo.CreatePlan(ctx, changedSpec, status); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("CreatePlan changed spec error = %v, want ErrInvalidRunPlan", err)
	}
	if err := planRepo.SavePlanState(ctx, agentosplan.PlanStateSnapshot{Spec: changedSpec, Status: status}); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SavePlanState changed spec error = %v, want ErrInvalidRunPlan", err)
	}
	nodeStatus := status
	nodeStatus.Nodes = append([]agentos.PlanNodeStatus(nil), status.Nodes...)
	nodeStatus.Nodes[0].LifecycleState = agentos.PlanNodeSucceeded
	nodeStatus.Nodes[0].Reason = "node table source"
	if err := planRepo.SavePlanState(ctx, agentosplan.PlanStateSnapshot{
		Spec:           spec,
		Status:         nodeStatus,
		IdempotencyKey: spec.IdempotencyKey,
	}); err != nil {
		t.Fatalf("SavePlanState node source: %v", err)
	}
	contradictoryStatus := nodeStatus
	contradictoryStatus.Nodes = append([]agentos.PlanNodeStatus(nil), nodeStatus.Nodes...)
	contradictoryStatus.Nodes[0].LifecycleState = agentos.PlanNodeFailed
	contradictoryStatus.Nodes[0].Reason = "stale aggregate json"
	contradictoryStatusJSON, err := json.Marshal(contradictoryStatus)
	if err != nil {
		t.Fatalf("marshal contradictory status: %v", err)
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plans SET status_json = $2 WHERE plan_id = $1`, spec.PlanID, contradictoryStatusJSON); err != nil {
		t.Fatalf("corrupt aggregate node status: %v", err)
	}
	loadedSnapshot, exists, err := planRepo.LoadPlanState(ctx, spec.PlanID)
	if err != nil {
		t.Fatalf("LoadPlanState: %v", err)
	}
	if !exists {
		t.Fatal("LoadPlanState did not find plan")
	}
	if len(loadedSnapshot.Status.Nodes) != 1 ||
		loadedSnapshot.Status.Nodes[0].LifecycleState != agentos.PlanNodeSucceeded ||
		loadedSnapshot.Status.Nodes[0].Reason != "node table source" {
		t.Fatalf("LoadPlanState nodes = %#v, want durable plan_nodes source", loadedSnapshot.Status.Nodes)
	}

	event := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Timestamp: time.Now().UTC(),
			Payload:   map[string]any{"source": "postgres-integration"},
		},
		PlanID: spec.PlanID,
	}
	firstEvent, err := planRepo.AppendPlanEvent(ctx, event, "plan-event-"+suffix)
	if err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}
	secondEvent, err := planRepo.AppendPlanEvent(ctx, event, "plan-event-"+suffix)
	if err != nil {
		t.Fatalf("AppendPlanEvent replay: %v", err)
	}
	if secondEvent.EventID != firstEvent.EventID || secondEvent.Sequence != firstEvent.Sequence {
		t.Fatalf("AppendPlanEvent replay = %#v, want %#v", secondEvent, firstEvent)
	}
	events, err := planRepo.ListPlanEvents(ctx, postgresIntegrationPlanStreamScope(spec), 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventID != firstEvent.EventID {
		t.Fatalf("ListPlanEvents = %#v", events)
	}
	assertPostgresPlanEventScope(t, pg, firstEvent.EventID, spec.AccountID, spec.ProjectID)
	if _, err := planRepo.ListPlanEvents(ctx, agentos.PlanStreamScope{
		PlanID:    spec.PlanID,
		AccountID: "acct-other",
		ProjectID: spec.ProjectID,
	}, 0); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanEvents mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	audit, created, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "audit-" + suffix,
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	})
	if err != nil {
		t.Fatalf("RecordAudit first: %v", err)
	}
	if !created {
		t.Fatal("RecordAudit first was not created")
	}
	auditReplay, created, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "audit-" + suffix,
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	})
	if err != nil {
		t.Fatalf("RecordAudit replay: %v", err)
	}
	if created || auditReplay.AuditID != audit.AuditID {
		t.Fatalf("RecordAudit replay = %#v created=%v, want %#v created=false", auditReplay, created, audit)
	}
	audits, err := planRepo.ListAuditRecords(ctx, agentos.PlanAuditScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Action:    agentos.PlanAuditActionControl,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}
	if len(audits) != 1 || audits[0].AuditID != audit.AuditID || audits[0].Action != agentos.PlanAuditActionControl {
		t.Fatalf("ListAuditRecords = %#v", audits)
	}
	if _, err := planRepo.ListAuditRecords(ctx, agentos.PlanAuditScope{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListAuditRecords mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	command, created, err := planRepo.RecordPlanCommand(ctx, agentosplan.PlanCommandRecord{
		PlanID:         spec.PlanID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "command-" + suffix,
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	})
	if err != nil {
		t.Fatalf("RecordPlanCommand first: %v", err)
	}
	if !created || command.Status != agentosplan.PlanCommandPending {
		t.Fatalf("RecordPlanCommand first = %#v created=%v", command, created)
	}
	commandReplay, created, err := planRepo.RecordPlanCommand(ctx, agentosplan.PlanCommandRecord{
		PlanID:         spec.PlanID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "command-" + suffix,
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	})
	if err != nil {
		t.Fatalf("RecordPlanCommand replay: %v", err)
	}
	if created || commandReplay.CommandID != command.CommandID {
		t.Fatalf("RecordPlanCommand replay = %#v created=%v, want %#v created=false", commandReplay, created, command)
	}
	deliveredCommand, err := planRepo.MarkPlanCommandDelivered(ctx, agentosplan.PlanCommandRefFromRecord(command))
	if err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	if deliveredCommand.Status != agentosplan.PlanCommandDelivered || deliveredCommand.FailureReason != "" {
		t.Fatalf("delivered command = %#v", deliveredCommand)
	}
	failedCommand, created, err := planRepo.RecordPlanCommand(ctx, agentosplan.PlanCommandRecord{
		PlanID:         spec.PlanID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "failed-command-" + suffix,
		Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
	})
	if err != nil {
		t.Fatalf("RecordPlanCommand failed command: %v", err)
	}
	if !created {
		t.Fatal("failed command was not created")
	}
	if _, err := planRepo.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(failedCommand), "temporal unavailable"); err != nil {
		t.Fatalf("MarkPlanCommandFailed: %v", err)
	}
	recoverableCommands, err := planRepo.ListRecoverablePlanCommands(ctx, agentosplan.PlanCommandScope{
		PlanID:   spec.PlanID,
		Statuses: []agentosplan.PlanCommandStatus{agentosplan.PlanCommandFailed},
	})
	if err != nil {
		t.Fatalf("ListRecoverablePlanCommands: %v", err)
	}
	if len(recoverableCommands) != 1 || recoverableCommands[0].CommandID != failedCommand.CommandID {
		t.Fatalf("recoverable commands = %#v", recoverableCommands)
	}

	runSpec := spec.Nodes[0].Run
	runSpec.IdempotencyKey, err = agentosplan.NodeStartIdempotencyKey(spec.PlanID, spec.Nodes[0].NodeID, 1)
	if err != nil {
		t.Fatalf("NodeStartIdempotencyKey: %v", err)
	}
	runStatus := agentos.RunStatus{RunID: runSpec.RunID, LifecycleState: "running"}
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, runSpec, runStatus); err != nil {
		t.Fatalf("BindPlanNode first: %v", err)
	}
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, runSpec, runStatus); err != nil {
		t.Fatalf("BindPlanNode replay: %v", err)
	}
	assertPostgresRunBackendIndexRecord(t, pg, runSpec.RunID, postgresRunBackendIndexExpectation{
		PlanID:         spec.PlanID,
		NodeID:         spec.Nodes[0].NodeID,
		ThreadID:       runSpec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		BackendKind:    string(runSpec.Backend.Kind),
		BackendName:    runSpec.Backend.Name,
		IdempotencyKey: runSpec.IdempotencyKey,
		LifecycleState: runStatus.LifecycleState,
	})
	resolved, err := routeIndex.Resolve(ctx, runSpec.RunID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved != runSpec.Backend {
		t.Fatalf("Resolve = %#v, want %#v", resolved, runSpec.Backend)
	}
	changedRun := runSpec
	changedRun.RunID = runSpec.RunID + "-changed"
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, changedRun, agentos.RunStatus{RunID: changedRun.RunID}); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("BindPlanNode changed run error = %v, want ErrInvalidRunSpec", err)
	}
	changedKeyRun := runSpec
	changedKeyRun.IdempotencyKey = runSpec.IdempotencyKey + "-changed"
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, changedKeyRun, runStatus); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("BindPlanNode changed idempotency key error = %v, want ErrInvalidRunSpec", err)
	}
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, agentos.RunSpec{
		RunID:     "missing-node-start-key-" + suffix,
		ThreadID:  runSpec.ThreadID,
		AccountID: runSpec.AccountID,
		ProjectID: runSpec.ProjectID,
		Backend:   runSpec.Backend,
	}, agentos.RunStatus{RunID: "missing-node-start-key-" + suffix}); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("BindPlanNode missing idempotency key error = %v, want ErrInvalidRunSpec", err)
	}
	reusedNodeKey := runSpec
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, "node-2", reusedNodeKey, runStatus); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("BindPlanNode reused node key error = %v, want ErrInvalidRunPlan", err)
	}
	standaloneRun := agentos.RunSpec{
		RunID:          "standalone-" + suffix,
		Backend:        runSpec.Backend,
		IdempotencyKey: "standalone-run-start-" + suffix,
	}
	if err := routeIndex.Bind(ctx, standaloneRun); err != nil {
		t.Fatalf("Bind standalone first: %v", err)
	}
	if err := routeIndex.Bind(ctx, standaloneRun); err != nil {
		t.Fatalf("Bind standalone replay: %v", err)
	}
	standaloneChangedBackend := standaloneRun
	standaloneChangedBackend.Backend = agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "other-backend"}
	if err := routeIndex.Bind(ctx, standaloneChangedBackend); !errors.Is(err, agentos.ErrInvalidBackendRef) {
		t.Fatalf("Bind standalone changed backend error = %v, want ErrInvalidBackendRef", err)
	}

	artifactRef := agentos.ArtifactRef{
		PlanID:    spec.PlanID,
		NodeID:    spec.Nodes[0].NodeID,
		RunID:     runSpec.RunID,
		Name:      "summary",
		Kind:      agentos.ArtifactKindObject,
		MediaType: "application/json",
	}
	storedArtifact, err := artifactStore.Put(ctx, artifactRef, map[string]any{"summary": "ok"}, "artifact-"+suffix)
	if err != nil {
		t.Fatalf("Artifact Put first: %v", err)
	}
	replayedArtifact, err := artifactStore.Put(ctx, artifactRef, map[string]any{"summary": "ok"}, "artifact-"+suffix)
	if err != nil {
		t.Fatalf("Artifact Put replay: %v", err)
	}
	if replayedArtifact.ArtifactID != storedArtifact.ArtifactID {
		t.Fatalf("Artifact replay = %#v, want %#v", replayedArtifact, storedArtifact)
	}
	loadedArtifact, payload, err := artifactStore.Get(ctx, agentos.PlanArtifactScope{
		PlanID:     spec.PlanID,
		AccountID:  spec.AccountID,
		ProjectID:  spec.ProjectID,
		ArtifactID: storedArtifact.ArtifactID,
	})
	if err != nil {
		t.Fatalf("Artifact Get: %v", err)
	}
	if loadedArtifact.PlanID != spec.PlanID || payload == nil {
		t.Fatalf("Artifact Get = %#v payload=%#v", loadedArtifact, payload)
	}

	capability := agentos.Capability{
		Backend:  spec.Nodes[0].Run.Backend,
		Name:     "run",
		Controls: []agentos.ControlOperation{agentos.ControlCancel},
	}
	capabilityKey, err := agentosplan.CapabilityRegistrationIdempotencyKey(capability)
	if err != nil {
		t.Fatalf("CapabilityRegistrationIdempotencyKey: %v", err)
	}
	registered, created, err := capabilityCatalog.RegisterCapability(ctx, capability, capabilityKey)
	if err != nil {
		t.Fatalf("RegisterCapability first: %v", err)
	}
	if !created {
		t.Fatal("RegisterCapability first was not created")
	}
	replayed, created, err := capabilityCatalog.RegisterCapability(ctx, capability, capabilityKey)
	if err != nil {
		t.Fatalf("RegisterCapability replay: %v", err)
	}
	if created || replayed.Name != registered.Name {
		t.Fatalf("RegisterCapability replay = %#v created=%v, want %#v created=false", replayed, created, registered)
	}
	loadedCapability, ok, err := capabilityCatalog.GetCapability(ctx, capability.Backend, capability.Name)
	if err != nil {
		t.Fatalf("GetCapability: %v", err)
	}
	if !ok || len(loadedCapability.Controls) != 1 || loadedCapability.Controls[0] != agentos.ControlCancel {
		t.Fatalf("GetCapability = %#v ok=%v", loadedCapability, ok)
	}
	changedCapability := capability
	changedCapability.Description = "changed"
	changedKey, err := agentosplan.CapabilityRegistrationIdempotencyKey(changedCapability)
	if err != nil {
		t.Fatalf("CapabilityRegistrationIdempotencyKey changed: %v", err)
	}
	if _, _, err := capabilityCatalog.RegisterCapability(ctx, changedCapability, changedKey); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RegisterCapability changed error = %v, want ErrInvalidRunPlan", err)
	}

	artifactSchema := agentos.ArtifactSchema{
		Ref:         "schema:summary:" + suffix,
		Description: "Summary artifact",
		Schema:      json.RawMessage(`{"type":"object","required":["summary"]}`),
	}
	artifactSchemaKey, err := agentosplan.ArtifactSchemaRegistrationIdempotencyKey(artifactSchema)
	if err != nil {
		t.Fatalf("ArtifactSchemaRegistrationIdempotencyKey: %v", err)
	}
	registeredSchema, created, err := artifactSchemaCatalog.RegisterArtifactSchema(ctx, artifactSchema, artifactSchemaKey)
	if err != nil {
		t.Fatalf("RegisterArtifactSchema first: %v", err)
	}
	if !created || registeredSchema.Ref != artifactSchema.Ref {
		t.Fatalf("RegisterArtifactSchema first = %#v created=%v", registeredSchema, created)
	}
	replayedSchema, created, err := artifactSchemaCatalog.RegisterArtifactSchema(ctx, artifactSchema, artifactSchemaKey)
	if err != nil {
		t.Fatalf("RegisterArtifactSchema replay: %v", err)
	}
	if created || replayedSchema.Ref != artifactSchema.Ref {
		t.Fatalf("RegisterArtifactSchema replay = %#v created=%v", replayedSchema, created)
	}
	loadedSchema, ok, err := artifactSchemaCatalog.GetArtifactSchema(ctx, artifactSchema.Ref)
	if err != nil {
		t.Fatalf("GetArtifactSchema: %v", err)
	}
	if !ok || len(loadedSchema) == 0 {
		t.Fatalf("GetArtifactSchema = %s ok=%v", string(loadedSchema), ok)
	}
	changedSchema := artifactSchema
	changedSchema.Schema = json.RawMessage(`{"type":"object","required":["title"]}`)
	changedSchemaKey, err := agentosplan.ArtifactSchemaRegistrationIdempotencyKey(changedSchema)
	if err != nil {
		t.Fatalf("ArtifactSchemaRegistrationIdempotencyKey changed: %v", err)
	}
	if _, _, err := artifactSchemaCatalog.RegisterArtifactSchema(ctx, changedSchema, changedSchemaKey); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("RegisterArtifactSchema changed error = %v, want ErrInvalidArtifact", err)
	}
}

func TestAgentOSPlanPostgresSavePlanStateRequiresIdempotencyKey(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)

	err := planRepo.SavePlanState(ctx, agentosplan.PlanStateSnapshot{
		Spec:   agentos.RunPlanSpec{PlanID: "plan-state-key-required-" + suffix},
		Status: agentos.RunPlanStatus{PlanID: "plan-state-key-required-" + suffix},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SavePlanState empty key error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestAgentOSPlanPostgresPlanEventIdempotencyDoesNotAdvanceSequence(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-event-"+suffix, "plan-event-start-"+suffix)
	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	if _, _, err := planRepo.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	event := agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "caller-event-" + suffix,
			EventType: agentos.EventPlanStarted,
			Sequence:  99,
			Timestamp: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
			Payload:   map[string]any{"state": "started"},
		},
		PlanID: spec.PlanID,
	}
	first, err := planRepo.AppendPlanEvent(ctx, event, "plan-event-key-"+suffix)
	if err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}
	replay, err := planRepo.AppendPlanEvent(ctx, event, "plan-event-key-"+suffix)
	if err != nil {
		t.Fatalf("AppendPlanEvent replay: %v", err)
	}
	if first.EventID != spec.PlanID+":1" || first.Sequence != 1 {
		t.Fatalf("first event = %#v, want store-owned identity %s:1", first, spec.PlanID)
	}
	if replay.EventID != first.EventID || replay.Sequence != 1 {
		t.Fatalf("replay = %#v, want first event at sequence 1", replay)
	}

	changed := event
	changed.EventType = agentos.EventPlanFailed
	changed.Payload = map[string]any{"state": "failed"}
	if _, err := planRepo.AppendPlanEvent(ctx, changed, "plan-event-key-"+suffix); !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent changed replay error = %v, want ErrInvalidPlanEvent", err)
	}

	next, err := planRepo.AppendPlanEvent(ctx, agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanSucceeded,
			Timestamp: time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC),
			Payload:   map[string]any{"state": "succeeded"},
		},
		PlanID: spec.PlanID,
	}, "plan-event-next-"+suffix)
	if err != nil {
		t.Fatalf("AppendPlanEvent next: %v", err)
	}
	if next.Sequence != 2 {
		t.Fatalf("next sequence = %d, want 2 after failed replay", next.Sequence)
	}
	events, err := planRepo.ListPlanEvents(ctx, postgresIntegrationPlanStreamScope(spec), 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %#v, want exactly first and next event", events)
	}
}

func TestAgentOSPlanPostgresAppendPlanEventRequiresIdempotencyKey(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-event-key-required-"+suffix, "plan-event-start-key-required-"+suffix)
	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	if _, _, err := planRepo.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	_, err := planRepo.AppendPlanEvent(ctx, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: spec.PlanID,
	}, "")
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent empty key error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestAgentOSPlanPostgresPlanRefsAndMetricCheckpoints(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-metrics-"+suffix, "plan-metrics-start-"+suffix)
	status := agentosplan.NewState(spec, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)).Status
	status.UpdatedAt = time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC)
	if _, _, err := planRepo.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	refs, err := planRepo.ListPlanRefs(ctx, agentosplan.PlanRefScope{
		AccountID:       spec.AccountID,
		ProjectID:       spec.ProjectID,
		LifecycleStates: []string{agentos.PlanLifecyclePending},
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("ListPlanRefs: %v", err)
	}
	if len(refs) != 1 || refs[0].PlanID != spec.PlanID || refs[0].AccountID != spec.AccountID || refs[0].ProjectID != spec.ProjectID {
		t.Fatalf("ListPlanRefs = %#v, want created plan ref", refs)
	}
	if _, err := planRepo.ListPlanRefs(ctx, agentosplan.PlanRefScope{LifecycleStates: []string{""}}); !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("ListPlanRefs empty lifecycle error = %v, want ErrInvalidPlanScope", err)
	}

	ref := agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
	checkpoint := agentosplan.PlanMetricCheckpoint{
		ExporterID: "exporter-" + suffix,
		PlanID:     spec.PlanID,
		AccountID:  spec.AccountID,
		ProjectID:  spec.ProjectID,
		Sequence:   3,
		Projection: agentosplan.PlanMetricProjectionState{
			PlanStartedAt: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
			NodeStartedAt: []agentosplan.PlanMetricNodeStartState{
				{
					NodeID:    "node-1",
					RunID:     spec.Nodes[0].Run.RunID,
					StartedAt: time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC),
				},
			},
		},
	}
	if err := planRepo.SavePlanMetricCheckpoint(ctx, checkpoint); err != nil {
		t.Fatalf("SavePlanMetricCheckpoint: %v", err)
	}
	loaded, exists, err := planRepo.GetPlanMetricCheckpoint(ctx, checkpoint.ExporterID, ref)
	if err != nil {
		t.Fatalf("GetPlanMetricCheckpoint: %v", err)
	}
	if !exists || loaded.Sequence != checkpoint.Sequence || !loaded.Projection.PlanStartedAt.Equal(checkpoint.Projection.PlanStartedAt) || len(loaded.Projection.NodeStartedAt) != 1 {
		t.Fatalf("loaded checkpoint = %#v exists=%v, want %#v", loaded, exists, checkpoint)
	}

	advanced := checkpoint
	advanced.Sequence = 5
	advanced.Projection.NodeStartedAt = nil
	if err := planRepo.SavePlanMetricCheckpoint(ctx, advanced); err != nil {
		t.Fatalf("SavePlanMetricCheckpoint advanced: %v", err)
	}
	if err := planRepo.SavePlanMetricCheckpoint(ctx, checkpoint); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SavePlanMetricCheckpoint rewind error = %v, want ErrInvalidRunPlan", err)
	}
	tenantMismatch := advanced
	tenantMismatch.AccountID = "acct-other"
	if err := planRepo.SavePlanMetricCheckpoint(ctx, tenantMismatch); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SavePlanMetricCheckpoint tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	sample := agentosplan.PlanMetricSample{
		Name:      agentosplan.PlanMetricPlanStartedTotal,
		Value:     1,
		Unit:      "count",
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		EventID:   spec.PlanID + ":1",
		Sequence:  1,
		Timestamp: time.Date(2026, 6, 19, 12, 0, 1, 123456000, time.UTC),
		Labels:    map[string]string{"lifecycle_state": agentos.PlanLifecycleRunning},
	}
	if err := planRepo.RecordPlanMetric(ctx, sample); err != nil {
		t.Fatalf("RecordPlanMetric first: %v", err)
	}
	if err := planRepo.RecordPlanMetric(ctx, sample); err != nil {
		t.Fatalf("RecordPlanMetric replay: %v", err)
	}
	var sampleCount int
	if err := pg.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM plan_metric_samples WHERE plan_id = $1`, spec.PlanID).Scan(&sampleCount); err != nil {
		t.Fatalf("count plan_metric_samples: %v", err)
	}
	if sampleCount != 1 {
		t.Fatalf("metric sample count = %d, want 1", sampleCount)
	}
	changedSample := sample
	changedSample.Value = 2
	if err := planRepo.RecordPlanMetric(ctx, changedSample); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordPlanMetric changed replay error = %v, want ErrInvalidRunPlan", err)
	}
	mismatchedSample := sample
	mismatchedSample.Sequence = 2
	mismatchedSample.EventID = spec.PlanID + ":2"
	mismatchedSample.AccountID = "acct-other"
	if err := planRepo.RecordPlanMetric(ctx, mismatchedSample); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("RecordPlanMetric tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestAgentOSArtifactPostgresRejectsDifferentIdempotencyReplay(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	blobStore, err := artifactblob.NewLocalBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBlobStore: %v", err)
	}
	artifactStore := NewAgentOSArtifactRepo(pg, blobStore)
	planRepo := NewAgentOSPlanRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-artifact-"+suffix, "plan-artifact-start-"+suffix)
	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	if _, _, err := planRepo.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	ref := agentos.ArtifactRef{
		PlanID:    spec.PlanID,
		NodeID:    "node-1",
		RunID:     "run-1",
		Name:      "summary",
		Kind:      agentos.ArtifactKindObject,
		MediaType: "application/json",
	}
	first, err := artifactStore.Put(ctx, ref, map[string]any{"summary": "ok"}, "artifact-key-"+suffix)
	if err != nil {
		t.Fatalf("Artifact Put first: %v", err)
	}
	replay, err := artifactStore.Put(ctx, ref, map[string]any{"summary": "ok"}, "artifact-key-"+suffix)
	if err != nil {
		t.Fatalf("Artifact Put replay: %v", err)
	}
	if replay.ArtifactID != first.ArtifactID || replay.Digest != first.Digest {
		t.Fatalf("Artifact replay = %#v, want %#v", replay, first)
	}

	if _, err := artifactStore.Put(ctx, ref, map[string]any{"summary": "changed"}, "artifact-key-"+suffix); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact Put changed payload error = %v, want ErrInvalidArtifact", err)
	}
	changedID := ref
	changedID.ArtifactID = "different-artifact-" + suffix
	if _, err := artifactStore.Put(ctx, changedID, map[string]any{"summary": "ok"}, "artifact-key-"+suffix); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact Put changed id error = %v, want ErrInvalidArtifact", err)
	}
	if _, err := artifactStore.Put(ctx, agentos.ArtifactRef{
		ArtifactID: first.ArtifactID,
		PlanID:     ref.PlanID,
		NodeID:     ref.NodeID,
		RunID:      ref.RunID,
		Name:       ref.Name,
		Kind:       ref.Kind,
		MediaType:  ref.MediaType,
	}, map[string]any{"summary": "ok"}, "artifact-other-key-"+suffix); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact Put reused artifact id error = %v, want ErrInvalidArtifact", err)
	}

	loaded, payload, err := artifactStore.Get(ctx, agentos.PlanArtifactScope{
		PlanID:     spec.PlanID,
		AccountID:  spec.AccountID,
		ProjectID:  spec.ProjectID,
		ArtifactID: first.ArtifactID,
	})
	if err != nil {
		t.Fatalf("Artifact Get: %v", err)
	}
	if loaded.Digest != first.Digest {
		t.Fatalf("loaded digest = %q, want %q", loaded.Digest, first.Digest)
	}
	value, ok := payload.(map[string]any)["summary"]
	if !ok || value != "ok" {
		t.Fatalf("payload = %#v, want original payload", payload)
	}
	assertPostgresArtifactScope(t, pg, first.ArtifactID, spec.AccountID, spec.ProjectID)
	refs, err := artifactStore.List(ctx, agentos.PlanArtifactScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		NodeID:    ref.NodeID,
		RunID:     ref.RunID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("Artifact List: %v", err)
	}
	if len(refs) != 1 || refs[0].ArtifactID != first.ArtifactID {
		t.Fatalf("Artifact List = %#v, want stored artifact", refs)
	}
	if _, _, err := artifactStore.Get(ctx, agentos.PlanArtifactScope{
		PlanID:     spec.PlanID,
		AccountID:  "acct-other",
		ProjectID:  spec.ProjectID,
		ArtifactID: first.ArtifactID,
	}); !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("Artifact Get tenant mismatch error = %v, want ErrArtifactNotFound", err)
	}
	wrongTenantRefs, err := artifactStore.List(ctx, agentos.PlanArtifactScope{
		PlanID:    spec.PlanID,
		AccountID: "acct-other",
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("Artifact List tenant mismatch: %v", err)
	}
	if len(wrongTenantRefs) != 0 {
		t.Fatalf("Artifact List tenant mismatch = %#v, want empty", wrongTenantRefs)
	}

	node := spec.Nodes[0]
	node.Inputs = []agentos.InputMapping{
		{
			Target:         "from_artifact.summary",
			SourceArtifact: "summary",
			SourcePath:     "summary",
			Required:       true,
		},
	}
	mapped, err := agentosplan.ResolveRunInput(ctx, artifactStore, nil, spec, agentos.RunPlanStatus{
		PlanID:    spec.PlanID,
		Artifacts: []agentos.ArtifactRef{first},
	}, node, nil)
	if err != nil {
		t.Fatalf("ResolveRunInput from postgres artifact: %v", err)
	}
	mappedArtifact, ok := mapped["from_artifact"].(map[string]any)
	if !ok || mappedArtifact["summary"] != "ok" {
		t.Fatalf("mapped artifact input = %#v, want summary payload", mapped)
	}
	if _, err := agentosplan.ResolveRunInput(ctx, artifactStore, nil, spec, agentos.RunPlanStatus{PlanID: spec.PlanID}, node, nil); !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("ResolveRunInput missing required artifact error = %v, want ErrArtifactNotFound", err)
	}
}

func postgresIntegrationPlanSpec(planID, idempotencyKey string) agentos.RunPlanSpec {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	return agentos.RunPlanSpec{
		PlanID:         planID,
		ThreadID:       "thread-" + planID,
		AccountID:      "account-" + planID,
		ProjectID:      "project-" + planID,
		IdempotencyKey: idempotencyKey,
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "node-1",
				Run: agentos.RunSpec{
					RunID:     "run-" + planID,
					ThreadID:  "thread-" + planID,
					AccountID: "account-" + planID,
					ProjectID: "project-" + planID,
					Backend:   ref,
				},
			},
		},
	}
}

func postgresIntegrationPlanStreamScope(spec agentos.RunPlanSpec) agentos.PlanStreamScope {
	return agentos.PlanStreamScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}

func assertPostgresPlanEventScope(t *testing.T, pg *postgres.Postgres, eventID, accountID, projectID string) {
	t.Helper()

	var storedAccountID string
	var storedProjectID string
	err := pg.Pool.QueryRow(t.Context(), `
SELECT account_id, project_id
FROM plan_events
WHERE event_id = $1`, eventID).Scan(&storedAccountID, &storedProjectID)
	if err != nil {
		t.Fatalf("read plan event tenant scope: %v", err)
	}
	if storedAccountID != accountID || storedProjectID != projectID {
		t.Fatalf("plan event tenant scope = %s/%s, want %s/%s", storedAccountID, storedProjectID, accountID, projectID)
	}
}

func assertPostgresArtifactScope(t *testing.T, pg *postgres.Postgres, artifactID, accountID, projectID string) {
	t.Helper()

	var storedAccountID string
	var storedProjectID string
	err := pg.Pool.QueryRow(t.Context(), `
SELECT account_id, project_id
FROM artifacts
WHERE artifact_id = $1`, artifactID).Scan(&storedAccountID, &storedProjectID)
	if err != nil {
		t.Fatalf("read artifact tenant scope: %v", err)
	}
	if storedAccountID != accountID || storedProjectID != projectID {
		t.Fatalf("artifact tenant scope = %s/%s, want %s/%s", storedAccountID, storedProjectID, accountID, projectID)
	}
}

type postgresRunBackendIndexExpectation struct {
	PlanID         string
	NodeID         string
	ThreadID       string
	AccountID      string
	ProjectID      string
	BackendKind    string
	BackendName    string
	IdempotencyKey string
	LifecycleState string
}

func assertPostgresRunBackendIndexRecord(t *testing.T, pg *postgres.Postgres, runID string, want postgresRunBackendIndexExpectation) {
	t.Helper()

	var got postgresRunBackendIndexExpectation
	err := pg.Pool.QueryRow(t.Context(), `
SELECT plan_id, node_id, thread_id, account_id, project_id, backend_kind, backend_name, idempotency_key, lifecycle_state
FROM run_backend_index
WHERE run_id = $1`, runID).Scan(
		&got.PlanID,
		&got.NodeID,
		&got.ThreadID,
		&got.AccountID,
		&got.ProjectID,
		&got.BackendKind,
		&got.BackendName,
		&got.IdempotencyKey,
		&got.LifecycleState,
	)
	if err != nil {
		t.Fatalf("read run backend index row: %v", err)
	}
	if got != want {
		t.Fatalf("run backend index row = %#v, want %#v", got, want)
	}
}

func newAgentOSPlanPostgresIntegrationDB(t *testing.T) (context.Context, *postgres.Postgres, string) {
	t.Helper()
	pgURL := os.Getenv("GOAGENT_POSTGRES_TEST_URL")
	if pgURL == "" {
		t.Fatal("GOAGENT_POSTGRES_TEST_URL is required for postgres_integration tests")
	}

	pg, err := postgres.New(pgURL, postgres.MaxPoolSize(1), postgres.ConnAttempts(1), postgres.ConnTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	t.Cleanup(pg.Close)
	waitForPostgres(t, pg)
	applyAgentOSPlanMigrations(t, pg)

	return t.Context(), pg, time.Now().UTC().Format("20060102150405.000000000")
}

func waitForPostgres(t *testing.T, pg *postgres.Postgres) {
	t.Helper()
	for range 30 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		err := pg.Pool.Ping(ctx)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatal("postgres did not become ready")
}

func applyAgentOSPlanMigrations(t *testing.T, pg *postgres.Postgres) {
	t.Helper()
	for _, migration := range []string{
		"20260617000001_create_agentos_plan_persistence.up.sql",
		"20260618000001_add_artifact_idempotency.up.sql",
		"20260619000001_create_agentos_capabilities.up.sql",
		"20260619000002_require_plan_event_idempotency.up.sql",
		"20260619000003_require_control_plane_idempotency.up.sql",
		"20260619000004_create_plan_commands.up.sql",
		"20260619000005_scope_plan_control_plane_records.up.sql",
		"20260619000006_create_plan_metric_checkpoints.up.sql",
		"20260619000007_create_plan_metric_samples.up.sql",
		"20260619000008_create_agentos_artifact_schemas.up.sql",
		"20260620000001_scope_agentos_idempotency_keys.up.sql",
	} {
		path := filepath.Join("..", "..", "..", "migrations", migration)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}
		if _, err := pg.Pool.Exec(t.Context(), string(data)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}
}
