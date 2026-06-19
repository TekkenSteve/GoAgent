//go:build postgres_integration

package persistent

import (
	"context"
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
	events, err := planRepo.ListPlanEvents(ctx, agentos.PlanStreamScope{PlanID: spec.PlanID}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventID != firstEvent.EventID {
		t.Fatalf("ListPlanEvents = %#v", events)
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
	deliveredCommand, err := planRepo.MarkPlanCommandDelivered(ctx, command.IdempotencyKey)
	if err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	if deliveredCommand.Status != agentosplan.PlanCommandDelivered || deliveredCommand.FailureReason != "" {
		t.Fatalf("delivered command = %#v", deliveredCommand)
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
	loadedArtifact, payload, err := artifactStore.Get(ctx, storedArtifact.ArtifactID)
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
			EventType: agentos.EventPlanStarted,
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
	events, err := planRepo.ListPlanEvents(ctx, agentos.PlanStreamScope{PlanID: spec.PlanID}, 0)
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

func TestAgentOSArtifactPostgresRejectsDifferentIdempotencyReplay(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	blobStore, err := artifactblob.NewLocalBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBlobStore: %v", err)
	}
	artifactStore := NewAgentOSArtifactRepo(pg, blobStore)

	ref := agentos.ArtifactRef{
		PlanID:    "plan-artifact-" + suffix,
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

	loaded, payload, err := artifactStore.Get(ctx, first.ArtifactID)
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
