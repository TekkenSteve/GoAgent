//go:build postgres_integration

package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	artifactblob "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
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
	assertPostgresCheckConstraints(t, pg,
		"plans_account_id_required",
		"plans_project_id_required",
		"plan_events_account_id_required",
		"plan_events_project_id_required",
		"run_backend_index_account_id_required",
		"run_backend_index_project_id_required",
		"artifacts_account_id_required",
		"artifacts_project_id_required",
		"plan_commands_account_id_required",
		"plan_commands_project_id_required",
		"plan_metric_checkpoints_account_id_required",
		"plan_metric_checkpoints_project_id_required",
		"run_backend_index_plan_node_pair",
		"artifacts_node_run_pair",
		"audit_logs_node_run_pair",
		"plan_events_transition_snapshot_pair",
		"plan_events_event_json_identity_matches_columns",
		"plan_events_payload_json_matches_event",
		"plans_spec_json_identity_matches_columns",
		"plans_status_json_matches_columns",
		"plan_nodes_status_json_matches_columns",
	)
	assertPostgresUniqueConstraint(t, pg, "plans_tenant_plan_unique")
	assertPostgresForeignKeyConstraint(t, pg, "run_backend_index_plan_node_fk")
	assertPostgresForeignKeyConstraint(t, pg, "run_backend_index_plan_tenant_fk")
	assertPostgresForeignKeyConstraint(t, pg, "artifacts_plan_fk")
	assertPostgresForeignKeyConstraint(t, pg, "artifacts_plan_node_fk")
	assertPostgresForeignKeyConstraint(t, pg, "artifacts_plan_node_run_fk")
	assertPostgresForeignKeyConstraint(t, pg, "audit_logs_plan_fk")
	assertPostgresForeignKeyConstraint(t, pg, "audit_logs_plan_node_fk")
	assertPostgresForeignKeyConstraint(t, pg, "audit_logs_plan_node_run_fk")
	assertPostgresTrigger(t, pg, "plan_commands_status_lifecycle")
	assertPostgresTrigger(t, pg, "plan_commands_delivered_audit")
	assertPostgresTrigger(t, pg, "plan_commands_identity_immutable")
	assertPostgresTrigger(t, pg, "plan_events_append_only")
	assertPostgresTrigger(t, pg, "audit_logs_append_only")
	assertPostgresTrigger(t, pg, "artifacts_append_only")
	assertPostgresTrigger(t, pg, "plan_metric_samples_append_only")
	assertPostgresTrigger(t, pg, "run_backend_ownership_immutable")
	assertPostgresTrigger(t, pg, "plan_identity_immutable")
	assertPostgresTrigger(t, pg, "plan_node_identity_immutable")
	assertPostgresTriggerAbsent(t, pg, "audit_logs_delivered_command_audit")

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
		Spec:   spec,
		Status: nodeStatus,
	}); err != nil {
		t.Fatalf("SavePlanState node source: %v", err)
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plans SET account_id = 'hijacked' WHERE plan_id = $1`, spec.PlanID); err == nil {
		t.Fatal("direct plan identity update succeeded, want immutable identity trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM plans WHERE plan_id = $1`, spec.PlanID); err == nil {
		t.Fatal("direct plan delete succeeded, want immutable identity trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_nodes SET backend_name = 'hijacked' WHERE plan_id = $1 AND node_id = $2`, spec.PlanID, spec.Nodes[0].NodeID); err == nil {
		t.Fatal("direct plan node identity update succeeded, want immutable identity trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM plan_nodes WHERE plan_id = $1 AND node_id = $2`, spec.PlanID, spec.Nodes[0].NodeID); err == nil {
		t.Fatal("direct plan node delete succeeded, want immutable identity trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO plan_commands (
    command_id,
    plan_id,
    account_id,
    project_id,
    actor_id,
    action,
    idempotency_key,
    payload_json,
    status
) VALUES ($1,$2,$3,$4,$5,$6,$7,'{}'::jsonb,'delivered')`,
		"invalid-initial-status-"+suffix,
		spec.PlanID,
		spec.AccountID,
		spec.ProjectID,
		"operator-1",
		string(agentosplan.AuditActionPlanControl),
		"invalid-initial-status-"+suffix,
	); err == nil {
		t.Fatal("direct plan command insert with delivered status succeeded")
	}
	stateConstraintSpec := postgresIntegrationPlanSpec("plan-state-json-"+suffix, "plan-state-json-start-"+suffix)
	stateConstraintStatus := agentosplan.NewState(stateConstraintSpec, time.Now().UTC()).Status
	badSpecJSON := stateConstraintSpec
	badSpecJSON.PlanID = "wrong-plan-json"
	assertPostgresPlanInsertRejected(t, pg, stateConstraintSpec, badSpecJSON, stateConstraintStatus)
	badStatusJSON := stateConstraintStatus
	badStatusJSON.PlanID = "wrong-plan-json"
	assertPostgresPlanInsertRejected(t, pg, stateConstraintSpec, stateConstraintSpec, badStatusJSON)

	badNodeStatus := nodeStatus.Nodes[0]
	badNodeStatus.NodeID = "wrong-node-json"
	assertPostgresPlanNodeInsertRejected(t, pg, spec, "plan-node-json-"+suffix, nodeStatus.Nodes[0], badNodeStatus)

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
	if _, err := pg.Pool.Exec(ctx, `UPDATE audit_logs SET payload_json = '{}'::jsonb WHERE audit_id = $1`, audit.AuditID); err == nil {
		t.Fatal("direct audit log update succeeded, want append-only trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM audit_logs WHERE audit_id = $1`, audit.AuditID); err == nil {
		t.Fatal("direct audit log delete succeeded, want append-only trigger rejection")
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
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_commands SET payload_json = '{"operation":"pause"}'::jsonb WHERE command_id = $1`, command.CommandID); err == nil {
		t.Fatal("direct plan command payload update succeeded, want immutable identity trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_commands SET failure_reason = 'not failed' WHERE command_id = $1`, command.CommandID); err == nil {
		t.Fatal("direct pending plan command failure reason update succeeded")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM plan_commands WHERE command_id = $1`, command.CommandID); err == nil {
		t.Fatal("direct plan command delete succeeded, want immutable identity trigger rejection")
	}
	if _, err := planRepo.MarkPlanCommandDelivered(ctx, agentosplan.PlanCommandRefFromRecord(command)); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("MarkPlanCommandDelivered without audit error = %v, want ErrInvalidRunPlan", err)
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_commands SET status = 'delivered' WHERE command_id = $1`, command.CommandID); err == nil {
		t.Fatal("direct plan command delivered update without audit succeeded")
	}
	commandAudit, _, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecordFromPlanCommand(command))
	if err != nil {
		t.Fatalf("RecordAudit for command: %v", err)
	}
	deliveredCommand, err := planRepo.MarkPlanCommandDelivered(ctx, agentosplan.PlanCommandRefFromRecord(command))
	if err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	if deliveredCommand.Status != agentosplan.PlanCommandDelivered || deliveredCommand.FailureReason != "" {
		t.Fatalf("delivered command = %#v", deliveredCommand)
	}
	if _, err := planRepo.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(command), "late failure"); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("MarkPlanCommandFailed delivered command error = %v, want ErrInvalidRunPlan", err)
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_commands SET failure_reason = 'late reason' WHERE command_id = $1`, command.CommandID); err == nil {
		t.Fatal("direct delivered plan command failure reason update succeeded")
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_commands SET status = 'failed' WHERE command_id = $1`, command.CommandID); err == nil {
		t.Fatal("direct plan command delivered->failed update succeeded")
	}
	if _, err := pg.Pool.Exec(ctx, `UPDATE audit_logs SET payload_json = '{}'::jsonb WHERE audit_id = $1`, commandAudit.AuditID); err == nil {
		t.Fatal("direct delivered command audit update succeeded")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM audit_logs WHERE audit_id = $1`, commandAudit.AuditID); err == nil {
		t.Fatal("direct delivered command audit delete succeeded")
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
	refreshedFailedCommand, err := planRepo.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(failedCommand), "temporal still unavailable")
	if err != nil {
		t.Fatalf("MarkPlanCommandFailed refresh: %v", err)
	}
	if refreshedFailedCommand.FailureReason != "temporal still unavailable" {
		t.Fatalf("refreshed failed command reason = %q", refreshedFailedCommand.FailureReason)
	}
	recoverableCommands, err := planRepo.ListRecoverablePlanCommands(ctx, agentosplan.PlanCommandScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Statuses:  []agentosplan.PlanCommandStatus{agentosplan.PlanCommandFailed},
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
	if _, _, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		NodeID:         spec.Nodes[0].NodeID,
		RunID:          runSpec.RunID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "audit-missing-route-" + suffix,
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}); !errors.Is(err, agentos.ErrRunRouteNotFound) {
		t.Fatalf("RecordAudit missing run route error = %v, want ErrRunRouteNotFound", err)
	}
	if _, _, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		NodeID:         spec.Nodes[0].NodeID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "audit-unpaired-node-" + suffix,
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordAudit unpaired node/run error = %v, want ErrInvalidRunPlan", err)
	}
	if _, _, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		NodeID:         "missing-node",
		RunID:          runSpec.RunID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "audit-missing-node-" + suffix,
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordAudit missing node error = %v, want ErrInvalidRunPlan", err)
	}
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
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, runSpec, agentos.RunStatus{
		RunID:          runSpec.RunID,
		LifecycleState: agentosruntime.RunBackendLifecycleClaiming,
	}); err != nil {
		t.Fatalf("BindPlanNode replay claim: %v", err)
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
	if _, err := pg.Pool.Exec(ctx, `UPDATE run_backend_index SET backend_name = 'hijacked' WHERE run_id = $1`, runSpec.RunID); err == nil {
		t.Fatal("direct run backend ownership update succeeded, want immutable ownership trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM run_backend_index WHERE run_id = $1`, runSpec.RunID); err == nil {
		t.Fatal("direct run backend ownership delete succeeded, want immutable ownership trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO run_backend_index (
    run_id,
    plan_id,
    node_id,
    thread_id,
    account_id,
    project_id,
    backend_kind,
    backend_name,
    idempotency_key,
    lifecycle_state
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		"wrong-tenant-route-"+suffix,
		spec.PlanID,
		spec.Nodes[0].NodeID,
		runSpec.ThreadID,
		"wrong-"+spec.AccountID,
		spec.ProjectID,
		string(runSpec.Backend.Kind),
		runSpec.Backend.Name,
		"wrong-tenant-route-"+suffix,
		runStatus.LifecycleState,
	); err == nil {
		t.Fatal("direct run backend ownership insert with mismatched tenant succeeded")
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
	if err := routeIndex.BindPlanNode(ctx, "missing-plan-"+suffix, spec.Nodes[0].NodeID, runSpec, runStatus); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("BindPlanNode missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, "missing-node", runSpec, runStatus); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("BindPlanNode missing durable node error = %v, want ErrInvalidRunPlan", err)
	}
	wrongTenantRun := runSpec
	wrongTenantRun.AccountID = "account-other-" + suffix
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, wrongTenantRun, runStatus); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("BindPlanNode tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
	nodeAudit, created, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		NodeID:         spec.Nodes[0].NodeID,
		RunID:          runSpec.RunID,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "audit-node-" + suffix,
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	})
	if err != nil {
		t.Fatalf("RecordAudit node/run: %v", err)
	}
	if !created || nodeAudit.NodeID != spec.Nodes[0].NodeID || nodeAudit.RunID != runSpec.RunID {
		t.Fatalf("node audit = %#v created=%v", nodeAudit, created)
	}
	if _, _, err := planRepo.RecordAudit(ctx, agentosplan.AuditRecord{
		PlanID:         spec.PlanID,
		NodeID:         spec.Nodes[0].NodeID,
		RunID:          "wrong-run-" + suffix,
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "audit-wrong-run-" + suffix,
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordAudit wrong run error = %v, want ErrInvalidRunPlan", err)
	}
	standaloneRun := agentos.RunSpec{
		RunID:          "standalone-" + suffix,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Backend:        runSpec.Backend,
		IdempotencyKey: "standalone-run-start-" + suffix,
	}
	standaloneStatus := agentos.RunStatus{RunID: standaloneRun.RunID, LifecycleState: "running"}
	if err := routeIndex.Bind(ctx, standaloneRun, standaloneStatus); err != nil {
		t.Fatalf("Bind standalone first: %v", err)
	}
	if err := routeIndex.Bind(ctx, standaloneRun, standaloneStatus); err != nil {
		t.Fatalf("Bind standalone replay: %v", err)
	}
	if err := routeIndex.Bind(ctx, standaloneRun, agentos.RunStatus{
		RunID:          standaloneRun.RunID,
		LifecycleState: agentosruntime.RunBackendLifecycleClaiming,
	}); err != nil {
		t.Fatalf("Bind standalone replay claim: %v", err)
	}
	assertPostgresRunBackendIndexRecord(t, pg, standaloneRun.RunID, postgresRunBackendIndexExpectation{
		AccountID:      standaloneRun.AccountID,
		ProjectID:      standaloneRun.ProjectID,
		BackendKind:    string(standaloneRun.Backend.Kind),
		BackendName:    standaloneRun.Backend.Name,
		IdempotencyKey: standaloneRun.IdempotencyKey,
		LifecycleState: standaloneStatus.LifecycleState,
	})
	assertPostgresStandaloneRunBackendIndexUsesNullPlanNode(t, pg, standaloneRun.RunID)
	if err := routeIndex.Bind(ctx, standaloneRun, agentos.RunStatus{
		RunID:          standaloneRun.RunID,
		LifecycleState: "succeeded",
	}); err != nil {
		t.Fatalf("Bind standalone lifecycle update: %v", err)
	}
	assertPostgresRunBackendIndexRecord(t, pg, standaloneRun.RunID, postgresRunBackendIndexExpectation{
		AccountID:      standaloneRun.AccountID,
		ProjectID:      standaloneRun.ProjectID,
		BackendKind:    string(standaloneRun.Backend.Kind),
		BackendName:    standaloneRun.Backend.Name,
		IdempotencyKey: standaloneRun.IdempotencyKey,
		LifecycleState: "succeeded",
	})
	standaloneChangedBackend := standaloneRun
	standaloneChangedBackend.Backend = agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "other-backend"}
	if err := routeIndex.Bind(ctx, standaloneChangedBackend, standaloneStatus); !errors.Is(err, agentos.ErrInvalidBackendRef) {
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
	if _, err := pg.Pool.Exec(ctx, `UPDATE artifacts SET metadata_json = '{}'::jsonb WHERE artifact_id = $1`, storedArtifact.ArtifactID); err == nil {
		t.Fatal("direct artifact update succeeded, want append-only trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM artifacts WHERE artifact_id = $1`, storedArtifact.ArtifactID); err == nil {
		t.Fatal("direct artifact delete succeeded, want append-only trigger rejection")
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
		Spec:   agentos.RunPlanSpec{PlanID: "plan-state-key-required-" + suffix, AccountID: "acct-" + suffix, ProjectID: "proj-" + suffix},
		Status: agentos.RunPlanStatus{PlanID: "plan-state-key-required-" + suffix},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SavePlanState empty key error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestAgentOSPlanPostgresSavePlanStateRejectsMissingPlan(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)
	spec := postgresIntegrationPlanSpec("plan-state-missing-"+suffix, "plan-state-missing-start-"+suffix)

	err := planRepo.SavePlanState(ctx, agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: agentosplan.NewState(spec, time.Now().UTC()).Status,
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SavePlanState missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestAgentOSPlanPostgresPersistPlanTransitionRejectsMissingPlan(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)
	spec := postgresIntegrationPlanSpec("plan-transition-missing-"+suffix, "plan-transition-missing-start-"+suffix)

	_, err := planRepo.PersistPlanTransition(ctx, agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: agentosplan.NewState(spec, time.Now().UTC()).Status,
	}, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: spec.PlanID,
	}, "plan-transition-missing-event-"+suffix)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("PersistPlanTransition missing plan error = %v, want ErrPlanRouteNotFound", err)
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
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_events SET payload_json = '{}'::jsonb WHERE event_id = $1`, first.EventID); err == nil {
		t.Fatal("direct plan_events update succeeded, want append-only trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM plan_events WHERE event_id = $1`, first.EventID); err == nil {
		t.Fatal("direct plan_events delete succeeded, want append-only trigger rejection")
	}
	mismatchedIdentity := first
	mismatchedIdentity.EventID = "plan-event-json-identity-" + suffix
	mismatchedIdentity.PlanID = "wrong-plan"
	mismatchedIdentity.Sequence = 100
	assertPostgresPlanEventInsertRejected(t, pg, spec, "plan-event-json-identity-"+suffix, 100, first.Payload, mismatchedIdentity)

	mismatchedPayload := first
	mismatchedPayload.EventID = "plan-event-json-payload-" + suffix
	mismatchedPayload.Sequence = 101
	assertPostgresPlanEventInsertRejected(t, pg, spec, "plan-event-json-payload-"+suffix, 101, map[string]any{"state": "wrong"}, mismatchedPayload)

	missingIdentityJSON, err := json.Marshal(map[string]any{
		"event_type": string(first.EventType),
		"plan_id":    spec.PlanID,
		"account_id": spec.AccountID,
		"project_id": spec.ProjectID,
		"sequence":   102,
		"timestamp":  first.Timestamp,
		"payload":    first.Payload,
	})
	if err != nil {
		t.Fatalf("marshal missing identity event json: %v", err)
	}
	assertPostgresPlanEventRawInsertRejected(
		t,
		pg,
		spec,
		"plan-event-json-missing-identity-"+suffix,
		102,
		first.NodeID,
		first.RunID,
		first.EventType,
		first.Payload,
		missingIdentityJSON,
		first.Timestamp,
	)

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

func TestAgentOSPlanPostgresPersistPlanTransitionIsAtomic(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-transition-"+suffix, "plan-transition-start-"+suffix)
	status := agentosplan.NewState(spec, time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)).Status
	status.LifecycleState = agentos.PlanLifecycleRunning
	if _, _, err := planRepo.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	firstEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "running"},
		},
		PlanID: spec.PlanID,
	}
	if _, err := planRepo.AppendPlanEvent(ctx, firstEvent, "transition-key-"+suffix); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	next := status
	next.LifecycleState = agentos.PlanLifecycleFailed
	next.Reason = "should not commit"
	changedEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanFailed,
			Payload:   map[string]any{"state": "failed"},
		},
		PlanID: spec.PlanID,
	}
	_, err := planRepo.PersistPlanTransition(ctx, agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: next,
	}, changedEvent, "transition-key-"+suffix)
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("PersistPlanTransition error = %v, want ErrInvalidPlanEvent", err)
	}

	snapshot, exists, err := planRepo.LoadPlanState(ctx, spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("LoadPlanState exists=%v err=%v", exists, err)
	}
	if snapshot.Status.LifecycleState != agentos.PlanLifecycleRunning || snapshot.Status.Reason != "" {
		t.Fatalf("snapshot status = %#v, want original running state", snapshot.Status)
	}
	events, err := planRepo.ListPlanEvents(ctx, postgresIntegrationPlanStreamScope(spec), 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventType != agentos.EventPlanStarted {
		t.Fatalf("events = %#v, want only original event", events)
	}
}

func TestAgentOSPlanPostgresPersistPlanTransitionRejectsSnapshotReplayMismatch(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	planRepo := NewAgentOSPlanRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-transition-replay-"+suffix, "plan-transition-replay-start-"+suffix)
	initial := agentosplan.NewState(spec, time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)).Status
	if _, _, err := planRepo.CreatePlan(ctx, spec, initial); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	running := initial
	running.LifecycleState = agentos.PlanLifecycleRunning
	event := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "running"},
		},
		PlanID: spec.PlanID,
	}
	if _, err := planRepo.PersistPlanTransition(ctx, agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: running,
	}, event, "transition-replay-key-"+suffix); err != nil {
		t.Fatalf("PersistPlanTransition first: %v", err)
	}
	var transitionDigest string
	var transitionJSON string
	if err := pg.Pool.QueryRow(ctx, `
SELECT transition_snapshot_digest, transition_snapshot_json::text
FROM plan_events
WHERE plan_id = $1 AND idempotency_key = $2`,
		spec.PlanID,
		"transition-replay-key-"+suffix,
	).Scan(&transitionDigest, &transitionJSON); err != nil {
		t.Fatalf("transition snapshot query: %v", err)
	}
	if transitionDigest == "" || transitionJSON == "" {
		t.Fatalf("transition snapshot digest=%q json=%q, want durable transition identity", transitionDigest, transitionJSON)
	}

	changed := running
	changed.LifecycleState = agentos.PlanLifecycleFailed
	changed.Reason = "same event different snapshot"
	_, err := planRepo.PersistPlanTransition(ctx, agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: changed,
	}, event, "transition-replay-key-"+suffix)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("PersistPlanTransition replay mismatch error = %v, want ErrInvalidRunPlan", err)
	}

	snapshot, exists, err := planRepo.LoadPlanState(ctx, spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("LoadPlanState exists=%v err=%v", exists, err)
	}
	if snapshot.Status.LifecycleState != agentos.PlanLifecycleRunning || snapshot.Status.Reason != "" {
		t.Fatalf("snapshot status = %#v, want original running state", snapshot.Status)
	}
	events, err := planRepo.ListPlanEvents(ctx, postgresIntegrationPlanStreamScope(spec), 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventType != agentos.EventPlanStarted {
		t.Fatalf("events = %#v, want only original event", events)
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

func TestAgentOSPlanPostgresPlanEventIdempotencyIndexIsTenantScoped(t *testing.T) {
	_, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)

	columns := postgresIndexColumns(t, pg, "idx_plan_events_idempotency_key")
	want := []string{"account_id", "project_id", "plan_id", "idempotency_key"}
	if !slices.Equal(columns, want) {
		t.Fatalf("idx_plan_events_idempotency_key columns = %#v, want %#v", columns, want)
	}
	if !postgresIndexIsUnique(t, pg, "idx_plan_events_idempotency_key") {
		t.Fatal("idx_plan_events_idempotency_key is not unique")
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
	if _, err := pg.Pool.Exec(ctx, `UPDATE plan_metric_samples SET value = 2 WHERE plan_id = $1 AND event_id = $2`, sample.PlanID, sample.EventID); err == nil {
		t.Fatal("direct metric sample update succeeded, want append-only trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM plan_metric_samples WHERE plan_id = $1 AND event_id = $2`, sample.PlanID, sample.EventID); err == nil {
		t.Fatal("direct metric sample delete succeeded, want append-only trigger rejection")
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
	routeIndex := NewRunBackendIndexRepo(pg)

	spec := postgresIntegrationPlanSpec("plan-artifact-"+suffix, "plan-artifact-start-"+suffix)
	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	if _, _, err := planRepo.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	runSpec := spec.Nodes[0].Run
	runSpec.IdempotencyKey, err = agentosplan.NodeStartIdempotencyKey(spec.PlanID, spec.Nodes[0].NodeID, 1)
	if err != nil {
		t.Fatalf("NodeStartIdempotencyKey: %v", err)
	}

	ref := agentos.ArtifactRef{
		PlanID:    spec.PlanID,
		NodeID:    spec.Nodes[0].NodeID,
		RunID:     runSpec.RunID,
		Name:      "summary",
		Kind:      agentos.ArtifactKindObject,
		MediaType: "application/json",
		Metadata:  map[string]string{"class": "summary"},
	}
	if _, err := artifactStore.Put(ctx, ref, map[string]any{"summary": "ok"}, "artifact-missing-route-"+suffix); !errors.Is(err, agentos.ErrRunRouteNotFound) {
		t.Fatalf("Artifact Put missing run route error = %v, want ErrRunRouteNotFound", err)
	}
	missingNodeRef := ref
	missingNodeRef.NodeID = "missing-node"
	if _, err := artifactStore.Put(ctx, missingNodeRef, map[string]any{"summary": "ok"}, "artifact-missing-node-"+suffix); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact Put missing node error = %v, want ErrInvalidArtifact", err)
	}
	unpairedRef := ref
	unpairedRef.RunID = ""
	if _, err := artifactStore.Put(ctx, unpairedRef, map[string]any{"summary": "ok"}, "artifact-unpaired-node-"+suffix); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact Put unpaired node/run error = %v, want ErrInvalidArtifact", err)
	}
	if err := routeIndex.BindPlanNode(ctx, spec.PlanID, spec.Nodes[0].NodeID, runSpec, agentos.RunStatus{RunID: runSpec.RunID, LifecycleState: "running"}); err != nil {
		t.Fatalf("BindPlanNode for artifact: %v", err)
	}
	refOnlyBlob := ref
	refOnlyBlob.ArtifactID = "artifact-ref-only-" + suffix
	refOnlyBlob.URI = "local://artifact/unowned"
	refOnlyBlob.SizeBytes = 2
	refOnlyBlob.Digest = agentosplan.DigestArtifactPayload([]byte("ok"))
	if _, err := artifactStore.Put(ctx, refOnlyBlob, nil, "artifact-ref-only-"+suffix); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact Put ref-only blob metadata error = %v, want ErrInvalidArtifact", err)
	}
	first, err := artifactStore.Put(ctx, ref, map[string]any{
		"summary":        "ok",
		"payload_marker": "payload-" + suffix,
	}, "artifact-key-"+suffix)
	if err != nil {
		t.Fatalf("Artifact Put first: %v", err)
	}
	replay, err := artifactStore.Put(ctx, ref, map[string]any{
		"summary":        "ok",
		"payload_marker": "payload-" + suffix,
	}, "artifact-key-"+suffix)
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
	assertPostgresArtifactMetadataOnly(t, pg, first.ArtifactID, "class", "summary", "payload_marker")
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
	metadataOnlyStore := NewAgentOSArtifactRepo(pg, nil)
	metadataRefs, err := metadataOnlyStore.List(ctx, agentos.PlanArtifactScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		NodeID:    ref.NodeID,
		RunID:     ref.RunID,
	})
	if err != nil {
		t.Fatalf("Artifact metadata-only List: %v", err)
	}
	if len(metadataRefs) != 1 || metadataRefs[0].ArtifactID != first.ArtifactID || metadataRefs[0].URI == "" {
		t.Fatalf("Artifact metadata-only List = %#v, want stored ref with blob uri", metadataRefs)
	}
	if _, _, err := metadataOnlyStore.Get(ctx, agentos.PlanArtifactScope{
		PlanID:     spec.PlanID,
		AccountID:  spec.AccountID,
		ProjectID:  spec.ProjectID,
		ArtifactID: first.ArtifactID,
	}); !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Artifact metadata-only Get error = %v, want ErrInvalidArtifact", err)
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
			SourceNodeID:   spec.Nodes[0].NodeID,
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

func assertPostgresArtifactMetadataOnly(t *testing.T, pg *postgres.Postgres, artifactID, metadataKey, metadataValue, payloadKey string) {
	t.Helper()

	var storedMetadataValue string
	var hasPayloadKey bool
	err := pg.Pool.QueryRow(t.Context(), `
SELECT metadata_json ->> $2,
       metadata_json ? $3
FROM artifacts
WHERE artifact_id = $1`, artifactID, metadataKey, payloadKey).Scan(&storedMetadataValue, &hasPayloadKey)
	if err != nil {
		t.Fatalf("read artifact metadata json: %v", err)
	}
	if storedMetadataValue != metadataValue {
		t.Fatalf("artifact metadata %q = %q, want %q", metadataKey, storedMetadataValue, metadataValue)
	}
	if hasPayloadKey {
		t.Fatalf("artifact metadata unexpectedly contains payload key %q", payloadKey)
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
SELECT COALESCE(plan_id, ''), COALESCE(node_id, ''), thread_id, account_id, project_id, backend_kind, backend_name, idempotency_key, lifecycle_state
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

func assertPostgresStandaloneRunBackendIndexUsesNullPlanNode(t *testing.T, pg *postgres.Postgres, runID string) {
	t.Helper()

	var planIDIsNull bool
	var nodeIDIsNull bool
	err := pg.Pool.QueryRow(t.Context(), `
SELECT plan_id IS NULL, node_id IS NULL
FROM run_backend_index
WHERE run_id = $1`, runID).Scan(&planIDIsNull, &nodeIDIsNull)
	if err != nil {
		t.Fatalf("read standalone run backend null plan node: %v", err)
	}
	if !planIDIsNull || !nodeIDIsNull {
		t.Fatalf("standalone run backend plan/node null = %v/%v, want true/true", planIDIsNull, nodeIDIsNull)
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
		"20260620000002_scope_plan_event_idempotency_keys.up.sql",
		"20260620000003_require_agentos_control_plane_scope.up.sql",
		"20260620000004_constrain_run_backend_plan_nodes.up.sql",
		"20260620000005_constrain_artifact_plan_scope.up.sql",
		"20260620000006_constrain_audit_log_plan_scope.up.sql",
		"20260620000007_enforce_plan_command_lifecycle.up.sql",
		"20260620000008_add_plan_event_transition_snapshots.up.sql",
		"20260620000009_require_delivered_command_audit.up.sql",
		"20260620000010_protect_delivered_command_audits.up.sql",
		"20260620000011_protect_plan_events_append_only.up.sql",
		"20260620000012_protect_audit_logs_append_only.up.sql",
		"20260620000013_protect_artifacts_append_only.up.sql",
		"20260620000014_protect_plan_metric_samples_append_only.up.sql",
		"20260620000015_protect_run_backend_ownership.up.sql",
		"20260620000016_protect_plan_identity.up.sql",
		"20260620000017_protect_plan_command_identity.up.sql",
		"20260620000018_constrain_plan_event_json.up.sql",
		"20260620000019_constrain_plan_state_json.up.sql",
		"20260620000020_constrain_run_backend_plan_tenant.up.sql",
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

func assertPostgresPlanInsertRejected(
	t *testing.T,
	pg *postgres.Postgres,
	rowSpec agentos.RunPlanSpec,
	specJSONValue agentos.RunPlanSpec,
	statusJSONValue agentos.RunPlanStatus,
) {
	t.Helper()

	specJSON, err := json.Marshal(specJSONValue)
	if err != nil {
		t.Fatalf("marshal plan spec json: %v", err)
	}
	statusJSON, err := json.Marshal(statusJSONValue)
	if err != nil {
		t.Fatalf("marshal plan status json: %v", err)
	}
	_, err = pg.Pool.Exec(t.Context(), `
INSERT INTO plans (
    plan_id,
    thread_id,
    account_id,
    project_id,
    idempotency_key,
    lifecycle_state,
    reason,
    spec_json,
    status_json,
    requested_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		rowSpec.PlanID,
		rowSpec.ThreadID,
		rowSpec.AccountID,
		rowSpec.ProjectID,
		rowSpec.IdempotencyKey,
		statusJSONValue.LifecycleState,
		statusJSONValue.Reason,
		specJSON,
		statusJSON,
		nullableTimeForIntegration(rowSpec.RequestedAt),
	)
	if err == nil {
		t.Fatalf("direct plans insert %q succeeded, want constraint rejection", rowSpec.PlanID)
	}
}

func assertPostgresPlanNodeInsertRejected(
	t *testing.T,
	pg *postgres.Postgres,
	spec agentos.RunPlanSpec,
	nodeID string,
	rowStatus agentos.PlanNodeStatus,
	statusJSONValue agentos.PlanNodeStatus,
) {
	t.Helper()

	statusJSON, err := json.Marshal(statusJSONValue)
	if err != nil {
		t.Fatalf("marshal plan node status json: %v", err)
	}
	_, err = pg.Pool.Exec(t.Context(), `
INSERT INTO plan_nodes (
    plan_id,
    node_id,
    run_id,
    backend_kind,
    backend_name,
    capability,
    lifecycle_state,
    attempts,
    reason,
    status_json
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		spec.PlanID,
		nodeID,
		rowStatus.RunID,
		string(rowStatus.Backend.Kind),
		rowStatus.Backend.Name,
		spec.Nodes[0].Capability,
		rowStatus.LifecycleState,
		rowStatus.Attempts,
		rowStatus.Reason,
		statusJSON,
	)
	if err == nil {
		t.Fatalf("direct plan_nodes insert %q succeeded, want constraint rejection", nodeID)
	}
}

func assertPostgresPlanEventInsertRejected(
	t *testing.T,
	pg *postgres.Postgres,
	spec agentos.RunPlanSpec,
	eventID string,
	sequence int64,
	payload map[string]any,
	event agentos.PlanEvent,
) {
	t.Helper()

	eventJSON, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		t.Fatalf("marshal plan event: %v", err)
	}
	assertPostgresPlanEventRawInsertRejected(
		t,
		pg,
		spec,
		eventID,
		sequence,
		event.NodeID,
		event.RunID,
		event.EventType,
		payload,
		eventJSON,
		event.Timestamp,
	)
}

func assertPostgresPlanEventRawInsertRejected(
	t *testing.T,
	pg *postgres.Postgres,
	spec agentos.RunPlanSpec,
	eventID string,
	sequence int64,
	nodeID string,
	runID string,
	eventType agentos.EventType,
	payload map[string]any,
	eventJSON []byte,
	timestamp time.Time,
) {
	t.Helper()

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	_, err = pg.Pool.Exec(t.Context(), `
INSERT INTO plan_events (
    event_id,
    plan_id,
    account_id,
    project_id,
    node_id,
    run_id,
    event_type,
    sequence,
    idempotency_key,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		eventID,
		spec.PlanID,
		spec.AccountID,
		spec.ProjectID,
		nodeID,
		runID,
		string(eventType),
		sequence,
		eventID,
		payloadJSON,
		eventJSON,
		timestamp,
	)
	if err == nil {
		t.Fatalf("direct plan_events insert %q succeeded, want constraint rejection", eventID)
	}
}

func nullableTimeForIntegration(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return value
}

func assertPostgresCheckConstraints(t *testing.T, pg *postgres.Postgres, names ...string) {
	t.Helper()
	for _, name := range names {
		var exists bool
		if err := pg.Pool.QueryRow(t.Context(), `
SELECT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = $1
      AND contype = 'c'
)`, name).Scan(&exists); err != nil {
			t.Fatalf("query check constraint %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("missing check constraint %s", name)
		}
	}
}

func assertPostgresForeignKeyConstraint(t *testing.T, pg *postgres.Postgres, name string) {
	t.Helper()

	var exists bool
	if err := pg.Pool.QueryRow(t.Context(), `
SELECT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = $1
      AND contype = 'f'
)`, name).Scan(&exists); err != nil {
		t.Fatalf("query foreign key constraint %s: %v", name, err)
	}
	if !exists {
		t.Fatalf("missing foreign key constraint %s", name)
	}
}

func assertPostgresUniqueConstraint(t *testing.T, pg *postgres.Postgres, name string) {
	t.Helper()

	var exists bool
	if err := pg.Pool.QueryRow(t.Context(), `
SELECT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = $1
      AND contype = 'u'
)`, name).Scan(&exists); err != nil {
		t.Fatalf("query unique constraint %s: %v", name, err)
	}
	if !exists {
		t.Fatalf("missing unique constraint %s", name)
	}
}

func assertPostgresTrigger(t *testing.T, pg *postgres.Postgres, name string) {
	t.Helper()

	if postgresTriggerCount(t, pg, name) == 0 {
		t.Fatalf("trigger %s is missing", name)
	}
}

func assertPostgresTriggerAbsent(t *testing.T, pg *postgres.Postgres, name string) {
	t.Helper()

	if count := postgresTriggerCount(t, pg, name); count != 0 {
		t.Fatalf("trigger %s exists, want absent", name)
	}
}

func postgresTriggerCount(t *testing.T, pg *postgres.Postgres, name string) int {
	t.Helper()

	var count int
	err := pg.Pool.QueryRow(t.Context(), `
SELECT COUNT(*)
FROM pg_trigger
WHERE tgname = $1
  AND NOT tgisinternal`, name).Scan(&count)
	if err != nil {
		t.Fatalf("query trigger %s: %v", name, err)
	}

	return count
}

func postgresIndexColumns(t *testing.T, pg *postgres.Postgres, indexName string) []string {
	t.Helper()

	rows, err := pg.Pool.Query(t.Context(), `
SELECT attribute.attname
FROM pg_class AS index_class
JOIN pg_index AS index_info
    ON index_info.indexrelid = index_class.oid
JOIN LATERAL unnest(index_info.indkey) WITH ORDINALITY AS key(attnum, ord)
    ON TRUE
JOIN pg_attribute AS attribute
    ON attribute.attrelid = index_info.indrelid
   AND attribute.attnum = key.attnum
WHERE index_class.relname = $1
ORDER BY key.ord`, indexName)
	if err != nil {
		t.Fatalf("query index columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan index column: %v", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("index column rows: %v", err)
	}

	return columns
}

func postgresIndexIsUnique(t *testing.T, pg *postgres.Postgres, indexName string) bool {
	t.Helper()

	var unique bool
	err := pg.Pool.QueryRow(t.Context(), `
SELECT index_info.indisunique
FROM pg_class AS index_class
JOIN pg_index AS index_info
    ON index_info.indexrelid = index_class.oid
WHERE index_class.relname = $1`, indexName).Scan(&unique)
	if err != nil {
		t.Fatalf("query index uniqueness: %v", err)
	}

	return unique
}
