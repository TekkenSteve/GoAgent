//go:build postgres_integration

package persistent

import (
	"errors"
	"fmt"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosaction"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosbatch"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosledger"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprojection"
)

func TestAgentOSProcessPlatformPostgresDurablePersistence(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	assertPostgresTables(
		t,
		pg,
		"ledger_entries",
		"governed_actions",
		"governed_action_status_updates",
		"worksets",
		"workset_status_updates",
		"workset_chunk_results",
	)
	assertPostgresForeignKeyConstraint(t, pg, "governed_action_status_updates_action_tenant_fk")
	assertPostgresForeignKeyConstraint(t, pg, "workset_status_updates_workset_tenant_fk")
	assertPostgresForeignKeyConstraint(t, pg, "workset_chunk_results_workset_tenant_fk")

	processStore := NewAgentOSProcessRepo(pg)
	ledgerStore := NewAgentOSLedgerRepo(pg)
	actionStore := NewAgentOSActionRepo(pg)
	worksetStore := NewAgentOSWorksetRepo(pg)

	ledgerRuntime, err := agentosledger.NewRuntime(ledgerStore)
	if err != nil {
		t.Fatalf("NewLedgerRuntime: %v", err)
	}
	actionRuntime, err := agentosaction.NewRuntime(actionStore)
	if err != nil {
		t.Fatalf("NewActionRuntime: %v", err)
	}
	worksetRuntime, err := agentosbatch.NewRuntime(worksetStore)
	if err != nil {
		t.Fatalf("NewWorksetRuntime: %v", err)
	}
	projectionRuntime, err := agentosprojection.NewRuntime(agentosprojection.Config{
		Processes: processStore,
		Ledger:    ledgerStore,
		Actions:   actionStore,
		Worksets:  worksetStore,
	})
	if err != nil {
		t.Fatalf("NewProjectionRuntime: %v", err)
	}

	resource := postgresIntegrationResource(suffix)
	processSpec := postgresIntegrationPlatformProcessSpec(suffix, resource)
	processStatus := postgresIntegrationProcessStatus(&processSpec, agentos.ProcessRunning)
	if _, created, err := processStore.CreateProcess(ctx, &processSpec, &processStatus); err != nil {
		t.Fatalf("CreateProcess: %v", err)
	} else if !created {
		t.Fatal("CreateProcess created=false, want true")
	}

	ledgerSpec := postgresIntegrationLedgerEntrySpec(suffix, resource, processSpec.ProcessID)
	firstLedger, err := ledgerRuntime.AppendLedgerEntry(ctx, &ledgerSpec)
	if err != nil {
		t.Fatalf("AppendLedgerEntry: %v", err)
	}
	replayedLedger, err := ledgerRuntime.AppendLedgerEntry(ctx, &ledgerSpec)
	if err != nil {
		t.Fatalf("AppendLedgerEntry replay: %v", err)
	}
	if replayedLedger.Sequence != firstLedger.Sequence || replayedLedger.EntryID != firstLedger.EntryID {
		t.Fatalf("ledger replay = %#v, want %#v", replayedLedger, firstLedger)
	}
	changedLedger := ledgerSpec
	changedLedger.Summary = "changed summary"
	if _, err := ledgerRuntime.AppendLedgerEntry(ctx, &changedLedger); !errors.Is(err, agentoscore.ErrInvalidLedgerEntry) {
		t.Fatalf("AppendLedgerEntry changed error = %v, want ErrInvalidLedgerEntry", err)
	}

	actionSpec := postgresIntegrationActionSpec(suffix, resource, processSpec.ProcessID)
	firstAction, err := actionRuntime.RequestAction(ctx, &actionSpec)
	if err != nil {
		t.Fatalf("RequestAction: %v", err)
	}
	replayedAction, err := actionRuntime.RequestAction(ctx, &actionSpec)
	if err != nil {
		t.Fatalf("RequestAction replay: %v", err)
	}
	if replayedAction.LifecycleState != firstAction.LifecycleState {
		t.Fatalf("action replay lifecycle = %q, want %q", replayedAction.LifecycleState, firstAction.LifecycleState)
	}
	dryRun := agentos.ActionDryRunResult{
		IdempotencyKey: "action-dry-run-" + suffix,
		Succeeded:      true,
		Summary:        "validated",
		Risk:           agentos.ActionRiskAssessment{Level: agentos.ActionRiskLow, Reason: "preview only"},
		RecordedAt:     postgresIntegrationPlatformTime(),
	}
	dryRunStatus, err := actionRuntime.RecordActionDryRun(ctx, actionRefFromPlatformSpec(&actionSpec), &dryRun)
	if err != nil {
		t.Fatalf("RecordActionDryRun: %v", err)
	}
	dryRun.Risk.Level = agentos.ActionRiskCritical
	replayedDryRun, err := actionRuntime.RecordActionDryRun(ctx, actionRefFromPlatformSpec(&actionSpec), &dryRun)
	if err != nil {
		t.Fatalf("RecordActionDryRun replay: %v", err)
	}
	if replayedDryRun.Risk.Level != dryRunStatus.Risk.Level {
		t.Fatalf("dry-run replay risk = %q, want %q", replayedDryRun.Risk.Level, dryRunStatus.Risk.Level)
	}

	worksetSpec := postgresIntegrationWorksetSpec(suffix, resource, processSpec.ProcessID)
	firstWorkset, err := worksetRuntime.StartWorkset(ctx, &worksetSpec)
	if err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}
	replayedWorkset, err := worksetRuntime.StartWorkset(ctx, &worksetSpec)
	if err != nil {
		t.Fatalf("StartWorkset replay: %v", err)
	}
	if replayedWorkset.LifecycleState != firstWorkset.LifecycleState {
		t.Fatalf("workset replay lifecycle = %q, want %q", replayedWorkset.LifecycleState, firstWorkset.LifecycleState)
	}
	chunkResult := agentos.WorksetChunkResult{
		ChunkID:        "chunk-" + suffix,
		IdempotencyKey: "chunk-key-" + suffix,
		CompletedItems: 5,
		Succeeded:      true,
		RecordedAt:     postgresIntegrationPlatformTime(),
	}
	chunkStatus, err := worksetRuntime.RecordWorksetChunk(ctx, worksetRefFromPlatformSpec(&worksetSpec), &chunkResult)
	if err != nil {
		t.Fatalf("RecordWorksetChunk: %v", err)
	}
	chunkResult.CompletedItems = 99
	replayedChunk, err := worksetRuntime.RecordWorksetChunk(ctx, worksetRefFromPlatformSpec(&worksetSpec), &chunkResult)
	if err != nil {
		t.Fatalf("RecordWorksetChunk replay: %v", err)
	}
	if replayedChunk.Progress.CompletedItems != chunkStatus.Progress.CompletedItems {
		t.Fatalf("chunk replay completed items = %d, want %d", replayedChunk.Progress.CompletedItems, chunkStatus.Progress.CompletedItems)
	}

	projection, err := projectionRuntime.GetResourceProjection(ctx, &agentos.ResourceProjectionScope{
		Resource: resource,
		Limit:    20,
	})
	if err != nil {
		t.Fatalf("GetResourceProjection: %v", err)
	}
	if len(projection.Processes) != 1 || len(projection.Ledger) != 1 || len(projection.Actions) != 1 || len(projection.Worksets) != 1 {
		t.Fatalf("projection = %#v, want one process, ledger entry, action, and workset", projection)
	}
}

func postgresIntegrationResource(suffix string) agentos.ResourceRef {
	return agentos.ResourceRef{
		Kind:       "case",
		ResourceID: "case-" + suffix,
		AccountID:  "acct-platform",
		ProjectID:  "proj-platform",
	}
}

func postgresIntegrationPlatformProcessSpec(suffix string, resource agentos.ResourceRef) agentos.Spec {
	return agentos.Spec{
		ProcessID:      "process-platform-" + suffix,
		Kind:           "case",
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		IdempotencyKey: "process-platform-start-" + suffix,
		Resource:       resource,
		RequestedAt:    postgresIntegrationPlatformTime(),
	}
}

func postgresIntegrationLedgerEntrySpec(suffix string, resource agentos.ResourceRef, processID string) agentos.LedgerEntrySpec {
	return agentos.LedgerEntrySpec{
		EntryID:        "ledger-platform-" + suffix,
		IdempotencyKey: "ledger-platform-key-" + suffix,
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		ProcessID:      processID,
		Resource:       resource,
		Kind:           agentos.LedgerEntryEvidence,
		Actor:          agentos.ActorRef{Kind: "agent", ActorID: "investigator"},
		OccurredAt:     postgresIntegrationPlatformTime(),
		Summary:        "evidence indexed",
		DataRefs: []agentos.LedgerDataRef{{
			Kind:      "evidence",
			URI:       fmt.Sprintf("s3://agentos-test/%s/evidence.json", suffix),
			MediaType: "application/json",
		}},
	}
}

func postgresIntegrationActionSpec(suffix string, resource agentos.ResourceRef, processID string) agentos.GovernedActionSpec {
	return agentos.GovernedActionSpec{
		ActionID:         "action-platform-" + suffix,
		IdempotencyKey:   "action-platform-key-" + suffix,
		AccountID:        resource.AccountID,
		ProjectID:        resource.ProjectID,
		ProcessID:        processID,
		Resource:         resource,
		Kind:             "isolate_host",
		Intent:           "contain confirmed host compromise",
		RequestedAt:      postgresIntegrationPlatformTime(),
		DryRunRequired:   true,
		ApprovalRequired: true,
		Risk:             agentos.ActionRiskAssessment{Level: agentos.ActionRiskHigh, Reason: "endpoint isolation"},
	}
}

func postgresIntegrationWorksetSpec(suffix string, resource agentos.ResourceRef, processID string) agentos.WorksetSpec {
	return agentos.WorksetSpec{
		WorksetID:      "workset-platform-" + suffix,
		IdempotencyKey: "workset-platform-key-" + suffix,
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		ProcessID:      processID,
		Resource:       resource,
		Kind:           "hunt-backfill",
		RequestedAt:    postgresIntegrationPlatformTime(),
		ItemsRef: agentos.WorksetItemsRef{
			Kind:  "dataset",
			URI:   fmt.Sprintf("s3://agentos-test/%s/hunt.jsonl", suffix),
			Count: 5,
		},
		Chunks: []agentos.WorksetChunkSpec{{
			ChunkID:   "chunk-" + suffix,
			ItemCount: 5,
			ItemsRef: agentos.WorksetItemsRef{
				Kind:  "chunk",
				URI:   fmt.Sprintf("s3://agentos-test/%s/chunk.jsonl", suffix),
				Count: 5,
			},
		}},
	}
}

func actionRefFromPlatformSpec(spec *agentos.GovernedActionSpec) agentos.ActionRef {
	return agentos.ActionRef{
		ActionID:  spec.ActionID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}

func worksetRefFromPlatformSpec(spec *agentos.WorksetSpec) agentos.WorksetRef {
	return agentos.WorksetRef{
		WorksetID: spec.WorksetID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}

func postgresIntegrationPlatformTime() time.Time {
	return time.Date(2026, 7, 5, 9, 0, 0, 0, time.UTC)
}
