package agentosplan

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanStartIdempotencyRejectsDifferentPlanID(t *testing.T) {
	err := ValidatePlanStartIdempotency(
		agentos.RunPlanSpec{PlanID: "plan-1", IdempotencyKey: "start-key"},
		agentos.RunPlanSpec{PlanID: "plan-2", IdempotencyKey: "start-key"},
	)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanStartIdempotencyRejectsDifferentRequest(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	existing := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		IdempotencyKey: "start-key",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1", Backend: ref}},
		},
	}
	requested := existing
	requested.Nodes = []agentos.PlanNodeSpec{
		{NodeID: "node-2", Run: agentos.RunSpec{RunID: "run-2", Backend: ref}},
	}

	err := ValidatePlanStartIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanStateIdentityAllowsWorkflowOwnedTopologyExpansion(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	existing := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		IdempotencyKey: "start-key",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}},
		},
	}
	expanded := existing
	expanded.Nodes = append(expanded.Nodes, agentos.PlanNodeSpec{NodeID: "expanded", Run: agentos.RunSpec{RunID: "run-expanded", Backend: ref}})
	expanded.Edges = []agentos.PlanEdgeSpec{{EdgeID: "seed-expanded", From: "seed", To: "expanded", On: agentos.EdgeOnSuccess}}

	if err := ValidatePlanStateIdentity(existing, expanded); err != nil {
		t.Fatalf("ValidatePlanStateIdentity: %v", err)
	}
}

func TestValidatePlanStateIdentityRejectsImmutableFieldChange(t *testing.T) {
	existing := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "account-1",
		IdempotencyKey: "start-key",
		Inputs:         map[string]any{"topic": "agentos"},
	}
	requested := existing
	requested.Inputs = map[string]any{"topic": "changed"}

	err := ValidatePlanStateIdentity(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidateAuditIdempotencyRejectsDifferentPayload(t *testing.T) {
	existing := AuditRecord{
		PlanID:         "plan-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-key",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	requested := existing
	requested.Payload = map[string]any{"type": string(agentos.SignalPlanReject)}

	err := ValidateAuditIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidateAuditIdempotencyRejectsDifferentTenantScope(t *testing.T) {
	existing := AuditRecord{
		PlanID:         "plan-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-key",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	requested := existing
	requested.AccountID = "account-2"

	err := ValidateAuditIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanCommandIdempotencyRejectsDifferentPayload(t *testing.T) {
	existing := PlanCommandRecord{
		PlanID:         "plan-1",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-key",
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	}
	requested := existing
	requested.Payload = map[string]any{"operation": string(agentos.ControlPause)}

	err := ValidatePlanCommandIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanCommandIdempotencyRejectsDifferentTenantScope(t *testing.T) {
	existing := PlanCommandRecord{
		PlanID:         "plan-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-key",
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	}
	requested := existing
	requested.ProjectID = "project-2"

	err := ValidatePlanCommandIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestPlanAuditRecordFromAuditRecordUsesPublicAction(t *testing.T) {
	record := PlanAuditRecordFromAuditRecord(AuditRecord{
		AuditID:        "audit-1",
		PlanID:         "plan-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		ActorID:        "operator-1",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	})
	if record.Action != agentos.PlanAuditActionControl ||
		record.AccountID != "account-1" ||
		record.ProjectID != "project-1" ||
		record.Payload["operation"] != string(agentos.ControlCancel) {
		t.Fatalf("record = %#v", record)
	}
}

func TestValidatePlanEventIdempotencyRejectsDifferentPayload(t *testing.T) {
	existing := agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "plan-1:1",
			EventType: agentos.EventPlanStarted,
			Sequence:  1,
			Payload:   map[string]any{"state": "started"},
		},
		PlanID: "plan-1",
	}
	requested := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "failed"},
		},
		PlanID: "plan-1",
	}

	err := ValidatePlanEventIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestValidatePlanEventIdempotencyRejectsDifferentTimestamp(t *testing.T) {
	existing := agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "plan-1:1",
			EventType: agentos.EventPlanStarted,
			Sequence:  1,
			Timestamp: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}
	requested := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Timestamp: time.Date(2026, 6, 20, 12, 1, 0, 0, time.UTC),
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	err := ValidatePlanEventIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestValidateArtifactPublishIdempotencyRejectsDifferentDigest(t *testing.T) {
	existing := agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
		MediaType:  "application/json",
		SizeBytes:  12,
		Digest:     "sha256:first",
	}
	requested := existing
	requested.Digest = "sha256:second"

	err := ValidateArtifactPublishIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}
