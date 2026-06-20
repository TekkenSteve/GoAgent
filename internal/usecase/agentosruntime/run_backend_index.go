package agentosruntime

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

const RunBackendLifecycleClaiming = "claiming"

// RunBackendIndexRecordFromRunSpec builds the ownership record for a
// standalone AgentOS run.
func RunBackendIndexRecordFromRunSpec(spec agentos.RunSpec, status agentos.RunStatus) entity.RunBackendIndexRecord {
	runID := status.RunID
	if runID == "" {
		runID = spec.RunID
	}
	lifecycle := status.LifecycleState
	if lifecycle == "" {
		lifecycle = "created"
	}

	return NormalizeRunBackendIndexRecord(entity.RunBackendIndexRecord{
		RunID:          runID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		BackendKind:    string(spec.Backend.Kind),
		BackendName:    spec.Backend.Name,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: lifecycle,
	})
}

// RunBackendIndexRecordFromPlanNode builds the ownership record for a
// backend-owned child run inside a RunPlan.
func RunBackendIndexRecordFromPlanNode(planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) entity.RunBackendIndexRecord {
	runID := status.RunID
	if runID == "" {
		runID = spec.RunID
	}
	lifecycle := status.LifecycleState
	if lifecycle == "" {
		lifecycle = "created"
	}

	return NormalizeRunBackendIndexRecord(entity.RunBackendIndexRecord{
		RunID:          runID,
		PlanID:         planID,
		NodeID:         nodeID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		BackendKind:    string(spec.Backend.Kind),
		BackendName:    spec.Backend.Name,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: lifecycle,
	})
}

// RunBackendIndexRecordFromOwnership maps the public durable ownership view
// into the internal idempotency record.
func RunBackendIndexRecordFromOwnership(ownership agentos.RunBackendOwnership) entity.RunBackendIndexRecord {
	return NormalizeRunBackendIndexRecord(entity.RunBackendIndexRecord{
		RunID:          ownership.RunID,
		PlanID:         ownership.PlanID,
		NodeID:         ownership.NodeID,
		ThreadID:       ownership.ThreadID,
		AccountID:      ownership.AccountID,
		ProjectID:      ownership.ProjectID,
		BackendKind:    string(ownership.Backend.Kind),
		BackendName:    ownership.Backend.Name,
		IdempotencyKey: ownership.IdempotencyKey,
		LifecycleState: ownership.LifecycleState,
		CreatedAt:      ownership.CreatedAt,
		UpdatedAt:      ownership.UpdatedAt,
	})
}

// RunBackendOwnershipFromRecord maps an internal ownership record to the
// public AgentOS routing view.
func RunBackendOwnershipFromRecord(record entity.RunBackendIndexRecord) agentos.RunBackendOwnership {
	record = NormalizeRunBackendIndexRecord(record)

	return agentos.RunBackendOwnership{
		RunID:          record.RunID,
		PlanID:         record.PlanID,
		NodeID:         record.NodeID,
		ThreadID:       record.ThreadID,
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		Backend:        agentos.BackendRef{Kind: agentos.BackendKind(record.BackendKind), Name: record.BackendName},
		IdempotencyKey: record.IdempotencyKey,
		LifecycleState: record.LifecycleState,
		CreatedAt:      record.CreatedAt,
		UpdatedAt:      record.UpdatedAt,
	}
}

// NormalizeRunBackendIndexRecord applies durable route defaults before storage
// or idempotency comparison.
func NormalizeRunBackendIndexRecord(record entity.RunBackendIndexRecord) entity.RunBackendIndexRecord {
	if record.LifecycleState == "" {
		record.LifecycleState = "created"
	}

	return record
}

// ValidateRunBackendIndexRecord checks the ownership record before a route
// index persists it.
func ValidateRunBackendIndexRecord(record entity.RunBackendIndexRecord, requireIdempotencyKey bool) error {
	if record.RunID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if record.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentos.ErrInvalidRunSpec)
	}
	if record.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentos.ErrInvalidRunSpec)
	}
	if record.BackendKind == "" || record.BackendName == "" {
		return fmt.Errorf("%w: kind and name are required", agentos.ErrInvalidBackendRef)
	}
	if requireIdempotencyKey && record.IdempotencyKey == "" {
		return fmt.Errorf("%w: run backend idempotency key is required", agentos.ErrInvalidRunSpec)
	}

	return nil
}

// ValidateRunBackendIndexIdempotency verifies that an ownership route replay
// describes the same backend-owned run.
func ValidateRunBackendIndexIdempotency(existing entity.RunBackendIndexRecord, requested entity.RunBackendIndexRecord) error {
	if existing.RunID != requested.RunID {
		return fmt.Errorf("%w: run backend idempotency key belongs to run %q", agentos.ErrInvalidRunSpec, existing.RunID)
	}
	if existing.PlanID != requested.PlanID || existing.NodeID != requested.NodeID {
		return fmt.Errorf("%w: run %q idempotency key was reused for a different plan node", agentos.ErrInvalidRunPlan, existing.RunID)
	}
	if existing.ThreadID != requested.ThreadID || existing.AccountID != requested.AccountID || existing.ProjectID != requested.ProjectID {
		return fmt.Errorf("%w: run %q idempotency key was reused for a different scope", agentos.ErrInvalidRunSpec, existing.RunID)
	}
	if existing.BackendKind != requested.BackendKind || existing.BackendName != requested.BackendName {
		return fmt.Errorf("%w: run %q idempotency key was reused for a different backend", agentos.ErrInvalidBackendRef, existing.RunID)
	}
	if existing.IdempotencyKey != "" && requested.IdempotencyKey != "" && existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: run %q was already bound with a different idempotency key", agentos.ErrInvalidRunSpec, existing.RunID)
	}

	return nil
}
