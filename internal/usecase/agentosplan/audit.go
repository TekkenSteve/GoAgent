package agentosplan

import (
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// ValidateAuditNodeRunPair validates the control-plane ownership shape for
// node-scoped audit records. Plan-level audits leave both fields empty; child
// run audits must identify both the node and the backend-owned run.
func ValidateAuditNodeRunPair(record *AuditRecord) error {
	if record.NodeID == "" && record.RunID == "" {
		return nil
	}

	if record.NodeID == "" || record.RunID == "" {
		return fmt.Errorf("%w: audit node id and run id must be provided together", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}

// ValidateAuditNodeRunInPlan validates node/run ownership against an in-memory
// durable plan snapshot.
func ValidateAuditNodeRunInPlan(record *AuditRecord, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) error {
	if err := ValidateAuditNodeRunPair(record); err != nil {
		return err
	}

	if record.NodeID == "" {
		return nil
	}

	runID, exists := auditPlanNodeRunID(spec, status, record.NodeID)
	if !exists {
		return fmt.Errorf("%w: audit node %q is not durable", agentoscore.ErrInvalidRunPlan, record.NodeID)
	}

	if runID == "" {
		return fmt.Errorf("%w: audit node %q has no durable run id", agentoscore.ErrInvalidRunPlan, record.NodeID)
	}

	if runID != record.RunID {
		return fmt.Errorf("%w: audit node %q has durable run id %q, got %q", agentoscore.ErrInvalidRunPlan, record.NodeID, runID, record.RunID)
	}

	return nil
}

func auditPlanNodeRunID(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, nodeID string) (string, bool) {
	if len(status.Nodes) > 0 {
		for i := range status.Nodes {
			node := &status.Nodes[i]
			if node.NodeID == nodeID {
				return node.RunID, true
			}
		}

		return "", false
	}

	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		if node.NodeID == nodeID {
			return node.Run.RunID, true
		}
	}

	return "", false
}

// PlanAuditRecordFromAuditRecord projects an internal audit record into the
// public AgentOS audit DTO.
func PlanAuditRecordFromAuditRecord(record *AuditRecord) agentos.PlanAuditRecord {
	return agentos.PlanAuditRecord{
		AuditID:        record.AuditID,
		PlanID:         record.PlanID,
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		RunID:          record.RunID,
		NodeID:         record.NodeID,
		ActorID:        record.ActorID,
		Action:         agentos.PlanAuditAction(record.Action),
		IdempotencyKey: record.IdempotencyKey,
		Payload:        normalizeAuditPayload(record.Payload),
		CreatedAt:      record.CreatedAt,
	}
}
