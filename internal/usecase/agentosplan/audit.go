package agentosplan

import "github.com/TekkenSteve/GoAgent/agentos"

// PlanAuditRecordFromAuditRecord projects an internal audit record into the
// public AgentOS audit DTO.
func PlanAuditRecordFromAuditRecord(record AuditRecord) agentos.PlanAuditRecord {
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
