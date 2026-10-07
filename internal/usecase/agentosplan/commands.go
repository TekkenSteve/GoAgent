package agentosplan

import (
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// RecoverablePlanCommandStatuses validates a command query scope and returns
// the statuses that a command reconciler may redeliver.
func RecoverablePlanCommandStatuses(scope *PlanCommandScope) ([]PlanCommandStatus, error) {
	if scope.Limit < 0 {
		return nil, fmt.Errorf("%w: command limit must be non-negative", agentoscore.ErrInvalidRunPlan)
	}

	if (scope.AccountID == "") != (scope.ProjectID == "") {
		return nil, fmt.Errorf("%w: command account_id and project_id must be provided together", agentoscore.ErrInvalidPlanScope)
	}

	if len(scope.Statuses) == 0 {
		return []PlanCommandStatus{PlanCommandPending, PlanCommandFailed}, nil
	}

	statuses := make([]PlanCommandStatus, 0, len(scope.Statuses))

	seen := make(map[PlanCommandStatus]struct{}, len(scope.Statuses))
	for _, status := range scope.Statuses {
		if status != PlanCommandPending && status != PlanCommandFailed {
			return nil, fmt.Errorf("%w: command status %q is not recoverable", agentoscore.ErrInvalidRunPlan, status)
		}

		if _, ok := seen[status]; ok {
			continue
		}

		seen[status] = struct{}{}
		statuses = append(statuses, status)
	}

	return statuses, nil
}

// NormalizeNewPlanCommandStatus validates the lifecycle state assigned to a new
// outbox command. Commands are written before delivery, so they always enter
// the durable log as pending.
func NormalizeNewPlanCommandStatus(status PlanCommandStatus) (PlanCommandStatus, error) {
	if status == "" {
		return PlanCommandPending, nil
	}

	if status != PlanCommandPending {
		return "", fmt.Errorf("%w: new command status must be %q, got %q", agentoscore.ErrInvalidRunPlan, PlanCommandPending, status)
	}

	return status, nil
}

// ValidatePlanCommandStatusTransition enforces the durable outbox lifecycle.
// Delivered is terminal; failed commands remain recoverable and may later be
// delivered or have their failure reason refreshed.
func ValidatePlanCommandStatusTransition(current, next PlanCommandStatus) error {
	if err := validatePlanCommandStatus(current); err != nil {
		return err
	}

	if err := validatePlanCommandStatus(next); err != nil {
		return err
	}

	if current == next {
		return nil
	}

	if current == PlanCommandDelivered {
		return fmt.Errorf("%w: delivered command is terminal", agentoscore.ErrInvalidRunPlan)
	}

	if next == PlanCommandPending {
		return fmt.Errorf("%w: command status cannot move back to %q", agentoscore.ErrInvalidRunPlan, PlanCommandPending)
	}

	return nil
}

func validatePlanCommandStatus(status PlanCommandStatus) error {
	switch status {
	case PlanCommandPending, PlanCommandDelivered, PlanCommandFailed:
		return nil
	default:
		return fmt.Errorf("%w: command status %q is invalid", agentoscore.ErrInvalidRunPlan, status)
	}
}

// ValidatePlanCommandRef validates the tenant scope and idempotency key of a durable plan command reference.
func ValidatePlanCommandRef(ref PlanCommandRef) error {
	if err := ValidatePlanRef(agentos.PlanRef{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}); err != nil {
		return err
	}

	if ref.IdempotencyKey == "" {
		return fmt.Errorf("%w: command idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}

// AuditRecordFromPlanCommand returns the durable audit record that must exist
// before a command may become delivered. Delivered commands are terminal, so
// this keeps the control-plane outbox and audit trail in lockstep.
func AuditRecordFromPlanCommand(command *PlanCommandRecord) AuditRecord {
	return AuditRecord{
		PlanID:         command.PlanID,
		AccountID:      command.AccountID,
		ProjectID:      command.ProjectID,
		ActorID:        command.ActorID,
		Action:         command.Action,
		IdempotencyKey: command.IdempotencyKey,
		Payload:        command.Payload,
	}
}

// ValidatePlanCommandDeliveredAudit verifies that the audit record for a
// delivered command matches the original command envelope.
func ValidatePlanCommandDeliveredAudit(command *PlanCommandRecord, audit *AuditRecord) error {
	record := AuditRecordFromPlanCommand(command)

	return ValidateAuditIdempotency(audit, &record)
}

// ValidateAuditRef validates the tenant scope and idempotency key of a durable audit reference.
func ValidateAuditRef(ref AuditRef) error {
	if err := ValidatePlanRef(agentos.PlanRef{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}); err != nil {
		return err
	}

	if ref.IdempotencyKey == "" {
		return fmt.Errorf("%w: audit idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}
