package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// RecoverablePlanCommandStatuses validates a command query scope and returns
// the statuses that a command reconciler may redeliver.
func RecoverablePlanCommandStatuses(scope PlanCommandScope) ([]PlanCommandStatus, error) {
	if scope.Limit < 0 {
		return nil, fmt.Errorf("%w: command limit must be non-negative", agentos.ErrInvalidRunPlan)
	}
	if len(scope.Statuses) == 0 {
		return []PlanCommandStatus{PlanCommandPending, PlanCommandFailed}, nil
	}

	statuses := make([]PlanCommandStatus, 0, len(scope.Statuses))
	seen := make(map[PlanCommandStatus]struct{}, len(scope.Statuses))
	for _, status := range scope.Statuses {
		switch status {
		case PlanCommandPending, PlanCommandFailed:
		default:
			return nil, fmt.Errorf("%w: command status %q is not recoverable", agentos.ErrInvalidRunPlan, status)
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		statuses = append(statuses, status)
	}

	return statuses, nil
}

func ValidatePlanCommandRef(ref PlanCommandRef) error {
	if err := ValidatePlanRef(agentos.PlanRef{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}); err != nil {
		return err
	}
	if ref.IdempotencyKey == "" {
		return fmt.Errorf("%w: command idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	return nil
}

func ValidateAuditRef(ref AuditRef) error {
	if err := ValidatePlanRef(agentos.PlanRef{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}); err != nil {
		return err
	}
	if ref.IdempotencyKey == "" {
		return fmt.Errorf("%w: audit idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	return nil
}
