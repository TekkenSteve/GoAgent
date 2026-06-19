package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidatePlanRef requires the public control-plane reference to carry tenant
// scope. PlanID alone is not a sufficient production boundary.
func ValidatePlanRef(ref agentos.PlanRef) error {
	if ref.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidPlanScope)
	}
	if ref.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentos.ErrInvalidPlanScope)
	}

	return nil
}

// ValidatePlanStreamScope validates event replay/subscribe scope.
func ValidatePlanStreamScope(scope agentos.PlanStreamScope) error {
	return ValidatePlanRef(agentos.PlanRef{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
}

// ValidatePlanEventScope validates durable event history query scope.
func ValidatePlanEventScope(scope agentos.PlanEventScope) error {
	if err := ValidatePlanRef(agentos.PlanRef{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}); err != nil {
		return err
	}
	if scope.Limit < 0 {
		return fmt.Errorf("%w: event limit must be non-negative", agentos.ErrInvalidPlanScope)
	}

	return nil
}

// ValidatePlanAuditScope validates durable audit query scope.
func ValidatePlanAuditScope(scope agentos.PlanAuditScope) error {
	if err := ValidatePlanRef(agentos.PlanRef{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}); err != nil {
		return err
	}
	if scope.Limit < 0 {
		return fmt.Errorf("%w: audit limit must be non-negative", agentos.ErrInvalidPlanScope)
	}

	return nil
}

// ValidatePlanArtifactScope validates durable artifact query scope.
func ValidatePlanArtifactScope(scope agentos.PlanArtifactScope) error {
	if err := ValidatePlanRef(agentos.PlanRef{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}); err != nil {
		return err
	}
	if scope.Limit < 0 {
		return fmt.Errorf("%w: artifact limit must be non-negative", agentos.ErrInvalidPlanScope)
	}

	return nil
}

// ValidatePlanTenantAccess returns ErrPlanRouteNotFound for tenant mismatches so
// callers cannot distinguish missing plans from plans outside their scope.
func ValidatePlanTenantAccess(ref agentos.PlanRef, spec agentos.RunPlanSpec) error {
	if err := ValidatePlanRef(ref); err != nil {
		return err
	}
	if spec.PlanID != ref.PlanID {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, ref.PlanID)
	}
	if spec.AccountID != ref.AccountID {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, ref.PlanID)
	}
	if spec.ProjectID != ref.ProjectID {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, ref.PlanID)
	}

	return nil
}
