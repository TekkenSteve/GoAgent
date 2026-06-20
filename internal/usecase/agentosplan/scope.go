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
	if ref.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentos.ErrInvalidPlanScope)
	}

	return nil
}

// ValidateRunPlanScope requires durable plans to carry their tenant boundary.
func ValidateRunPlanScope(spec agentos.RunPlanSpec) error {
	if spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentos.ErrInvalidRunPlan)
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

// ValidatePlanDebugTraceScope validates durable debug trace query scope.
func ValidatePlanDebugTraceScope(scope agentos.PlanDebugTraceScope) error {
	if err := ValidatePlanRef(agentos.PlanRef{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}); err != nil {
		return err
	}
	if scope.Limit < 0 {
		return fmt.Errorf("%w: debug trace limit must be non-negative", agentos.ErrInvalidPlanScope)
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

// ScopePlanEventToSpec assigns the durable plan scope to an append request and
// rejects caller-provided scope that does not match the stored plan.
func ScopePlanEventToSpec(event agentos.PlanEvent, spec agentos.RunPlanSpec) (agentos.PlanEvent, error) {
	if err := ValidateRunPlanScope(spec); err != nil {
		return agentos.PlanEvent{}, err
	}
	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidPlanEvent)
	}
	if event.PlanID != spec.PlanID {
		return agentos.PlanEvent{}, fmt.Errorf("%w: event plan %q does not match stored plan %q", agentos.ErrInvalidPlanEvent, event.PlanID, spec.PlanID)
	}
	if event.AccountID != "" && event.AccountID != spec.AccountID {
		return agentos.PlanEvent{}, fmt.Errorf("%w: event account %q does not match stored plan account %q", agentos.ErrInvalidPlanEvent, event.AccountID, spec.AccountID)
	}
	if event.ProjectID != "" && event.ProjectID != spec.ProjectID {
		return agentos.PlanEvent{}, fmt.Errorf("%w: event project %q does not match stored plan project %q", agentos.ErrInvalidPlanEvent, event.ProjectID, spec.ProjectID)
	}
	event.AccountID = spec.AccountID
	event.ProjectID = spec.ProjectID

	return event, nil
}
