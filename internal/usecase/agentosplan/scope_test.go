package agentosplan

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanRefRequiresAccountScope(t *testing.T) {
	err := ValidatePlanRef(agentos.PlanRef{PlanID: "plan-1"})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestValidatePlanTenantAccessHidesMismatches(t *testing.T) {
	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-2", ProjectID: "proj-1"}, spec)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("account mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	err = ValidatePlanTenantAccess(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1"}, spec)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("project mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestValidatePlanAuditScopeRejectsNegativeLimit(t *testing.T) {
	err := ValidatePlanAuditScope(agentos.PlanAuditScope{PlanID: "plan-1", AccountID: "acct-1", Limit: -1})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}
}
