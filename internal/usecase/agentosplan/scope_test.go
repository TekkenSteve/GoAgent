package agentosplan

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanRefRequiresTenantScope(t *testing.T) {
	t.Parallel()

	err := ValidatePlanRef(agentos.PlanRef{PlanID: "plan-1"})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}

	err = ValidatePlanRef(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1"})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("project error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestValidatePlanTenantAccessHidesMismatches(t *testing.T) {
	t.Parallel()

	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}

	err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-2", ProjectID: "proj-1"}, &spec)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("account mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	err = ValidatePlanTenantAccess(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-2"}, &spec)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("project mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestValidatePlanScopesRejectNilPointers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want error
	}{
		{name: "run plan", err: ValidateRunPlanScope(nil), want: agentos.ErrInvalidRunPlan},
		{name: "stream", err: ValidatePlanStreamScope(nil), want: agentos.ErrInvalidPlanScope},
		{name: "event", err: ValidatePlanEventScope(nil), want: agentos.ErrInvalidPlanScope},
		{name: "debug trace", err: ValidatePlanDebugTraceScope(nil), want: agentos.ErrInvalidPlanScope},
		{name: "audit", err: ValidatePlanAuditScope(nil), want: agentos.ErrInvalidPlanScope},
		{name: "artifact", err: ValidatePlanArtifactScope(nil), want: agentos.ErrInvalidPlanScope},
	}
	for _, tc := range cases {
		if !errors.Is(tc.err, tc.want) {
			t.Fatalf("%s error = %v, want %v", tc.name, tc.err, tc.want)
		}
	}
}

func TestScopePlanEventToSpecRejectsNilPointers(t *testing.T) {
	t.Parallel()

	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}

	_, err := ScopePlanEventToSpec(nil, &spec)
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("nil event error = %v, want ErrInvalidPlanEvent", err)
	}

	_, err = ScopePlanEventToSpec(&agentos.PlanEvent{}, nil)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("nil spec error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanAuditScopeRejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	err := ValidatePlanAuditScope(&agentos.PlanAuditScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", Limit: -1})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestValidatePlanDebugTraceScopeRejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	err := ValidatePlanDebugTraceScope(&agentos.PlanDebugTraceScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", Limit: -1})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}
}
