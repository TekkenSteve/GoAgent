package agentosplan

import (
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestValidatePlanRefRequiresTenantScope(t *testing.T) {
	t.Parallel()

	err := ValidatePlanRef(agentos.PlanRef{PlanID: "plan-1"})
	if !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}

	err = ValidatePlanRef(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1"})
	if !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
		t.Fatalf("project error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestValidatePlanTenantAccessHidesMismatches(t *testing.T) {
	t.Parallel()

	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}

	err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-2", ProjectID: "proj-1"}, &spec)
	if !errors.Is(err, agentoscore.ErrPlanRouteNotFound) {
		t.Fatalf("account mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	err = ValidatePlanTenantAccess(agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-2"}, &spec)
	if !errors.Is(err, agentoscore.ErrPlanRouteNotFound) {
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
		{name: "run plan", err: ValidateRunPlanScope(nil), want: agentoscore.ErrInvalidRunPlan},
		{name: "stream", err: ValidatePlanStreamScope(nil), want: agentoscore.ErrInvalidPlanScope},
		{name: "event", err: ValidatePlanEventScope(nil), want: agentoscore.ErrInvalidPlanScope},
		{name: "debug trace", err: ValidatePlanDebugTraceScope(nil), want: agentoscore.ErrInvalidPlanScope},
		{name: "audit", err: ValidatePlanAuditScope(nil), want: agentoscore.ErrInvalidPlanScope},
		{name: "artifact", err: ValidatePlanArtifactScope(nil), want: agentoscore.ErrInvalidPlanScope},
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
	if !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("nil event error = %v, want ErrInvalidPlanEvent", err)
	}

	_, err = ScopePlanEventToSpec(&agentos.PlanEvent{}, nil)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("nil spec error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanAuditScopeRejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	err := ValidatePlanAuditScope(&agentos.PlanAuditScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", Limit: -1})
	if !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestValidatePlanDebugTraceScopeRejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	err := ValidatePlanDebugTraceScope(&agentos.PlanDebugTraceScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", Limit: -1})
	if !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
		t.Fatalf("error = %v, want ErrInvalidPlanScope", err)
	}
}
