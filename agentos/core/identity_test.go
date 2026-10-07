package core

import (
	"context"
	"errors"
	"testing"
)

func validTenant(t *testing.T) TenantScope {
	t.Helper()

	scope, err := NewTenantScope("account-1", "project-1")
	if err != nil {
		t.Fatalf("NewTenantScope: %v", err)
	}

	return scope
}

func TestNewTenantScopeRejectsIncompleteTenants(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		account string
		project string
	}{
		"missing account": {account: "", project: "project-1"},
		"blank account":   {account: "   ", project: "project-1"},
		"missing project": {account: "account-1", project: ""},
		"blank project":   {account: "account-1", project: "\t"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewTenantScope(tc.account, tc.project)
			if !errors.Is(err, ErrInvalidTenantScope) {
				t.Fatalf("NewTenantScope error = %v, want ErrInvalidTenantScope", err)
			}
		})
	}
}

func TestNewTenantScopeTrimsIdentifiers(t *testing.T) {
	t.Parallel()

	scope, err := NewTenantScope("  account-1  ", " project-1 ")
	if err != nil {
		t.Fatalf("NewTenantScope: %v", err)
	}

	if scope.AccountID != "account-1" || scope.ProjectID != "project-1" {
		t.Fatalf("scope = %q, want trimmed identifiers", scope)
	}
}

func TestTenantScopeSameAccountRequiresANonZeroAccount(t *testing.T) {
	t.Parallel()

	scope := validTenant(t)
	otherProject := TenantScope{AccountID: scope.AccountID, ProjectID: "project-2"}
	otherAccount := TenantScope{AccountID: "account-2", ProjectID: scope.ProjectID}

	if !scope.SameAccount(otherProject) {
		t.Fatal("same account with a different project must match the account boundary")
	}

	if scope.SameAccount(otherAccount) {
		t.Fatal("a different account must not match")
	}

	if (TenantScope{}).SameAccount(TenantScope{}) {
		t.Fatal("a zero account must match nothing")
	}
}

func TestNewPrincipalRejectsIncompleteIdentities(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		account string
		actor   string
		source  PrincipalSource
	}{
		"missing account": {account: "", source: PrincipalSourceJWT},
		"blank account":   {account: "  ", source: PrincipalSourceJWT},
		"missing source":  {account: "account-1", source: ""},
		"unknown source":  {account: "account-1", source: "self-reported"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewPrincipal(tc.account, tc.actor, tc.source)
			if !errors.Is(err, ErrInvalidPrincipal) {
				t.Fatalf("NewPrincipal error = %v, want ErrInvalidPrincipal", err)
			}
		})
	}
}

func TestPrincipalAcceptsEveryKnownSource(t *testing.T) {
	t.Parallel()

	for _, source := range []PrincipalSource{PrincipalSourceJWT, PrincipalSourceGateway} {
		t.Run(string(source), func(t *testing.T) {
			t.Parallel()

			if _, err := NewPrincipal("account-1", "actor-1", source); err != nil {
				t.Fatalf("NewPrincipal(%q): %v", source, err)
			}
		})
	}
}

func TestPrincipalEffectiveActorFallsBackToTheAccount(t *testing.T) {
	t.Parallel()

	withActor, err := NewPrincipal("account-1", "actor-1", PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	if got := withActor.EffectiveActor(); got != "actor-1" {
		t.Fatalf("EffectiveActor() = %q, want the explicit actor", got)
	}

	withoutActor, err := NewPrincipal("account-1", "", PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	if got := withoutActor.EffectiveActor(); got != "account-1" {
		t.Fatalf("EffectiveActor() = %q, want the account", got)
	}
}

// TestPrincipalTenantBindsTheAuthenticatedAccount is the regression guard for
// self-reported tenancy: the project half is whatever the request asked for,
// but the account half can only ever be the authenticated one.
func TestPrincipalTenantBindsTheAuthenticatedAccount(t *testing.T) {
	t.Parallel()

	principal, err := NewPrincipal("account-1", "", PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	scope, err := principal.Tenant("project-9")
	if err != nil {
		t.Fatalf("Tenant: %v", err)
	}

	if scope.AccountID != "account-1" {
		t.Fatalf("account = %q, want the authenticated account", scope.AccountID)
	}

	if scope.ProjectID != "project-9" {
		t.Fatalf("project = %q, want the requested project", scope.ProjectID)
	}

	zero := Principal{}
	if _, err := zero.Tenant("project-9"); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("Tenant on a zero principal error = %v, want ErrInvalidPrincipal", err)
	}
}

func TestObjectRefRejectsIncompleteReferences(t *testing.T) {
	t.Parallel()

	tenant := validTenant(t)

	cases := map[string]ObjectRef{
		"missing kind":   {Kind: "", ID: "plan-1", Tenant: tenant},
		"missing id":     {Kind: "plan", ID: "", Tenant: tenant},
		"missing tenant": {Kind: "plan", ID: "plan-1"},
	}
	for name, ref := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reference := ref

			if err := reference.Validate(); !errors.Is(err, ErrInvalidAuthorizationRequest) {
				t.Fatalf("Validate error = %v, want ErrInvalidAuthorizationRequest", err)
			}
		})
	}
}

func TestAuthorizeRequestRejectsIncompleteDecisions(t *testing.T) {
	t.Parallel()

	principal, err := NewPrincipal("account-1", "", PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	object, err := NewObjectRef("plan", "plan-1", validTenant(t))
	if err != nil {
		t.Fatalf("NewObjectRef: %v", err)
	}

	complete := AuthorizeRequest{Principal: principal, Action: ActionPlanRead, Object: object}

	if err := complete.Validate(); err != nil {
		t.Fatalf("Validate on a complete request: %v", err)
	}

	cases := map[string]AuthorizeRequest{
		"missing principal": {Action: ActionPlanRead, Object: object},
		"missing action":    {Principal: principal, Object: object},
		"missing object":    {Principal: principal, Action: ActionPlanRead},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			invalid := request

			if err := invalid.Validate(); !errors.Is(err, ErrInvalidAuthorizationRequest) {
				t.Fatalf("Validate error = %v, want ErrInvalidAuthorizationRequest", err)
			}
		})
	}
}

// stubAuthorizer proves the port is relationship-shaped: an implementation
// needs nothing but (subject, action, object) to decide, which is exactly what
// a ReBAC engine (OpenFGA, SpiceDB, Ory Keto) evaluates.
type stubAuthorizer struct {
	denied bool
}

func (s stubAuthorizer) Authorize(_ context.Context, request *AuthorizeRequest) error {
	if s.denied {
		return ErrForbidden
	}

	if request.Object.Kind != "plan" || request.Action != ActionPlanRead {
		return ErrForbidden
	}

	return nil
}

func TestAuthorizerPortIsDecidableFromTheRelationshipAlone(t *testing.T) {
	t.Parallel()

	principal, err := NewPrincipal("account-1", "actor-1", PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	object, err := NewObjectRef("plan", "plan-1", validTenant(t))
	if err != nil {
		t.Fatalf("NewObjectRef: %v", err)
	}

	request := AuthorizeRequest{Principal: principal, Action: ActionPlanRead, Object: object}

	if err := (stubAuthorizer{}).Authorize(context.Background(), &request); err != nil {
		t.Fatalf("allowed request: %v", err)
	}

	if err := (stubAuthorizer{denied: true}).Authorize(context.Background(), &request); !errors.Is(err, ErrForbidden) {
		t.Fatalf("denied request error = %v, want ErrForbidden", err)
	}
}
