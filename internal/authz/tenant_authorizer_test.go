package authz

import (
	"context"
	"errors"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func principalFor(t *testing.T) agentoscore.Principal {
	t.Helper()

	principal, err := agentoscore.NewPrincipal("account-1", "", agentoscore.PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	return principal
}

func objectFor(t *testing.T, accountID, projectID string) agentoscore.ObjectRef {
	t.Helper()

	scope, err := agentoscore.NewTenantScope(accountID, projectID)
	if err != nil {
		t.Fatalf("NewTenantScope: %v", err)
	}

	object, err := agentoscore.NewObjectRef("run", "run-1", scope)
	if err != nil {
		t.Fatalf("NewObjectRef: %v", err)
	}

	return object
}

func TestTenantAuthorizerAllowsTheOwningAccount(t *testing.T) {
	t.Parallel()

	err := TenantAuthorizer{}.Authorize(context.Background(), &agentoscore.AuthorizeRequest{
		Principal: principalFor(t),
		Action:    agentoscore.ActionRunStart,
		Object:    objectFor(t, "account-1", "project-1"),
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
}

// TestTenantAuthorizerDeniesAnotherAccount is the boundary the platform relies
// on: a valid credential for one account cannot act on another account's object.
func TestTenantAuthorizerDeniesAnotherAccount(t *testing.T) {
	t.Parallel()

	err := TenantAuthorizer{}.Authorize(context.Background(), &agentoscore.AuthorizeRequest{
		Principal: principalFor(t),
		Action:    agentoscore.ActionRunStart,
		Object:    objectFor(t, "account-2", "project-1"),
	})
	if !errors.Is(err, agentoscore.ErrForbidden) {
		t.Fatalf("Authorize error = %v, want ErrForbidden", err)
	}
}

// TestTenantAuthorizerFailsClosed covers the "cannot tell" cases: they must
// deny rather than pass through.
func TestTenantAuthorizerFailsClosed(t *testing.T) {
	t.Parallel()

	valid := agentoscore.AuthorizeRequest{
		Principal: principalFor(t),
		Action:    agentoscore.ActionRunStart,
		Object:    objectFor(t, "account-1", "project-1"),
	}

	cases := map[string]agentoscore.AuthorizeRequest{
		"missing principal": {Action: valid.Action, Object: valid.Object},
		"missing action":    {Principal: valid.Principal, Object: valid.Object},
		"missing object":    {Principal: valid.Principal, Action: valid.Action},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := (TenantAuthorizer{}).Authorize(context.Background(), &request); err == nil {
				t.Fatal("an undecidable request must be denied")
			}
		})
	}
}

// TestTenantAuthorizerIgnoresTheProjectDimension documents the deliberate
// limit of the built-in decision: it enforces the account boundary and leaves
// project-level relationships to a policy engine on the same port.
func TestTenantAuthorizerIgnoresTheProjectDimension(t *testing.T) {
	t.Parallel()

	err := TenantAuthorizer{}.Authorize(context.Background(), &agentoscore.AuthorizeRequest{
		Principal: principalFor(t),
		Action:    agentoscore.ActionPlanRead,
		Object:    objectFor(t, "account-1", "some-other-project"),
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
}
