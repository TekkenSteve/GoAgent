// Package authz holds authorization implementations for the agentos.Authorizer
// port (agentos/core/authz.go).
//
// Why a package rather than a single function: the port is the seam where
// authorization grows from "does this principal own this tenant" into a real
// policy engine, and the built-in implementation must not be the only shape
// callers can rely on. An engine-backed implementation (OpenFGA, SpiceDB, Ory
// Keto — all of which evaluate the same (subject, action, object) tuple the
// port exposes) is added here and selected in wiring; no caller changes.
//
// Every implementation fails closed: an error, an unknown relationship or an
// unreachable engine denies.
package authz

import (
	"context"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// TenantAuthorizer is the built-in authorization decision: a principal may act
// on an object when the object belongs to the principal's account.
//
// It needs no external state, which is what makes it safe as the default: the
// account half of the scope comes from a verified credential, so the check
// cannot be influenced by the request. It deliberately does not decide
// project- or resource-level relationships (roles, sharing, delegation) —
// those need a relationship store, and guessing them here would be a fake
// gate. The port is where such an engine plugs in.
type TenantAuthorizer struct{}

// Authorize enforces the account boundary.
func (TenantAuthorizer) Authorize(_ context.Context, request *agentoscore.AuthorizeRequest) error {
	if request == nil {
		return fmt.Errorf("%w: authorization request is required", agentoscore.ErrInvalidAuthorizationRequest)
	}

	// An incomplete decision is a bug in the caller, not a denial — but it
	// still denies, because the only safe answer to "I cannot tell" is no.
	if err := request.Validate(); err != nil {
		return err
	}

	if !request.Object.Tenant.OwnedBy(request.Principal.AccountID) {
		return fmt.Errorf(
			"%w: account %q cannot %s %s",
			agentoscore.ErrForbidden,
			request.Principal.AccountID,
			request.Action,
			request.Object,
		)
	}

	return nil
}

// Ensure the built-in implementation satisfies the port at compile time.
var _ agentoscore.Authorizer = TenantAuthorizer{}
