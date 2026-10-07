package core

import (
	"fmt"
	"strings"
)

// TenantScope is the addressing pair runtime ports use to select data: the
// account that owns the work and the project it belongs to.
//
// It is not an identity. Only the AccountID half is authenticated (it comes
// from a verified token); ProjectID is a resource dimension the request names
// and authorization decides on. The two halves are kept together because every
// store keys rows by (account_id, project_id) — which is also what makes the
// pair safe: an attacker who names someone else's project still cannot reach
// the row, because its account_id is not theirs.
//
// Construct it at a boundary through Principal.Tenant; construct it from
// already-trusted input through NewTenantScope. Never build one from request
// fields alone.
//
// Relationship to the per-domain scopes (PlanScope, LedgerScope,
// GovernedActionScope, WorksetScope, ProcessStreamScope, ...): those bundle a
// tenant with domain selectors (record id, kind, limit, cursor) and today each
// redeclares AccountID/ProjectID. They are expected to embed TenantScope as
// they are touched, so tenant identity has exactly one definition.
type TenantScope struct {
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// NewTenantScope validates and returns a tenant scope from trusted input.
func NewTenantScope(accountID, projectID string) (TenantScope, error) {
	scope := TenantScope{
		AccountID: strings.TrimSpace(accountID),
		ProjectID: strings.TrimSpace(projectID),
	}

	if err := scope.Validate(); err != nil {
		return TenantScope{}, err
	}

	return scope, nil
}

// Validate reports whether both identifiers are present.
func (s TenantScope) Validate() error {
	if strings.TrimSpace(s.AccountID) == "" {
		return fmt.Errorf("%w: account_id is required", ErrInvalidTenantScope)
	}

	if strings.TrimSpace(s.ProjectID) == "" {
		return fmt.Errorf("%w: project_id is required", ErrInvalidTenantScope)
	}

	return nil
}

// IsZero reports whether the scope carries no tenant identity at all.
func (s TenantScope) IsZero() bool {
	return strings.TrimSpace(s.AccountID) == "" && strings.TrimSpace(s.ProjectID) == ""
}

// OwnedBy reports whether the scope belongs to the given account. A blank
// account owns nothing, so an unauthenticated caller never matches.
func (s TenantScope) OwnedBy(accountID string) bool {
	accountID = strings.TrimSpace(accountID)

	return accountID != "" && s.AccountID == accountID
}

// SameAccount reports whether two scopes address the same account. This is the
// tenant boundary the built-in authorizer enforces; project-level decisions
// belong to a policy engine (see Authorizer).
func (s TenantScope) SameAccount(other TenantScope) bool {
	return s.OwnedBy(other.AccountID)
}

// String renders the scope for logs and error messages.
func (s TenantScope) String() string {
	return s.AccountID + "/" + s.ProjectID
}

// PrincipalSource records how a Principal was established. It exists so
// downstream code can tell a verified identity from an asserted one, and so
// audit records can state their basis.
type PrincipalSource string

const (
	// PrincipalSourceJWT is an identity taken from a verified token.
	PrincipalSourceJWT PrincipalSource = "jwt"
	// PrincipalSourceGateway is an identity asserted by a trusted in-cluster
	// gateway that terminated authentication in front of this service and
	// signed the assertion this service verified.
	PrincipalSourceGateway PrincipalSource = "gateway"
	// PrincipalSourceRun is an identity inherited from an already-verified
	// run: the enforcement point sits inside the run's own execution (a tool
	// call being the case that exists), so the account comes from the run the
	// platform launched under a verified principal rather than from a
	// credential presented to this process.
	//
	// The distinction is the point of recording it: an audit trail that
	// claimed a fresh token verification, or a gateway assertion, for an act
	// that had neither would be describing evidence that does not exist.
	PrincipalSourceRun PrincipalSource = "run"
)

// Principal is the authenticated subject behind a request: which account may
// act, which actor an audit trail attributes the action to, and how that
// identity was established.
//
// AccountID is the tenant boundary and comes from a verified credential —
// never from the request body. ActorID distinguishes the human or service
// acting inside the account (defaults to the account itself).
type Principal struct {
	AccountID string
	ActorID   string
	Source    PrincipalSource
}

// NewPrincipal validates and returns a principal.
func NewPrincipal(accountID, actorID string, source PrincipalSource) (Principal, error) {
	principal := Principal{
		AccountID: strings.TrimSpace(accountID),
		ActorID:   strings.TrimSpace(actorID),
		Source:    source,
	}

	if err := principal.Validate(); err != nil {
		return Principal{}, err
	}

	return principal, nil
}

// Validate reports whether the principal names an account and a known source.
func (p Principal) Validate() error {
	if strings.TrimSpace(p.AccountID) == "" {
		return fmt.Errorf("%w: account_id is required", ErrInvalidPrincipal)
	}

	switch p.Source {
	case PrincipalSourceJWT, PrincipalSourceGateway, PrincipalSourceRun:
	default:
		return fmt.Errorf("%w: unknown principal source %q", ErrInvalidPrincipal, p.Source)
	}

	return nil
}

// IsZero reports whether the principal is unset. Callers refuse to operate on
// an unauthenticated request rather than falling back to a default tenant.
func (p Principal) IsZero() bool {
	return p.AccountID == "" && p.ActorID == "" && p.Source == ""
}

// EffectiveActor is the actor an audit record should name: the explicit actor
// when the credential carried one, otherwise the account itself.
func (p Principal) EffectiveActor() string {
	if p.ActorID != "" {
		return p.ActorID
	}

	return p.AccountID
}

// Tenant pairs the authenticated account with a requested project, in the form
// every runtime port and store expects. It is the single sanctioned way to
// turn a request-supplied project id into an addressing scope: the account
// half cannot be spoofed, so a forged project id addresses nothing.
func (p Principal) Tenant(projectID string) (TenantScope, error) {
	if err := p.Validate(); err != nil {
		return TenantScope{}, err
	}

	return NewTenantScope(p.AccountID, projectID)
}

// String renders the principal for logs without leaking anything sensitive.
func (p Principal) String() string {
	return fmt.Sprintf("%s@%s(%s)", p.EffectiveActor(), p.AccountID, p.Source)
}
