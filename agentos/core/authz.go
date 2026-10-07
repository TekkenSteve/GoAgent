package core

import (
	"context"
	"fmt"
	"strings"
)

// Action names an operation being authorized, in "resource:verb" form
// ("run:start", "action:approve", "ledger:write", "plan:read").
//
// Actions are plain strings because the set is open: a deployment's domain
// packages add their own without touching this package.
type Action string

// Common control-plane actions. Domain distributions are expected to define
// their own alongside these rather than extend the list in core.
const (
	ActionRunStart      Action = "run:start"
	ActionRunSignal     Action = "run:signal"
	ActionRunControl    Action = "run:control"
	ActionRunRead       Action = "run:read"
	ActionPlanRead      Action = "plan:read"
	ActionPlanControl   Action = "plan:control"
	ActionProcessRead   Action = "process:read"
	ActionProcessWrite  Action = "process:write"
	ActionWorksetRead   Action = "workset:read"
	ActionWorksetWrite  Action = "workset:write"
	ActionLedgerRead    Action = "ledger:read"
	ActionLedgerWrite   Action = "ledger:write"
	ActionActionRead    Action = "action:read"
	ActionActionWrite   Action = "action:write"
	ActionActionApprove Action = "action:approve"
	// ActionToolExecute is a run executing a tool. A tool call is an act of
	// the run, so it is authorized like any other object the run touches.
	ActionToolExecute Action = "tool:execute"
)

// ObjectRef identifies the object an Action targets.
//
// Kind is a domain-owned noun ("run", "plan", "governed_action", "workset",
// "artifact"); this package does not own the vocabulary. ID is the object's
// identifier within its kind, and Tenant is the tenant that owns it — carried
// so a scope-based authorizer can decide without a database round trip.
type ObjectRef struct {
	Kind   string
	ID     string
	Tenant TenantScope
}

// NewObjectRef validates and returns an object reference.
func NewObjectRef(kind, id string, tenant TenantScope) (ObjectRef, error) {
	ref := ObjectRef{
		Kind:   strings.TrimSpace(kind),
		ID:     strings.TrimSpace(id),
		Tenant: tenant,
	}

	if err := ref.Validate(); err != nil {
		return ObjectRef{}, err
	}

	return ref, nil
}

// Validate reports whether the reference names a kind, an id and a tenant.
func (r *ObjectRef) Validate() error {
	if strings.TrimSpace(r.Kind) == "" {
		return fmt.Errorf("%w: object kind is required", ErrInvalidAuthorizationRequest)
	}

	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("%w: object id is required", ErrInvalidAuthorizationRequest)
	}

	if r.Tenant.IsZero() {
		return fmt.Errorf("%w: object tenant is required", ErrInvalidAuthorizationRequest)
	}

	return nil
}

// String renders "kind:id@tenant" for logs and audit records.
func (r ObjectRef) String() string {
	return fmt.Sprintf("%s:%s@%s", r.Kind, r.ID, r.Tenant)
}

// AuthorizeRequest is one authorization decision: may Principal perform Action
// on Object?
type AuthorizeRequest struct {
	Principal Principal
	Action    Action
	Object    ObjectRef
}

// Validate reports whether the request is complete enough to decide.
func (r *AuthorizeRequest) Validate() error {
	if err := r.Principal.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidAuthorizationRequest, err)
	}

	if strings.TrimSpace(string(r.Action)) == "" {
		return fmt.Errorf("%w: action is required", ErrInvalidAuthorizationRequest)
	}

	object := r.Object

	return object.Validate()
}

// Authorizer decides whether a principal may perform an action on an object.
//
// The signature is deliberately relationship-shaped — (subject, action,
// object) — because authorization is expected to graduate to a ReBAC engine
// rather than accumulate ad-hoc rules here. OpenFGA, SpiceDB and Ory Keto all
// evaluate exactly a (user, relation, object) tuple, so an implementation
// backed by one maps Action onto a relation and ObjectRef onto
// "<kind>:<id>" without changing a single caller.
//
// Implementations must fail closed: an error, a missing binding or an
// unreachable policy engine denies. Callers must never treat a nil Authorizer
// as "allow" — wiring code rejects a nil authorization port at construction.
type Authorizer interface {
	Authorize(ctx context.Context, request *AuthorizeRequest) error
}
