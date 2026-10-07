package v1

import (
	"errors"
	"net/http"
	"strings"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	"github.com/gofiber/fiber/v2"
)

// tenantFromRequest resolves the tenant a request acts in and asks the
// authorization port whether the caller may act there.
//
// This is the only sanctioned way a handler obtains a scope: the account half
// comes from the authenticated principal, the project half from the request,
// and the combination is authorized before any runtime port sees it. Handlers
// therefore never read an account id from a body or query parameter — a
// self-reported tenant is not an identity.
//
// On failure the response is already written and the result is false, so the
// handler returns nil (the request is finished, not continued). It reports a
// bool rather than an error on purpose: writing a response yields a nil error
// on success, so an error-returning helper would let a caller continue with an
// empty tenant — the exact silent failure this guard exists to prevent.
func (r *V1) tenantFromRequest(ctx *fiber.Ctx, projectID string, action agentoscore.Action, objectKind string) (agentoscore.TenantScope, bool) {
	principal, ok := middleware.PrincipalFromCtx(ctx)
	if !ok {
		// Reaching this means the route was mounted without RequireAuth in
		// front of it — a wiring bug, answered as unauthenticated rather than
		// served with an empty tenant.
		writeError(ctx, http.StatusUnauthorized, "authentication required")

		return agentoscore.TenantScope{}, false
	}

	scope, err := principal.Tenant(strings.TrimSpace(projectID))
	if err != nil {
		writeError(ctx, http.StatusBadRequest, "project_id is required")

		return agentoscore.TenantScope{}, false
	}

	if !r.authorize(ctx, principal, action, objectKind, scope) {
		return agentoscore.TenantScope{}, false
	}

	return scope, true
}

// runActorFromRequest resolves the caller and the run reference for an
// operation addressed by run id.
//
// The account and the acting party both come from the credential; the project
// deliberately does not, because a by-id request never names one.
// Authorization for these operations is the runtime's ownership check, which
// applies the same account boundary to the run's recorded tenant — the
// authorization port is not consulted here because the resource's tenant is
// unknown until the run is read, and asserting one would be a gate that checks
// nothing.
func (r *V1) runActorFromRequest(ctx *fiber.Ctx) (agentoscore.Principal, agentos.RunRef, bool) {
	principal, ok := middleware.PrincipalFromCtx(ctx)
	if !ok {
		writeError(ctx, http.StatusUnauthorized, "authentication required")

		return agentoscore.Principal{}, agentos.RunRef{}, false
	}

	ref, err := agentos.NewRunRef(principal, ctx.Params("run_id"))
	if err != nil {
		writeError(ctx, http.StatusBadRequest, "run_id is required")

		return agentoscore.Principal{}, agentos.RunRef{}, false
	}

	return principal, ref, true
}

// authorize consults the authorization port for one (principal, action,
// object) decision, writing the response and reporting false on denial.
func (r *V1) authorize(ctx *fiber.Ctx, principal agentoscore.Principal, action agentoscore.Action, objectKind string, scope agentoscore.TenantScope) bool {
	object, err := agentoscore.NewObjectRef(objectKind, scope.ProjectID, scope)
	if err != nil {
		writeError(ctx, http.StatusBadRequest, "invalid request scope")

		return false
	}

	err = r.authorizer.Authorize(ctx.UserContext(), &agentoscore.AuthorizeRequest{
		Principal: principal,
		Action:    action,
		Object:    object,
	})
	if err == nil {
		return true
	}

	// An undecidable request is a caller bug; anything else is a denial. Both
	// deny — the only unsafe answer would be allowing on error.
	if errors.Is(err, agentoscore.ErrForbidden) || errors.Is(err, agentoscore.ErrInvalidAuthorizationRequest) {
		writeError(ctx, http.StatusForbidden, "forbidden")

		return false
	}

	writeError(ctx, http.StatusServiceUnavailable, "authorization unavailable")

	return false
}

// writeError answers the request with an error payload and no error return.
//
// The write error is dropped deliberately: the response is committed by the
// time it could be reported, and every caller's only correct next step is to
// stop handling the request. Returning it would invite the mistake this file
// documents — treating a written 401 as a nil error and carrying on.
func writeError(ctx *fiber.Ctx, code int, message string) {
	//nolint:errcheck // a failed payload write cannot change the caller's next step: the status line is already committed
	_ = errorResponse(ctx, code, message)
}
