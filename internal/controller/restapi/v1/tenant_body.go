package v1

import (
	"net/http"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosprocess "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/gofiber/fiber/v2"
)

// This file holds the one shape every "create" request that carries its own
// tenant shares.
//
// A plan spec, a process spec, a ledger entry, a governed action and a workset
// all legitimately carry the tenant they address, because they name engine-level
// resources. None of them may decide it: the account comes from the credential
// and the project is authorized, so the server overwrites what the body said.
// The engines additionally reject a spec whose nested resource or actor
// references disagree with that tenant, so a foreign tenant cannot arrive
// through a nested field either.

// tenantScopedBody reads and replaces the tenant a parsed request body carries.
// Each engine spec gets a stateless adapter below; the adapters exist so the
// rule can live in one function instead of in every create handler.
type tenantScopedBody[S any] interface {
	projectID(spec *S) string
	applyTenant(spec *S, accountID, projectID string)
}

// withTenantScopedBody parses a body, authorizes the (credential account,
// requested project) pair, overwrites the tenant the body specified, and hands
// the spec to the operation. The operation owns its response: this only turns an
// operation error into the API's error shape.
func withTenantScopedBody[S any](
	r *V1,
	ctx *fiber.Ctx,
	body tenantScopedBody[S],
	action agentoscore.Action,
	objectKind string,
	operation func(spec *S) error,
) error {
	var spec S

	if err := ctx.BodyParser(&spec); err != nil {
		writeError(ctx, http.StatusBadRequest, "invalid request body")

		return nil
	}

	tenant, ok := r.tenantFromRequest(ctx, body.projectID(&spec), action, objectKind)
	if !ok {
		return nil
	}

	body.applyTenant(&spec, tenant.AccountID, tenant.ProjectID)

	if err := operation(&spec); err != nil {
		return agentOSError(ctx, err)
	}

	return nil
}

// planSpecBody adapts a RunPlanSpec to the shared tenant rule.
type planSpecBody struct{}

func (planSpecBody) projectID(spec *agentos.RunPlanSpec) string { return spec.ProjectID }

func (planSpecBody) applyTenant(spec *agentos.RunPlanSpec, accountID, projectID string) {
	spec.AccountID, spec.ProjectID = accountID, projectID
}

// processSpecBody adapts a process Spec.
type processSpecBody struct{}

func (processSpecBody) projectID(spec *agentosprocess.Spec) string { return spec.ProjectID }

func (processSpecBody) applyTenant(spec *agentosprocess.Spec, accountID, projectID string) {
	spec.AccountID, spec.ProjectID = accountID, projectID
}

// ledgerEntryBody adapts a LedgerEntrySpec.
type ledgerEntryBody struct{}

func (ledgerEntryBody) projectID(spec *agentosprocess.LedgerEntrySpec) string { return spec.ProjectID }

func (ledgerEntryBody) applyTenant(spec *agentosprocess.LedgerEntrySpec, accountID, projectID string) {
	spec.AccountID, spec.ProjectID = accountID, projectID
}

// governedActionBody adapts a GovernedActionSpec.
type governedActionBody struct{}

func (governedActionBody) projectID(spec *agentosprocess.GovernedActionSpec) string {
	return spec.ProjectID
}

func (governedActionBody) applyTenant(spec *agentosprocess.GovernedActionSpec, accountID, projectID string) {
	spec.AccountID, spec.ProjectID = accountID, projectID
}

// worksetBody adapts a WorksetSpec.
type worksetBody struct{}

func (worksetBody) projectID(spec *agentosprocess.WorksetSpec) string { return spec.ProjectID }

func (worksetBody) applyTenant(spec *agentosprocess.WorksetSpec, accountID, projectID string) {
	spec.AccountID, spec.ProjectID = accountID, projectID
}
