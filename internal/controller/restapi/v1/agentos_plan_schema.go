package v1

import (
	"net/http"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/gofiber/fiber/v2"
)

// @Summary     Get AgentOS plan authoring schema
// @Description Return a public JSON Schema used to author AgentOS RunPlan wire contracts.
// @ID          agentos-plan-schema
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       kind path string true "Schema kind: run-plan, plan-delta, capability-catalog, artifact-schema-catalog"
// @Success     200 {object} map[string]any
// @Failure     400 {object} response.Error
// @Router      /agentos/plans/schemas/{kind} [get]
// The schema is served as the wire contract for authoring AgentOS plans.
func (r *V1) agentOSPlanSchema(ctx *fiber.Ctx) error {
	schema, err := agentos.PlanJSONSchema(agentos.PlanSchemaKind(ctx.Params("kind")))
	if err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	ctx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSONCharsetUTF8)

	return ctx.Status(http.StatusOK).Send(schema)
}
