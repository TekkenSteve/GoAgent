package v1

import (
	"net/http"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
)

// templateHandler is the unexported handler group for template endpoints.
// It is instantiated inside NewRoutes when a TemplateManager is available.
type templateHandler struct {
	m usecase.TemplateManager
	l logger.Interface
}

// importYAML handles POST /v1/templates/import.
func (r *templateHandler) importYAML(ctx *fiber.Ctx) error {
	var req request.TemplateImport
	if err := ctx.BodyParser(&req); err != nil {
		r.l.Error(err, "restapi - v1 - importYAML")

		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	if req.AccountID == "" || req.YAMLData == "" {
		return errorResponse(ctx, http.StatusBadRequest, "account_id and yaml_data are required")
	}

	tpl, err := r.m.CreateFromYAML(ctx.UserContext(), req.AccountID, []byte(req.YAMLData))
	if err != nil {
		r.l.Error(err, "restapi - v1 - importYAML")

		return errorResponse(ctx, http.StatusInternalServerError, "import failed: "+err.Error())
	}

	return ctx.Status(http.StatusCreated).JSON(tpl)
}

// list handles GET /v1/templates?account_id=...
func (r *templateHandler) list(ctx *fiber.Ctx) error {
	accountID := ctx.Query("account_id")
	if accountID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "account_id query parameter is required")
	}

	templates, err := r.m.ListByAccount(ctx.UserContext(), accountID)
	if err != nil {
		r.l.Error(err, "restapi - v1 - list")

		return errorResponse(ctx, http.StatusInternalServerError, "list failed")
	}

	if templates == nil {
		templates = []entity.WorkflowTemplate{}
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"data": templates,
	})
}

// get handles GET /v1/templates/:template_id.
func (r *templateHandler) get(ctx *fiber.Ctx) error {
	templateID := ctx.Params("template_id")
	if templateID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "missing template_id")
	}

	tpl, err := r.m.Get(ctx.UserContext(), templateID)
	if err != nil {
		r.l.Error(err, "restapi - v1 - get")

		return errorResponse(ctx, http.StatusNotFound, "template not found")
	}

	return ctx.Status(http.StatusOK).JSON(tpl)
}

// delete handles DELETE /v1/templates/:template_id.
func (r *templateHandler) delete(ctx *fiber.Ctx) error {
	templateID := ctx.Params("template_id")
	if templateID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "missing template_id")
	}

	if err := r.m.Delete(ctx.UserContext(), templateID); err != nil {
		r.l.Error(err, "restapi - v1 - delete")

		return errorResponse(ctx, http.StatusInternalServerError, "delete failed")
	}

	return ctx.SendStatus(http.StatusNoContent)
}
