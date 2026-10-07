package v1

import (
	"errors"
	"net/http"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	templatepkg "github.com/TekkenSteve/GoAgent/internal/usecase/template"
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

	if req.YAMLData == "" {
		return errorResponse(ctx, http.StatusBadRequest, "yaml_data is required")
	}

	// The template belongs to the authenticated account: a body cannot place it
	// in someone else's.
	principal, ok := middleware.PrincipalFromCtx(ctx)
	if !ok {
		writeError(ctx, http.StatusUnauthorized, "authentication required")

		return nil
	}

	tpl, err := r.m.CreateFromYAML(ctx.UserContext(), principal.AccountID, []byte(req.YAMLData))
	if err != nil {
		r.l.Error(err, "restapi - v1 - importYAML")

		return errorResponse(ctx, http.StatusInternalServerError, "import failed: "+err.Error())
	}

	return ctx.Status(http.StatusCreated).JSON(tpl)
}

// list handles GET /v1/templates. It lists the authenticated account's
// templates; the account is not a query parameter.
func (r *templateHandler) list(ctx *fiber.Ctx) error {
	principal, ok := middleware.PrincipalFromCtx(ctx)
	if !ok {
		writeError(ctx, http.StatusUnauthorized, "authentication required")

		return nil
	}

	templates, err := r.m.ListByAccount(ctx.UserContext(), principal.AccountID)
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

// get handles GET /v1/templates/:template_id for the authenticated account.
// The account is what makes the lookup scoped: a template id belonging to
// someone else answers "not found", which is the same answer an unknown id
// gets.
func (r *templateHandler) get(ctx *fiber.Ctx) error {
	principal, ok := middleware.PrincipalFromCtx(ctx)
	if !ok {
		writeError(ctx, http.StatusUnauthorized, "authentication required")

		return nil
	}

	templateID := ctx.Params("template_id")
	if templateID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "missing template_id")
	}

	tpl, err := r.m.Get(ctx.UserContext(), principal.AccountID, templateID)
	if err != nil {
		r.l.Error(err, "restapi - v1 - get")

		return errorResponse(ctx, http.StatusNotFound, "template not found")
	}

	return ctx.Status(http.StatusOK).JSON(tpl)
}

// delete handles DELETE /v1/templates/:template_id for the authenticated
// account. Deleting a template the account does not own removes nothing and
// reports not found.
func (r *templateHandler) delete(ctx *fiber.Ctx) error {
	principal, ok := middleware.PrincipalFromCtx(ctx)
	if !ok {
		writeError(ctx, http.StatusUnauthorized, "authentication required")

		return nil
	}

	templateID := ctx.Params("template_id")
	if templateID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "missing template_id")
	}

	if err := r.m.Delete(ctx.UserContext(), principal.AccountID, templateID); err != nil {
		r.l.Error(err, "restapi - v1 - delete")

		if errors.Is(err, templatepkg.ErrTemplateNotFound) {
			return errorResponse(ctx, http.StatusNotFound, "template not found")
		}

		return errorResponse(ctx, http.StatusInternalServerError, "delete failed")
	}

	return ctx.SendStatus(http.StatusNoContent)
}
