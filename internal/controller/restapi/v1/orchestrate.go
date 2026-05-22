package v1

import (
	"net/http"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/response"
	"github.com/gofiber/fiber/v2"
)

// @Summary     Execute orchestration workflow
// @Description Start a multi-step orchestration workflow from a TeamSpec or step queue
// @ID          orchestrate
// @Tags        orchestration
// @Accept      json
// @Produce     json
// @Param       request body request.Orchestrate true "Orchestration request"
// @Success     200 {object} response.RunStatus
// @Failure     400 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /orchestration/execute [post]
func (r *V1) orchestrate(ctx *fiber.Ctx) error {
	var req request.Orchestrate
	if err := ctx.BodyParser(&req); err != nil {
		r.l.Error(err, "restapi - v1 - orchestrate")

		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	if err := r.v.Struct(&req); err != nil {
		r.l.Error(err, "restapi - v1 - orchestrate - validation")

		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	status, err := r.o.ExecuteOrchestration(ctx.UserContext(), &entity.OrchestrationInput{
		RunID:          req.RunID,
		TeamSpec:       req.TeamSpec,
		Steps:          req.Steps,
		SystemPrompt:   req.SystemPrompt,
		Message:        req.Message,
		MaxDepth:       req.MaxDepth,
		ContinuePolicy: req.ContinuePolicy,
	})
	if err != nil {
		r.l.Error(err, "restapi - v1 - orchestrate")

		return errorResponse(ctx, http.StatusInternalServerError, "orchestration failed")
	}

	return ctx.Status(http.StatusOK).JSON(response.NewRunStatus(&status))
}

// @Summary     Get orchestration workflow status
// @Description Query the current status of an orchestration workflow
// @ID          orchestration-status
// @Tags        orchestration
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Success     200 {object} response.RunStatus
// @Failure     500 {object} response.Error
// @Router      /orchestration/status/{run_id} [get]
func (r *V1) orchestrationStatus(ctx *fiber.Ctx) error {
	runID := ctx.Params("run_id")

	status, err := r.o.GetOrchestrationStatus(ctx.UserContext(), runID)
	if err != nil {
		r.l.Error(err, "restapi - v1 - orchestrationStatus")

		return errorResponse(ctx, http.StatusInternalServerError, "query failed")
	}

	return ctx.Status(http.StatusOK).JSON(response.NewRunStatus(&status))
}
