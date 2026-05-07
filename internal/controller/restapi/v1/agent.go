package v1

import (
	"net/http"
	"strconv"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/response"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/gofiber/fiber/v2"
)

// @Summary     Execute agent run
// @Description Start a new agent workflow run
// @ID          execute
// @Tags        agent
// @Accept      json
// @Produce     json
// @Param       request body request.Execute true "Agent execution request"
// @Success     200 {object} response.RunStatus
// @Failure     400 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agent/execute [post]
func (r *V1) execute(ctx *fiber.Ctx) error {
	var req request.Execute
	if err := ctx.BodyParser(&req); err != nil {
		r.l.Error(err, "restapi - v1 - execute")
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	status, err := r.t.Execute(ctx.UserContext(), entity.ExecuteRequest{
		RunID:          req.RunID,
		ThreadID:        req.ThreadID,
		ProjectID:       req.ProjectID,
		AccountID:       req.AccountID,
		ModelRef:        req.ModelRef,
		AgentID:         req.AgentID,
		AgentVersionID:  req.AgentVersionID,
		ToolSchemaVer:   req.ToolSchemaVer,
		UserMessage:     req.UserMessage,
		IsNewThread:     req.IsNewThread,
		BypassAdmission: req.BypassAdmission,
		IdempotencyKey:  req.IdempotencyKey,
		EventSchemaVer:  req.EventSchemaVer,
		WorkflowVersion: req.WorkflowVersion,
	})
	if err != nil {
		r.l.Error(err, "restapi - v1 - execute")
		return errorResponse(ctx, http.StatusInternalServerError, "execution failed")
	}

	return ctx.Status(http.StatusOK).JSON(response.NewRunStatus(status))
}

// @Summary     Get run status
// @Description Get the current status of an agent run
// @ID          status
// @Tags        agent
// @Accept      json
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Success     200 {object} response.RunStatus
// @Failure     500 {object} response.Error
// @Router      /agent/status/{run_id} [get]
func (r *V1) status(ctx *fiber.Ctx) error {
	runID := ctx.Params("run_id")
	status, err := r.t.GetStatus(ctx.UserContext(), runID)
	if err != nil {
		r.l.Error(err, "restapi - v1 - status")
		return errorResponse(ctx, http.StatusInternalServerError, "query failed")
	}

	return ctx.Status(http.StatusOK).JSON(response.NewRunStatus(status))
}

func (r *V1) listMessages(ctx *fiber.Ctx) error {
	runID := ctx.Params("run_id")
	if runID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "missing run_id")
	}

	limit, _ := strconv.ParseUint(ctx.Query("limit", "50"), 10, 64)
	offset, _ := strconv.ParseUint(ctx.Query("offset", "0"), 10, 64)

	records, err := r.h.ListMessages(ctx.UserContext(), runID, limit, offset)
	if err != nil {
		r.l.Error(err, "restapi - v1 - listMessages")
		return errorResponse(ctx, http.StatusInternalServerError, "query failed")
	}

	if records == nil {
		records = []entity.MessageRecord{}
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"data":   records,
		"limit":  limit,
		"offset": offset,
	})
}

func (r *V1) listToolResults(ctx *fiber.Ctx) error {
	runID := ctx.Params("run_id")
	if runID == "" {
		return errorResponse(ctx, http.StatusBadRequest, "missing run_id")
	}

	limit, _ := strconv.ParseUint(ctx.Query("limit", "50"), 10, 64)
	offset, _ := strconv.ParseUint(ctx.Query("offset", "0"), 10, 64)

	records, err := r.h.ListToolResults(ctx.UserContext(), runID, limit, offset)
	if err != nil {
		r.l.Error(err, "restapi - v1 - listToolResults")
		return errorResponse(ctx, http.StatusInternalServerError, "query failed")
	}

	if records == nil {
		records = []entity.ToolResultRecord{}
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"data":   records,
		"limit":  limit,
		"offset": offset,
	})
}
