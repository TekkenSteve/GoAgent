package v1

import (
	"errors"
	"net/http"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/gofiber/fiber/v2"
)

// @Summary     Start AgentOS run
// @Description Start a generic AgentOS run on the selected backend.
// @ID          agentos-start-run
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       request body request.AgentOSStart true "AgentOS run request"
// @Success     202 {object} agentos.RunStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/runs [post]
func (r *V1) startAgentOSRun(ctx *fiber.Ctx) error {
	if r.agentOSRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos runtime is not configured")
	}

	var req request.AgentOSStart
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}
	if isNativeAgentOSBackend(req.Backend) && req.UserMessage == "" {
		return errorResponse(ctx, http.StatusBadRequest, "user_message is required for native backend")
	}

	status, err := r.agentOSRuntime.Start(ctx.UserContext(), agentos.RunSpec{
		RunID:          req.RunID,
		ThreadID:       req.ThreadID,
		AccountID:      req.AccountID,
		ProjectID:      req.ProjectID,
		AgentID:        req.AgentID,
		ModelRef:       req.ModelRef,
		SystemPrompt:   req.SystemPrompt,
		UserMessage:    req.UserMessage,
		IdempotencyKey: req.IdempotencyKey,
		RequestedAt:    req.RequestedAt,
		Metadata:       req.Metadata,
		Backend:        req.Backend,
		Input:          req.Input,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusAccepted).JSON(status)
}

func isNativeAgentOSBackend(ref agentos.BackendRef) bool {
	return ref.Kind == agentos.BackendKindNative
}

// @Summary     Signal AgentOS run
// @Description Send business input such as user.message to an AgentOS run.
// @ID          agentos-signal-run
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Param       request body request.AgentOSSignal true "AgentOS signal"
// @Success     202
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/runs/{run_id}/signals [post]
func (r *V1) signalAgentOSRun(ctx *fiber.Ctx) error {
	if r.agentOSRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos runtime is not configured")
	}

	var req request.AgentOSSignal
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	err := r.agentOSRuntime.Signal(ctx.UserContext(), ctx.Params("run_id"), agentos.Signal{
		Type:           req.Type,
		IdempotencyKey: req.IdempotencyKey,
		ActorID:        req.ActorID,
		Payload:        req.Payload,
		SentAt:         req.SentAt,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.SendStatus(http.StatusAccepted)
}

// @Summary     Control AgentOS run
// @Description Send lifecycle control such as pause, resume, or cancel to an AgentOS run.
// @ID          agentos-control-run
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Param       request body request.AgentOSControl true "AgentOS control operation"
// @Success     202
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/runs/{run_id}/control [post]
func (r *V1) controlAgentOSRun(ctx *fiber.Ctx) error {
	if r.agentOSRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos runtime is not configured")
	}

	var req request.AgentOSControl
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	if err := r.agentOSRuntime.Control(ctx.UserContext(), ctx.Params("run_id"), agentos.ControlRequest{
		Operation:      req.Operation,
		IdempotencyKey: req.IdempotencyKey,
		RequestedAt:    req.RequestedAt,
		ActorID:        req.ActorID,
		Metadata:       req.Metadata,
	}); err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.SendStatus(http.StatusAccepted)
}

// @Summary     Get AgentOS run status
// @Description Query the current status of an AgentOS run.
// @ID          agentos-run-status
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Success     200 {object} agentos.RunStatus
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/runs/{run_id}/status [get]
func (r *V1) statusAgentOSRun(ctx *fiber.Ctx) error {
	if r.agentOSRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos runtime is not configured")
	}

	status, err := r.agentOSRuntime.Status(ctx.UserContext(), ctx.Params("run_id"))
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(status)
}

func agentOSError(ctx *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, agentos.ErrInvalidRunSpec),
		errors.Is(err, agentos.ErrInvalidBackendRef),
		errors.Is(err, agentos.ErrInvalidSignal),
		errors.Is(err, agentos.ErrInvalidControlOperation),
		errors.Is(err, agentos.ErrInvalidStreamScope),
		errors.Is(err, agentos.ErrInvalidPlanScope),
		errors.Is(err, agentos.ErrInvalidRunPlan),
		errors.Is(err, agentos.ErrInvalidArtifact),
		errors.Is(err, agentos.ErrInvalidExpression),
		errors.Is(err, agentos.ErrInvalidPlanEvent):
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	case errors.Is(err, agentos.ErrBackendNotFound),
		errors.Is(err, agentos.ErrRunRouteNotFound),
		errors.Is(err, agentos.ErrPlanRouteNotFound),
		errors.Is(err, agentos.ErrCapabilityNotFound),
		errors.Is(err, agentos.ErrArtifactNotFound):
		return errorResponse(ctx, http.StatusNotFound, err.Error())
	default:
		return errorResponse(ctx, http.StatusInternalServerError, err.Error())
	}
}
