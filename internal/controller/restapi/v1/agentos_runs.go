package v1

import (
	"errors"
	"net/http"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
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
// The run is started on its declared backend.
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

	spec := agentos.RunSpec{
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
	}

	status, err := r.agentOSRuntime.Start(ctx.UserContext(), &spec)
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
// The signal is forwarded to the active run.
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

	signal := agentoscore.Signal{
		Type:           req.Type,
		IdempotencyKey: req.IdempotencyKey,
		ActorID:        req.ActorID,
		Payload:        req.Payload,
		SentAt:         req.SentAt,
	}

	err := r.agentOSRuntime.Signal(ctx.UserContext(), ctx.Params("run_id"), &signal)
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
// The control operation is applied to the run.
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

	control := agentoscore.ControlRequest{
		Operation:      req.Operation,
		IdempotencyKey: req.IdempotencyKey,
		RequestedAt:    req.RequestedAt,
		ActorID:        req.ActorID,
		Metadata:       req.Metadata,
	}
	if err := r.agentOSRuntime.Control(ctx.UserContext(), ctx.Params("run_id"), &control); err != nil {
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
// The status aggregates run state and events.
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
	case errors.Is(err, agentoscore.ErrInvalidRunSpec),
		errors.Is(err, agentoscore.ErrInvalidBackendRef),
		errors.Is(err, agentoscore.ErrInvalidSignal),
		errors.Is(err, agentoscore.ErrInvalidControlOperation),
		errors.Is(err, agentoscore.ErrInvalidStreamScope),
		errors.Is(err, agentoscore.ErrInvalidPlanScope),
		errors.Is(err, agentoscore.ErrInvalidRunPlan),
		errors.Is(err, agentoscore.ErrInvalidArtifact),
		errors.Is(err, agentoscore.ErrInvalidExpression),
		errors.Is(err, agentoscore.ErrInvalidPlanEvent),
		errors.Is(err, agentoscore.ErrInvalidResourceRef),
		errors.Is(err, agentoscore.ErrInvalidProcess),
		errors.Is(err, agentoscore.ErrInvalidProcessScope),
		errors.Is(err, agentoscore.ErrInvalidLedgerEntry),
		errors.Is(err, agentoscore.ErrInvalidLedgerScope),
		errors.Is(err, agentoscore.ErrInvalidGovernedAction),
		errors.Is(err, agentoscore.ErrInvalidGovernedActionScope),
		errors.Is(err, agentoscore.ErrInvalidWorkset),
		errors.Is(err, agentoscore.ErrInvalidWorksetScope):
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	case errors.Is(err, agentoscore.ErrBackendNotFound),
		errors.Is(err, agentoscore.ErrRunRouteNotFound),
		errors.Is(err, agentoscore.ErrPlanRouteNotFound),
		errors.Is(err, agentoscore.ErrCapabilityNotFound),
		errors.Is(err, agentoscore.ErrArtifactNotFound):
		return errorResponse(ctx, http.StatusNotFound, err.Error())
	default:
		return errorResponse(ctx, http.StatusInternalServerError, err.Error())
	}
}
