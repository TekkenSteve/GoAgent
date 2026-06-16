package v1

import (
	"errors"
	"net/http"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/gofiber/fiber/v2"
)

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
		Payload:        req.Payload,
		SentAt:         req.SentAt,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.SendStatus(http.StatusAccepted)
}

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

	if err := r.agentOSRuntime.Control(ctx.UserContext(), ctx.Params("run_id"), req.Operation); err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.SendStatus(http.StatusAccepted)
}

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
		errors.Is(err, agentos.ErrInvalidControlOperation):
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	case errors.Is(err, agentos.ErrBackendNotFound),
		errors.Is(err, agentos.ErrRunRouteNotFound):
		return errorResponse(ctx, http.StatusNotFound, err.Error())
	default:
		return errorResponse(ctx, http.StatusInternalServerError, err.Error())
	}
}
