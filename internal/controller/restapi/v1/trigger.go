package v1

import (
	"fmt"
	"net/http"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/response"
	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// triggerWebhookHandler handles event-trigger webhook POSTs.
type triggerWebhookHandler struct {
	eh usecase.TriggerEventHandler
	t  usecase.AgentExecutor
	l  logger.Interface
}

// handleEvent receives a webhook event, resolves matching triggers, and starts agent workflows.
// POST /v1/triggers/events
func (h *triggerWebhookHandler) handleEvent(ctx *fiber.Ctx) error {
	var req request.EventWebhook
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	if req.EventSlug == "" {
		return errorResponse(ctx, http.StatusBadRequest, "event_slug is required")
	}

	if req.Payload == nil {
		req.Payload = make(map[string]string)
	}

	// Resolve matching triggers via usecase
	results, err := h.eh.HandleEvent(ctx.UserContext(), req.EventSlug, req.Payload)
	if err != nil {
		h.l.Warn("trigger webhook: %v", err)

		return errorResponse(ctx, http.StatusNotFound, fmt.Sprintf("no triggers matched event: %s", req.EventSlug))
	}

	// Start an agent workflow for each resolved trigger
	fired := make([]response.TriggerFireResult, 0, len(results))

	var lastErr error

	for _, r := range results {
		runID := r.RunID
		if runID == "" {
			runID = uuid.New().String()
		}

		status, execErr := h.t.Execute(ctx.UserContext(), &entity.ExecuteRequest{
			RunID:        runID,
			SystemPrompt: r.SystemPrompt,
			UserMessage:  r.Message,
			ModelRef:     r.ModelRef,
		})
		if execErr != nil {
			lastErr = execErr
			h.l.Error("trigger webhook - start workflow for trigger %s: %v", r.TriggerID, execErr)

			continue
		}

		_ = status

		fired = append(fired, response.NewTriggerFireResult(&r))
	}

	if len(fired) == 0 && lastErr != nil {
		return errorResponse(ctx, http.StatusInternalServerError, fmt.Sprintf("failed to start workflows: %v", lastErr))
	}

	return ctx.JSON(response.EventWebhookResponse{
		EventSlug: req.EventSlug,
		Fired:     fired,
		Count:     len(fired),
	})
}
