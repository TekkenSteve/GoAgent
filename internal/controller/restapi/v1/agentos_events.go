package v1

import (
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/eventing"
	"github.com/gofiber/fiber/v2"
)

type ingestAgentOSEventResponse struct {
	RunID     string `json:"run_id"`
	EventID   string `json:"event_id"`
	Sequence  int64  `json:"sequence"`
	Duplicate bool   `json:"duplicate"`
}

// @Summary     Ingest AgentOS event
// @Description Accept a normalized event from an external AgentOS backend.
// @ID          agentos-ingest-event
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Param       request body eventing.IngestEvent true "AgentOS event"
// @Success     202 {object} ingestAgentOSEventResponse
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/runs/{run_id}/events [post]
func (r *V1) ingestAgentOSEvent(c *fiber.Ctx) error {
	if r.eventIngest == nil {
		return errorResponse(c, fiber.StatusNotFound, "agentos event ingest is not configured")
	}

	runID := c.Params("run_id")
	var req eventing.IngestEvent
	if err := c.BodyParser(&req); err != nil {
		return errorResponse(c, fiber.StatusBadRequest, "invalid event body")
	}
	if req.RunID == "" {
		req.RunID = runID
	}
	if req.RunID != runID {
		return errorResponse(c, fiber.StatusBadRequest, "run_id in path and body must match")
	}

	result, err := r.eventIngest.Ingest(c.UserContext(), req)
	if err != nil {
		if errors.Is(err, eventing.ErrInvalidEvent) {
			return errorResponse(c, fiber.StatusBadRequest, err.Error())
		}

		return errorResponse(c, fiber.StatusInternalServerError, fmt.Sprintf("ingest event: %v", err))
	}

	return c.Status(fiber.StatusAccepted).JSON(ingestAgentOSEventResponse{
		RunID:     result.RunID,
		EventID:   result.EventID,
		Sequence:  result.Sequence,
		Duplicate: result.Duplicate,
	})
}
