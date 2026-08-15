package v1

import (
	"context"
	"net/http"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/gofiber/fiber/v2"
)

// RunEventReader reads a run's durable projected milestone timeline — the
// authoritative history of what the frontend saw, minus the transient byte
// deltas that never leave the bus history window.
type RunEventReader interface {
	ListRunEvents(ctx context.Context, runID string, after int64, limit int) ([]agentoscore.Event, error)
}

// @Summary     List AgentOS run event history
// @Description Query the durable projected milestone timeline of an AgentOS run.
// @ID          agentos-list-run-events
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       run_id path string true "Run ID"
// @Param       after_sequence query int false "Only return events after this sequence"
// @Param       limit query int false "Maximum events"
// @Success     200 {array} agentoscore.Event
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/runs/{run_id}/events/history [get]
// The history replays the run's projected milestones in sequence order.
func (r *V1) listAgentOSRunEvents(ctx *fiber.Ctx) error {
	if r.runEventReader == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos run event history is not configured")
	}

	var req request.AgentOSRunEventScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid run event scope")
	}

	if err := validateRESTQuery(r, &req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	events, err := r.runEventReader.ListRunEvents(ctx.UserContext(), ctx.Params("run_id"), req.AfterSequence, req.Limit)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(events)
}
