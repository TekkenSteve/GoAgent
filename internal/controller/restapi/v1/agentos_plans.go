package v1

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
	"github.com/gofiber/fiber/v2"
)

const (
	agentOSPlanEventStreamContentType = "text/event-stream"
	agentOSPlanEventStreamErrorType   = "error"
)

// @Summary     Start AgentOS plan
// @Description Start a durable cross-backend AgentOS RunPlan.
// @ID          agentos-start-plan
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       request body agentos.RunPlanSpec true "AgentOS run plan request"
// @Success     202 {object} agentos.RunPlanStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans [post]
func (r *V1) startAgentOSPlan(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var spec agentos.RunPlanSpec
	if err := ctx.BodyParser(&spec); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	status, err := r.planRuntime.StartPlan(ctx.UserContext(), spec)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusAccepted).JSON(status)
}

// @Summary     Get AgentOS plan status
// @Description Query the current aggregate status of an AgentOS RunPlan.
// @ID          agentos-plan-status
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Success     200 {object} agentos.RunPlanStatus
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/status [get]
func (r *V1) statusAgentOSPlan(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid plan scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	status, err := r.planRuntime.StatusPlan(ctx.UserContext(), agentos.PlanRef{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(status)
}

// @Summary     Describe AgentOS plan
// @Description Query the current public topology and aggregate status of an AgentOS RunPlan.
// @ID          agentos-plan-description
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string false "Project ID"
// @Success     200 {object} agentos.RunPlanDescription
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/description [get]
func (r *V1) describeAgentOSPlan(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid plan scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	description, err := r.planRuntime.DescribePlan(ctx.UserContext(), agentos.PlanRef{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(description)
}

// @Summary     Signal AgentOS plan
// @Description Send plan-level business input such as retry, approve, or reject.
// @ID          agentos-signal-plan
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       request body request.AgentOSPlanSignal true "AgentOS plan signal"
// @Success     202
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/signals [post]
func (r *V1) signalAgentOSPlan(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanSignal
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	if err := r.planRuntime.SignalPlan(ctx.UserContext(), agentos.PlanRef{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
	}, agentos.Signal{
		Type:           req.Type,
		IdempotencyKey: req.IdempotencyKey,
		ActorID:        req.ActorID,
		Payload:        req.Payload,
		SentAt:         req.SentAt,
	}); err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.SendStatus(http.StatusAccepted)
}

// @Summary     Control AgentOS plan
// @Description Send lifecycle control such as pause, resume, or cancel to an AgentOS RunPlan.
// @ID          agentos-control-plan
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       request body request.AgentOSPlanControl true "AgentOS plan control operation"
// @Success     202
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/control [post]
func (r *V1) controlAgentOSPlan(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanControl
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	if err := r.planRuntime.ControlPlan(ctx.UserContext(), agentos.PlanRef{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
	}, agentos.ControlRequest{
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

// @Summary     List AgentOS plan audits
// @Description Query durable audit records for plan control-plane actions.
// @ID          agentos-list-plan-audits
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string false "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       action query string false "Audit action"
// @Param       limit query int false "Maximum records"
// @Success     200 {array} agentos.PlanAuditRecord
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/audits [get]
func (r *V1) listAgentOSPlanAudits(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanAuditScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid audit scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	records, err := r.planRuntime.ListPlanAudits(ctx.UserContext(), agentos.PlanAuditScope{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
		NodeID:    req.NodeID,
		RunID:     req.RunID,
		Action:    req.Action,
		Limit:     req.Limit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(records)
}

// @Summary     List AgentOS plan artifacts
// @Description Query durable artifact refs for a RunPlan.
// @ID          agentos-list-plan-artifacts
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string false "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       limit query int false "Maximum refs"
// @Success     200 {array} agentos.ArtifactRef
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/artifacts [get]
func (r *V1) listAgentOSPlanArtifacts(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanArtifactScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid artifact scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	refs, err := r.planRuntime.ListPlanArtifacts(ctx.UserContext(), agentos.PlanArtifactScope{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
		NodeID:    req.NodeID,
		RunID:     req.RunID,
		Limit:     req.Limit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(refs)
}

// @Summary     Get AgentOS plan artifact
// @Description Read one durable artifact document for a RunPlan.
// @ID          agentos-get-plan-artifact
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       artifact_id path string true "Artifact ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string false "Project ID"
// @Success     200 {object} agentos.Artifact
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/artifacts/{artifact_id} [get]
func (r *V1) getAgentOSPlanArtifact(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid artifact scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	artifact, err := r.planRuntime.GetPlanArtifact(ctx.UserContext(), agentos.PlanArtifactScope{
		PlanID:     ctx.Params("plan_id"),
		AccountID:  req.AccountID,
		ProjectID:  req.ProjectID,
		ArtifactID: ctx.Params("artifact_id"),
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(artifact)
}

// @Summary     List AgentOS plan event history
// @Description Query durable RunPlan events for timeline and debug views.
// @ID          agentos-list-plan-events
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string false "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       after_sequence query int false "Only return events after this sequence"
// @Param       limit query int false "Maximum events"
// @Success     200 {array} agentos.PlanEvent
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/events/history [get]
func (r *V1) listAgentOSPlanEvents(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanEventScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid event scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	events, err := r.planRuntime.ListPlanEvents(ctx.UserContext(), agentos.PlanEventScope{
		PlanID:        ctx.Params("plan_id"),
		AccountID:     req.AccountID,
		ProjectID:     req.ProjectID,
		NodeID:        req.NodeID,
		RunID:         req.RunID,
		AfterSequence: req.AfterSequence,
		Limit:         req.Limit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(events)
}

// @Summary     List AgentOS plan debug traces
// @Description Query typed debug traces projected from durable RunPlan events.
// @ID          agentos-list-plan-debug-traces
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string false "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       after_sequence query int false "Only return traces after this sequence"
// @Param       limit query int false "Maximum traces"
// @Success     200 {array} agentos.PlanDebugTrace
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/debug/traces [get]
func (r *V1) listAgentOSPlanDebugTraces(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanDebugTraceScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid debug trace scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	traces, err := r.planRuntime.ListPlanDebugTraces(ctx.UserContext(), agentos.PlanDebugTraceScope{
		PlanID:        ctx.Params("plan_id"),
		AccountID:     req.AccountID,
		ProjectID:     req.ProjectID,
		NodeID:        req.NodeID,
		RunID:         req.RunID,
		AfterSequence: req.AfterSequence,
		Limit:         req.Limit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(traces)
}

// @Summary     Stream AgentOS plan events
// @Description Stream durable RunPlan events as Server-Sent Events.
// @ID          agentos-stream-plan-events
// @Tags        agentos
// @Accept      json
// @Produce     text/event-stream
// @Param       plan_id path string true "Plan ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       after_sequence query int false "Replay events after this sequence"
// @Success     200 {string} string "SSE stream"
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/events [get]
func (r *V1) streamAgentOSPlanEvents(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanStreamScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid stream scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	sub, err := r.planRuntime.SubscribePlan(ctx.UserContext(), agentos.PlanStreamScope{
		PlanID:        ctx.Params("plan_id"),
		AccountID:     req.AccountID,
		ProjectID:     req.ProjectID,
		NodeID:        req.NodeID,
		RunID:         req.RunID,
		AfterSequence: req.AfterSequence,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	ctx.Set(fiber.HeaderContentType, agentOSPlanEventStreamContentType)
	ctx.Set(fiber.HeaderCacheControl, "no-cache")
	ctx.Set(fiber.HeaderConnection, "keep-alive")
	ctx.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer sub.Close()
		for event := range sub.Events() {
			if !writeAgentOSEventSSE(w, event) {
				return
			}
		}
	})

	return nil
}

func writeAgentOSEventSSE(w *bufio.Writer, event agentos.Event) bool {
	data, err := json.Marshal(event)
	if err != nil {
		return writeAgentOSStreamError(w, err)
	}

	if err := sse.WriteEvent(w, sse.Event{
		LastEventID: eventSSEID(event),
		Type:        string(event.EventType),
		Data:        string(data),
	}); err != nil {
		return false
	}

	return w.Flush() == nil
}

func writeAgentOSStreamError(w *bufio.Writer, err error) bool {
	if err := sse.WriteEvent(w, sse.Event{
		Type: agentOSPlanEventStreamErrorType,
		Data: err.Error(),
	}); err != nil {
		return false
	}

	return w.Flush() == nil
}

func eventSSEID(event agentos.Event) string {
	if event.EventID != "" {
		return event.EventID
	}
	if event.Sequence > 0 {
		return strconv.FormatInt(event.Sequence, 10)
	}

	return ""
}
