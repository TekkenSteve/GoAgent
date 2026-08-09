package v1

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
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
// The plan is started on its declared backend and tracked durably.
func (r *V1) startAgentOSPlan(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var spec agentos.RunPlanSpec
	if err := ctx.BodyParser(&spec); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	status, err := r.planRuntime.StartPlan(ctx.UserContext(), &spec)
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
// The status aggregates node outcomes, signals, and events for the plan.
func (r *V1) statusAgentOSPlan(ctx *fiber.Ctx) error {
	return r.withPlanRef(ctx, func(ref agentos.PlanRef) error {
		status, err := r.planRuntime.StatusPlan(ctx.UserContext(), ref)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(status)
	})
}

// @Summary     Describe AgentOS plan
// @Description Query the current public topology and aggregate status of an AgentOS RunPlan.
// @ID          agentos-plan-description
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Success     200 {object} agentos.RunPlanDescription
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/description [get]
// The description exposes the plan topology and aggregate status.
func (r *V1) describeAgentOSPlan(ctx *fiber.Ctx) error {
	return r.withPlanRef(ctx, func(ref agentos.PlanRef) error {
		description, err := r.planRuntime.DescribePlan(ctx.UserContext(), ref)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(description)
	})
}

func (r *V1) withPlanRef(ctx *fiber.Ctx, fn func(agentos.PlanRef) error) error {
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

	if err := req.Validate(); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	return fn(agentos.PlanRef{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
	})
}

func withPlanBody[T any](r *V1, ctx *fiber.Ctx, fn func(T) error) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req T
	if err := ctx.BodyParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	if err := fn(req); err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.SendStatus(http.StatusAccepted)
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
// The signal is forwarded to the active plan run for business input.
func (r *V1) signalAgentOSPlan(ctx *fiber.Ctx) error {
	return withPlanActionBody(r, ctx, func(req request.AgentOSPlanSignal, ref agentos.PlanRef) error {
		signal := agentoscore.Signal{
			Type:           req.Type,
			IdempotencyKey: req.IdempotencyKey,
			ActorID:        req.ActorID,
			Payload:        req.Payload,
			SentAt:         req.SentAt,
		}

		return r.planRuntime.SignalPlan(ctx.UserContext(), ref, &signal)
	})
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
// The control operation is applied to the active plan run.
func (r *V1) controlAgentOSPlan(ctx *fiber.Ctx) error {
	return withPlanActionBody(r, ctx, func(req request.AgentOSPlanControl, ref agentos.PlanRef) error {
		control := agentoscore.ControlRequest{
			Operation:      req.Operation,
			IdempotencyKey: req.IdempotencyKey,
			RequestedAt:    req.RequestedAt,
			ActorID:        req.ActorID,
			Metadata:       req.Metadata,
		}

		return r.planRuntime.ControlPlan(ctx.UserContext(), ref, &control)
	})
}

type planScopedBody interface {
	GetAccountID() string
	GetProjectID() string
}

func withPlanActionBody[T any, PT interface {
	*T
	planScopedBody
}](r *V1, ctx *fiber.Ctx, fn func(T, agentos.PlanRef) error) error {
	return withPlanBody(r, ctx, func(req T) error {
		scoped := PT(&req)
		ref := agentos.PlanRef{
			PlanID:    ctx.Params("plan_id"),
			AccountID: scoped.GetAccountID(),
			ProjectID: scoped.GetProjectID(),
		}

		return fn(req, ref)
	})
}

func withQueryScope[T any](r *V1, ctx *fiber.Ctx, scopeName string, fn func(T) (any, error)) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req T
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid "+scopeName)
	}

	if err := validateRESTQuery(r, &req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	result, err := fn(req)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(result)
}

// @Summary     List AgentOS plan audits
// @Description Query durable audit records for plan control-plane actions.
// @ID          agentos-list-plan-audits
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       action query string false "Audit action"
// @Param       limit query int false "Maximum records"
// @Success     200 {array} agentos.PlanAuditRecord
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/audits [get]
// The audit trail records plan mutations and node outcomes.
func (r *V1) listAgentOSPlanAudits(ctx *fiber.Ctx) error {
	return withQueryScope(r, ctx, "audit scope", func(req request.AgentOSPlanAuditScope) (any, error) {
		return r.planRuntime.ListPlanAudits(ctx.UserContext(), &agentos.PlanAuditScope{
			PlanID:    ctx.Params("plan_id"),
			AccountID: req.AccountID,
			ProjectID: req.ProjectID,
			NodeID:    req.NodeID,
			RunID:     req.RunID,
			Action:    req.Action,
			Limit:     req.Limit,
		})
	})
}

// @Summary     List AgentOS plan artifacts
// @Description Query durable artifact refs for a RunPlan.
// @ID          agentos-list-plan-artifacts
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       limit query int false "Maximum refs"
// @Success     200 {array} agentoscore.ArtifactRef
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/artifacts [get]
// The artifacts index lists refs produced by the plan's nodes.
func (r *V1) listAgentOSPlanArtifacts(ctx *fiber.Ctx) error {
	return withQueryScope(r, ctx, "artifact scope", func(req request.AgentOSPlanArtifactScope) (any, error) {
		return r.planRuntime.ListPlanArtifacts(ctx.UserContext(), &agentos.PlanArtifactScope{
			PlanID:    ctx.Params("plan_id"),
			AccountID: req.AccountID,
			ProjectID: req.ProjectID,
			NodeID:    req.NodeID,
			RunID:     req.RunID,
			Limit:     req.Limit,
		})
	})
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
// @Param       project_id query string true "Project ID"
// @Success     200 {object} agentoscore.Artifact
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/artifacts/{artifact_id} [get]
// The artifact payload is served from the plan's artifact store.
func (r *V1) getAgentOSPlanArtifact(ctx *fiber.Ctx) error {
	return withQueryScope(r, ctx, "artifact scope", func(req request.AgentOSPlanScope) (any, error) {
		return r.planRuntime.GetPlanArtifact(ctx.UserContext(), &agentos.PlanArtifactScope{
			PlanID:     ctx.Params("plan_id"),
			AccountID:  req.AccountID,
			ProjectID:  req.ProjectID,
			ArtifactID: ctx.Params("artifact_id"),
		})
	})
}

// @Summary     List AgentOS plan event history
// @Description Query durable RunPlan events for timeline and debug views.
// @ID          agentos-list-plan-events
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       after_sequence query int false "Only return events after this sequence"
// @Param       limit query int false "Maximum events"
// @Success     200 {array} agentos.PlanEvent
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/events/history [get]
// The history replays plan events in sequence order.
func (r *V1) listAgentOSPlanEvents(ctx *fiber.Ctx) error {
	return withQueryScope(r, ctx, "event scope", func(req request.AgentOSPlanEventScope) (any, error) {
		return r.planRuntime.ListPlanEvents(ctx.UserContext(), &agentos.PlanEventScope{
			PlanID:        ctx.Params("plan_id"),
			AccountID:     req.AccountID,
			ProjectID:     req.ProjectID,
			NodeID:        req.NodeID,
			RunID:         req.RunID,
			AfterSequence: req.AfterSequence,
			Limit:         req.Limit,
		})
	})
}

// @Summary     List AgentOS plan debug traces
// @Description Query typed debug traces projected from durable RunPlan events.
// @ID          agentos-list-plan-debug-traces
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       node_id query string false "Node ID"
// @Param       run_id query string false "Child run ID"
// @Param       after_sequence query int false "Only return traces after this sequence"
// @Param       limit query int false "Maximum traces"
// @Success     200 {array} agentos.PlanDebugTrace
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/debug/traces [get]
// The traces expose debug information for plan runs.
func (r *V1) listAgentOSPlanDebugTraces(ctx *fiber.Ctx) error {
	return withQueryScope(r, ctx, "debug trace scope", func(req request.AgentOSPlanDebugTraceScope) (any, error) {
		return r.planRuntime.ListPlanDebugTraces(ctx.UserContext(), &agentos.PlanDebugTraceScope{
			PlanID:        ctx.Params("plan_id"),
			AccountID:     req.AccountID,
			ProjectID:     req.ProjectID,
			NodeID:        req.NodeID,
			RunID:         req.RunID,
			AfterSequence: req.AfterSequence,
			Limit:         req.Limit,
		})
	})
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
// The event stream replays plan events over SSE from the after_sequence cursor.
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

	sub, err := r.planRuntime.SubscribePlan(ctx.UserContext(), &agentos.PlanStreamScope{
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
		defer func() {
			if err := sub.Close(); err != nil {
				log.Printf("agentos plan stream: close subscription: %v", err)
			}
		}()

		for event := range sub.Events() {
			if !writeAgentOSEventSSE(w, &event) {
				return
			}
		}
	})

	return nil
}

func writeAgentOSEventSSE(w *bufio.Writer, event *agentoscore.Event) bool {
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

func eventSSEID(event *agentoscore.Event) string {
	if event.EventID != "" {
		return event.EventID
	}

	if event.Sequence > 0 {
		return strconv.FormatInt(event.Sequence, 10)
	}

	return ""
}
