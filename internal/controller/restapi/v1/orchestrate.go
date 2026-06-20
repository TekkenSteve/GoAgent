package v1

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/response"
	"github.com/TekkenSteve/GoAgent/internal/entity"
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

	input, err := nativeOrchestrationInput(req)
	if err != nil {
		r.l.Error(err, "restapi - v1 - orchestrate - native decode")

		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	status, err := r.o.ExecuteOrchestration(ctx.UserContext(), input)
	if err != nil {
		r.l.Error(err, "restapi - v1 - orchestrate")

		return errorResponse(ctx, http.StatusInternalServerError, "orchestration failed")
	}

	return ctx.Status(http.StatusOK).JSON(response.NewRunStatus(&status))
}

func nativeOrchestrationInput(req request.Orchestrate) (*entity.OrchestrationInput, error) {
	teamSpec, err := decodeNativeTeamSpec(req.TeamSpec)
	if err != nil {
		return nil, err
	}
	steps, err := decodeNativeSteps(req.Steps)
	if err != nil {
		return nil, err
	}
	continuePolicy, err := decodeNativeContinuePolicy(req.ContinuePolicy)
	if err != nil {
		return nil, err
	}

	return &entity.OrchestrationInput{
		RunID:          req.RunID,
		TeamSpec:       teamSpec,
		Steps:          steps,
		SystemPrompt:   req.SystemPrompt,
		Message:        req.Message,
		MaxDepth:       req.MaxDepth,
		ContinuePolicy: continuePolicy,
	}, nil
}

func decodeNativeTeamSpec(data json.RawMessage) (*entity.TeamSpec, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var spec *entity.TeamSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("invalid team_spec: %w", err)
	}

	return spec, nil
}

func decodeNativeSteps(data json.RawMessage) ([]entity.Step, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var steps []entity.Step
	if err := json.Unmarshal(data, &steps); err != nil {
		return nil, fmt.Errorf("invalid steps: %w", err)
	}

	return steps, nil
}

func decodeNativeContinuePolicy(data json.RawMessage) (entity.ContinuePolicy, error) {
	if len(data) == 0 {
		return entity.ContinuePolicy{}, nil
	}

	var policy entity.ContinuePolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return entity.ContinuePolicy{}, fmt.Errorf("invalid continue_policy: %w", err)
	}

	return policy, nil
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
