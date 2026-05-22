package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentfw/agent"
	"github.com/TekkenSteve/GoAgent/agentfw/team"
	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/usecase"
)

var ErrUnknownOperation = errors.New("unknown operation")

// UseCase -.
type UseCase struct {
	temporal usecase.ExecutorRepo
}

// New -.
func New(r usecase.ExecutorRepo) *UseCase {
	return &UseCase{temporal: r}
}

// Execute -.
func (uc *UseCase) Execute(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
	status, err := uc.temporal.StartExecution(ctx, req)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("UseCase - Execute - uc.temporal.StartExecution: %w", err)
	}

	return status, nil
}

// ExecuteOrchestration starts an OrchestrationWorkflow from a TeamSpec or step queue.
// If TeamSpec is provided (with no pre-expanded Steps), it expands the team hierarchy
// into a flat step queue before starting the workflow.
func (uc *UseCase) ExecuteOrchestration(ctx context.Context, input *entity.OrchestrationInput) (entity.RunStatus, error) {
	// TeamSpec must be expanded into Steps before the workflow starts
	// (the OrchestrationWorkflow rejects raw TeamSpec).
	if input.TeamSpec != nil && len(input.Steps) == 0 {
		registry := agent.NewRegistry()

		expanded, err := team.Expand(input.TeamSpec, registry)
		if err != nil {
			return entity.RunStatus{}, fmt.Errorf("UseCase - ExecuteOrchestration - team.Expand: %w", err)
		}

		input.Steps = expanded
		input.TeamSpec = nil
	}

	status, err := uc.temporal.StartOrchestration(ctx, input)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("UseCase - ExecuteOrchestration - uc.temporal.StartOrchestration: %w", err)
	}

	return status, nil
}

// GetStatus -.
func (uc *UseCase) GetStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	status, err := uc.temporal.GetStatus(ctx, runID)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("UseCase - GetStatus - uc.temporal.GetStatus: %w", err)
	}

	return status, nil
}

// GetOrchestrationStatus queries the status of an OrchestrationWorkflow.
func (uc *UseCase) GetOrchestrationStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	status, err := uc.temporal.GetOrchestrationStatus(ctx, runID)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("UseCase - GetOrchestrationStatus - uc.temporal.GetOrchestrationStatus: %w", err)
	}

	return status, nil
}

// Control -.
func (uc *UseCase) Control(ctx context.Context, runID string, op entity.ControlOperation) error {
	var err error

	switch op {
	case entity.ControlPause:
		err = uc.temporal.Pause(ctx, runID)
	case entity.ControlResume:
		err = uc.temporal.Resume(ctx, runID)
	case entity.ControlCancel:
		err = uc.temporal.Cancel(ctx, runID)
	default:
		return fmt.Errorf("UseCase - Control - %w: %s", ErrUnknownOperation, op)
	}

	if err != nil {
		return fmt.Errorf("UseCase - Control - uc.temporal.%s: %w", op, err)
	}

	return nil
}
