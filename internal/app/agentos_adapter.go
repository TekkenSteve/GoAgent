package app

import (
	"context"
	"errors"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
)

var errAgentOSExecutorUnknownControl = errors.New("agentos executor - unknown control operation")

type agentOSExecutor struct {
	runtime agentos.Runtime
}

func newAgentOSExecutor(runtime agentos.Runtime) *agentOSExecutor {
	return &agentOSExecutor{runtime: runtime}
}

func (e *agentOSExecutor) Execute(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
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
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}

	status, err := e.runtime.Start(ctx, &spec)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("agentos executor - execute: %w", err)
	}

	return runStatusToEntity(&status), nil
}

func (e *agentOSExecutor) GetStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	status, err := e.runtime.Status(ctx, runID)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("agentos executor - status: %w", err)
	}

	return runStatusToEntity(&status), nil
}

func (e *agentOSExecutor) Control(ctx context.Context, runID string, op entity.ControlOperation) error {
	agentOSOp, err := controlOperationToAgentOS(op)
	if err != nil {
		return err
	}

	control := agentoscore.ControlRequest{Operation: agentOSOp}
	if err := e.runtime.Control(ctx, runID, &control); err != nil {
		return fmt.Errorf("agentos executor - control: %w", err)
	}

	return nil
}

func runStatusToEntity(status *agentos.RunStatus) entity.RunStatus {
	var step int32
	if status.Progress != nil {
		step = status.Progress.Current
	}

	return entity.RunStatus{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           step,
		Reason:         status.Reason,
		UpdatedAt:      status.UpdatedAt,
	}
}

func controlOperationToAgentOS(op entity.ControlOperation) (agentoscore.ControlOperation, error) {
	switch op {
	case entity.ControlPause:
		return agentoscore.ControlPause, nil
	case entity.ControlResume:
		return agentoscore.ControlResume, nil
	case entity.ControlCancel:
		return agentoscore.ControlCancel, nil
	default:
		return "", fmt.Errorf("%w: %s", errAgentOSExecutorUnknownControl, op)
	}
}

var _ usecase.AgentExecutor = (*agentOSExecutor)(nil)
