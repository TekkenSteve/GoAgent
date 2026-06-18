package app

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
)

type agentOSExecutor struct {
	runtime agentos.Runtime
}

func newAgentOSExecutor(runtime agentos.Runtime) *agentOSExecutor {
	return &agentOSExecutor{runtime: runtime}
}

func (e *agentOSExecutor) Execute(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
	status, err := e.runtime.Start(ctx, agentos.RunSpec{
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
	})
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("agentos executor - execute: %w", err)
	}

	return runStatusToEntity(status), nil
}

func (e *agentOSExecutor) GetStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	status, err := e.runtime.Status(ctx, runID)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("agentos executor - status: %w", err)
	}

	return runStatusToEntity(status), nil
}

func (e *agentOSExecutor) Control(ctx context.Context, runID string, op entity.ControlOperation) error {
	agentOSOp, err := controlOperationToAgentOS(op)
	if err != nil {
		return err
	}

	if err := e.runtime.Control(ctx, runID, agentos.ControlRequest{Operation: agentOSOp}); err != nil {
		return fmt.Errorf("agentos executor - control: %w", err)
	}

	return nil
}

func runStatusToEntity(status agentos.RunStatus) entity.RunStatus {
	return entity.RunStatus{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           status.Step,
		Reason:         status.Reason,
		UpdatedAt:      status.UpdatedAt,
	}
}

func controlOperationToAgentOS(op entity.ControlOperation) (agentos.ControlOperation, error) {
	switch op {
	case entity.ControlPause:
		return agentos.ControlPause, nil
	case entity.ControlResume:
		return agentos.ControlResume, nil
	case entity.ControlCancel:
		return agentos.ControlCancel, nil
	default:
		return "", fmt.Errorf("agentos executor - unknown control operation: %s", op)
	}
}

var _ usecase.AgentExecutor = (*agentOSExecutor)(nil)
