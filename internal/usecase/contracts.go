package usecase

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type (
	// AgentExecutor is the business interface for agent workflow execution.
	AgentExecutor interface {
		Execute(ctx context.Context, req entity.ExecuteRequest) (entity.RunStatus, error)
		GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
		Control(ctx context.Context, runID string, op entity.ControlOperation) error
	}
)
