package temporal

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func temporalConfig(cfg RuntimeConfig) agentfwconfig.Temporal {
	base := agentfwconfig.Default().Temporal
	if cfg.TemporalAddress != "" {
		base.Address = cfg.TemporalAddress
	}
	if cfg.TemporalNamespace != "" {
		base.Namespace = cfg.TemporalNamespace
	}
	if cfg.TemporalTaskQueue != "" {
		base.TaskQueue = cfg.TemporalTaskQueue
	}

	return base
}

func executionRequestFromRunSpec(spec agentos.RunSpec) (*entity.ExecuteRequest, error) {
	if spec.RunID == "" {
		return nil, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.UserMessage == "" {
		return nil, fmt.Errorf("%w: user message is required", agentos.ErrInvalidRunSpec)
	}

	requestedAt := spec.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}

	return &entity.ExecuteRequest{
		RunID:          spec.RunID,
		ThreadID:       spec.ThreadID,
		ProjectID:      spec.ProjectID,
		AccountID:      spec.AccountID,
		ModelRef:       spec.ModelRef,
		AgentID:        spec.AgentID,
		SystemPrompt:   spec.SystemPrompt,
		UserMessage:    spec.UserMessage,
		IdempotencyKey: spec.IdempotencyKey,
		RequestedAt:    requestedAt,
	}, nil
}

func runStatusFromEntity(status entity.RunStatus) agentos.RunStatus {
	return agentos.RunStatus{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           status.Step,
		Reason:         status.Reason,
		UpdatedAt:      status.UpdatedAt,
	}
}

func controlOperationToEntity(op agentos.ControlOperation) (entity.ControlOperation, error) {
	switch op {
	case agentos.ControlPause:
		return entity.ControlPause, nil
	case agentos.ControlResume:
		return entity.ControlResume, nil
	case agentos.ControlCancel:
		return entity.ControlCancel, nil
	default:
		return "", fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
	}
}
