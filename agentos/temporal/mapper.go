package temporal

import (
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func temporalConfig(cfg *RuntimeConfig) agentfwconfig.Temporal {
	base := agentfwconfig.Default().Temporal
	base.TaskQueues = agentfwconfig.TaskQueues{}

	if cfg.TemporalAddress != "" {
		base.Address = cfg.TemporalAddress
	}

	if cfg.TemporalNamespace != "" {
		base.Namespace = cfg.TemporalNamespace
	}

	if cfg.NexusEndpoint != "" {
		base.NexusEndpoint = cfg.NexusEndpoint
	}

	if len(cfg.NexusPeers) > 0 {
		base.NexusPeers = cfg.NexusPeers
	}

	base.TaskQueues = cfg.TemporalTaskQueues.agentFWTaskQueues()

	return base
}

// workerNexusEndpoint resolves the Nexus endpoint name for a worker runtime,
// falling back to the framework default when the deployment sets none.
func workerNexusEndpoint(cfg *WorkerConfig) string {
	if cfg != nil && cfg.NexusEndpoint != "" {
		return cfg.NexusEndpoint
	}

	return agentfwconfig.DefaultNexusEndpoint
}

func executionRequestFromRunSpec(spec *agentos.RunSpec) (*entity.ExecuteRequest, error) {
	if spec.RunID == "" {
		return nil, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	stepQueue, err := orchestration.DecodeStepQueueInput(spec.Input)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", agentoscore.ErrInvalidRunSpec, err)
	}

	// A step queue carries its own messages inside the steps, so the top-level
	// message is required only for the single-agent loop.
	if spec.UserMessage == "" && stepQueue == nil {
		return nil, fmt.Errorf("%w: user message is required", agentoscore.ErrInvalidRunSpec)
	}

	requestedAt := spec.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}

	request := &entity.ExecuteRequest{
		RunID:          spec.RunID,
		ThreadID:       spec.ThreadID,
		ProjectID:      spec.ProjectID,
		AccountID:      spec.AccountID,
		ModelRef:       spec.ModelRef,
		AgentID:        spec.AgentID,
		SystemPrompt:   spec.SystemPrompt,
		UserMessage:    spec.UserMessage,
		AwaitUserInput: true,
		IdempotencyKey: spec.IdempotencyKey,
		RequestedAt:    requestedAt,
	}

	if stepQueue != nil {
		request.Steps = stepQueue.Steps
		request.TeamSpec = stepQueue.TeamSpec
		request.MaxDepth = stepQueue.MaxDepth
		request.ContinuePolicy = stepQueue.ContinuePolicy
	}

	return request, nil
}

func runStatusFromEntity(status *entity.RunStatus) agentos.RunStatus {
	mapped := agentos.RunStatus{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Reason:         status.Reason,
		UpdatedAt:      status.UpdatedAt,
	}
	if status.Step > 0 {
		mapped.Progress = &agentoscore.RunProgress{
			Current: status.Step,
		}
	}

	return mapped
}

func controlOperationToEntity(op agentoscore.ControlOperation) (entity.ControlOperation, error) {
	switch op {
	case agentoscore.ControlPause:
		return entity.ControlPause, nil
	case agentoscore.ControlResume:
		return entity.ControlResume, nil
	case agentoscore.ControlCancel:
		return entity.ControlCancel, nil
	default:
		return "", fmt.Errorf("%w: %s", agentoscore.ErrInvalidControlOperation, op)
	}
}
