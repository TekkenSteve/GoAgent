package persistent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/client"
)

var errExecutorTemporalConfigRequired = errors.New("ExecutorTemporal - config is required")

type ExecutorTemporal struct {
	client client.Client
	opts   options
}

type options struct {
	workflowName       string
	workflowIDPrefix   string
	workflowTaskQueues *orchestration.WorkflowTaskQueues
}

func NewExecutorTemporal(c client.Client, cfg *config.Temporal) (*ExecutorTemporal, error) {
	if cfg == nil {
		return nil, errExecutorTemporalConfigRequired
	}

	if err := cfg.TaskQueues.Validate(); err != nil {
		return nil, err
	}

	taskQueues := orchestration.WorkflowTaskQueues{
		NativeControl: cfg.TaskQueues.NativeControl,
		NativeLLM:     cfg.TaskQueues.NativeLLM,
		NativeTool:    cfg.TaskQueues.NativeTool,
		Stream:        cfg.TaskQueues.Stream,
		Trigger:       cfg.TaskQueues.Trigger,
	}

	return &ExecutorTemporal{
		client: c,
		opts: options{
			workflowName:       orchestration.AgentWorkflowName,
			workflowIDPrefix:   "agentfw-run-",
			workflowTaskQueues: &taskQueues,
		},
	}, nil
}

func (r *ExecutorTemporal) StartExecution(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
	workflowID := r.opts.workflowIDPrefix + req.RunID

	input := orchestration.AgentWorkflowInput{
		RunID:            req.RunID,
		AccountID:        req.AccountID,
		SystemPrompt:     req.SystemPrompt,
		Message:          req.UserMessage,
		Config:           entity.LLMConfig{Model: req.ModelRef},
		MCPServerConfigs: req.MCPServerConfigs,
		AwaitUserInput:   req.AwaitUserInput,
		TaskQueues:       *r.opts.workflowTaskQueues,
	}

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: r.opts.workflowTaskQueues.NativeControl,
	}

	_, err := r.client.ExecuteWorkflow(ctx, opts, r.opts.workflowName, &input)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - StartExecution - r.client.ExecuteWorkflow: %w", err)
	}

	return entity.RunStatus{
		RunID:          req.RunID,
		LifecycleState: string(entity.LifecycleCreated),
		Step:           0,
		UpdatedAt:      time.Now(),
	}, nil
}

func (r *ExecutorTemporal) GetStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	workflowID := r.opts.workflowIDPrefix + runID

	resp, err := r.client.QueryWorkflow(ctx, workflowID, "", orchestration.QueryRunStatus)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetStatus - r.client.QueryWorkflow: %w", err)
	}

	// Deserialize into orchestration.RunStatus first (no JSON tags → matches Temporal's
	// serialized field names exactly), then convert to the API-facing entity type.
	var orchStatus orchestration.RunStatus
	if err := resp.Get(&orchStatus); err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetStatus - resp.Get: %w", err)
	}

	return entity.RunStatus{
		RunID:          orchStatus.RunID,
		LifecycleState: orchStatus.LifecycleState,
		Step:           orchStatus.Step,
		Reason:         orchStatus.Reason,
		UpdatedAt:      orchStatus.UpdatedAt,
	}, nil
}

func (r *ExecutorTemporal) StartOrchestration(ctx context.Context, input *entity.OrchestrationInput) (entity.RunStatus, error) {
	workflowID := "orch-" + r.opts.workflowIDPrefix + input.RunID

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: r.opts.workflowTaskQueues.NativeControl,
	}

	_, err := r.client.ExecuteWorkflow(ctx, opts, orchestration.OrchestrationWorkflowName, &orchestration.WorkflowInput{
		Input:      *input,
		TaskQueues: *r.opts.workflowTaskQueues,
	})
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - StartOrchestration - r.client.ExecuteWorkflow: %w", err)
	}

	return entity.RunStatus{
		RunID:          input.RunID,
		LifecycleState: string(entity.LifecycleCreated),
		Step:           0,
		UpdatedAt:      time.Now(),
	}, nil
}

func (r *ExecutorTemporal) GetOrchestrationStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	workflowID := "orch-" + r.opts.workflowIDPrefix + runID

	resp, err := r.client.QueryWorkflow(ctx, workflowID, "", "query-run-status")
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetOrchestrationStatus - r.client.QueryWorkflow: %w", err)
	}

	var orchStatus orchestration.Status
	if err := resp.Get(&orchStatus); err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetOrchestrationStatus - resp.Get: %w", err)
	}

	return entity.RunStatus{
		RunID:          orchStatus.RunID,
		LifecycleState: orchStatus.State,
		Step:           orchStatus.Round,
		UpdatedAt:      time.Now(),
	}, nil
}

func (r *ExecutorTemporal) Pause(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentCommandSignal, "pause"); err != nil {
		return fmt.Errorf("ExecutorTemporal - Pause - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

func (r *ExecutorTemporal) Resume(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentCommandSignal, "resume"); err != nil {
		return fmt.Errorf("ExecutorTemporal - Resume - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

func (r *ExecutorTemporal) Cancel(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentCommandSignal, "cancel"); err != nil {
		return fmt.Errorf("ExecutorTemporal - Cancel - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

func (r *ExecutorTemporal) SignalUserMessage(ctx context.Context, runID string, message *orchestration.UserMessageSignal) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentMessageSignal, message); err != nil {
		return fmt.Errorf("ExecutorTemporal - SignalUserMessage - r.client.SignalWorkflow: %w", err)
	}

	return nil
}
