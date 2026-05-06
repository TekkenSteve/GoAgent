package persistent

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/client"
)

type ExecutorTemporal struct {
	client client.Client
	opts   options
}

type options struct {
	taskQueue        string
	workflowName     string
	workflowIDPrefix string
}

func NewExecutorTemporal(c client.Client, cfg config.Temporal) *ExecutorTemporal {
	return &ExecutorTemporal{
		client: c,
		opts: options{
			taskQueue:        cfg.TaskQueue,
			workflowName:     orchestration.AgentWorkflowName,
			workflowIDPrefix: "agentfw-run-",
		},
	}
}

func (r *ExecutorTemporal) StartExecution(ctx context.Context, req entity.ExecuteRequest) (entity.RunStatus, error) {
	workflowID := r.opts.workflowIDPrefix + req.RunID

	input := orchestration.WorkflowInput{
		Request: orchestration.ExecuteRequest{
			RunID:           req.RunID,
			ThreadID:        req.ThreadID,
			ProjectID:       req.ProjectID,
			AccountID:       req.AccountID,
			ModelRef:        req.ModelRef,
			AgentID:         req.AgentID,
			AgentVersionID:  req.AgentVersionID,
			AgentConfigVer:  req.AgentConfigVer,
			ToolSchemaVer:   req.ToolSchemaVer,
			UserMessage:     req.UserMessage,
			IsNewThread:     req.IsNewThread,
			BypassAdmission: req.BypassAdmission,
			IdempotencyKey:  req.IdempotencyKey,
			EventSchemaVer:  req.EventSchemaVer,
			WorkflowVersion: req.WorkflowVersion,
			RequestedAt:     req.RequestedAt,
		},
	}

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: r.opts.taskQueue,
	}

	_, err := r.client.ExecuteWorkflow(ctx, opts, r.opts.workflowName, input)
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

	var status entity.RunStatus
	if err := resp.Get(&status); err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetStatus - resp.Get: %w", err)
	}

	return status, nil
}

func (r *ExecutorTemporal) Pause(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.SignalPause, nil); err != nil {
		return fmt.Errorf("ExecutorTemporal - Pause - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

func (r *ExecutorTemporal) Resume(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.SignalResume, nil); err != nil {
		return fmt.Errorf("ExecutorTemporal - Resume - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

func (r *ExecutorTemporal) Cancel(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.SignalCancel, nil); err != nil {
		return fmt.Errorf("ExecutorTemporal - Cancel - r.client.SignalWorkflow: %w", err)
	}

	return nil
}
