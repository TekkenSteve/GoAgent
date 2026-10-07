package persistent

import (
	"context"
	"fmt"
	"time"

	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"go.temporal.io/sdk/client"
)

// TemporalStreamExecutor implements usecase.StreamExecutor by running agent
// execution inside a Temporal workflow. Events are written to the EventStore
// by workflow activities, and the controller reads them via its existing
// EventStore subscription.
type TemporalStreamExecutor struct {
	client     client.Client
	taskQueue  string
	taskQueues *orchestration.WorkflowTaskQueues
	delegation orchestration.DelegateBudget
	runTimeout time.Duration
}

// NewTemporalStreamExecutor creates a streaming executor backed by Temporal.
func NewTemporalStreamExecutor(c client.Client, cfg *agentfwconfig.Temporal, taskQueue string, taskQueues *orchestration.WorkflowTaskQueues) *TemporalStreamExecutor {
	executor := &TemporalStreamExecutor{
		client:     c,
		taskQueue:  taskQueue,
		taskQueues: taskQueues,
	}

	if cfg != nil {
		executor.delegation = orchestration.DelegateBudget{
			MaxDepth:    cfg.MaxDelegateDepth,
			TokenBudget: cfg.DelegateTokenBudget,
		}
		executor.runTimeout = cfg.RunWorkflowTimeout
	}

	return executor
}

// ExecuteStream starts a Temporal streaming workflow and returns immediately.
// The workflow activities write events to the EventStore, which the controller
// consumes via its EventStore subscription.
func (e *TemporalStreamExecutor) ExecuteStream(ctx context.Context, req *entity.StreamRequest, _ usecase.StreamEventWriter) error {
	workflowID := "agentfw-stream-" + req.RunID

	input := orchestration.InitStreamInput{
		SessionID:        req.RunID,
		AccountID:        req.AccountID,
		ProjectID:        req.ProjectID,
		RunID:            req.RunID,
		SystemPrompt:     req.SystemPrompt,
		Message:          req.Message,
		History:          req.History,
		Tools:            req.Tools,
		Config:           req.Config,
		MCPServerConfigs: req.MCPServerConfigs,
		TaskQueues:       *e.taskQueues,
		Delegate:         e.delegation,
	}

	_, err := e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                e.taskQueue,
		WorkflowExecutionTimeout: e.runTimeout,
		TypedSearchAttributes:    orchestration.SearchAttributesForRun(req.RunID, "running"),
	}, orchestration.StreamWorkflowName, &input)
	if err != nil {
		return fmt.Errorf("TemporalStreamExecutor - ExecuteStream - start workflow: %w", err)
	}

	return nil
}
