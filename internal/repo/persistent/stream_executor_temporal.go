package persistent

import (
	"context"
	"fmt"

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
}

// NewTemporalStreamExecutor creates a streaming executor backed by Temporal.
func NewTemporalStreamExecutor(c client.Client, taskQueue string, taskQueues *orchestration.WorkflowTaskQueues) *TemporalStreamExecutor {
	return &TemporalStreamExecutor{
		client:     c,
		taskQueue:  taskQueue,
		taskQueues: taskQueues,
	}
}

// ExecuteStream starts a Temporal streaming workflow and returns immediately.
// The workflow activities write events to the EventStore, which the controller
// consumes via its EventStore subscription.
func (e *TemporalStreamExecutor) ExecuteStream(ctx context.Context, req *entity.StreamRequest, _ usecase.StreamEventWriter) error {
	workflowID := "agentfw-stream-" + req.RunID

	input := orchestration.InitStreamInput{
		SessionID:        req.RunID,
		AccountID:        req.AccountID,
		RunID:            req.RunID,
		SystemPrompt:     req.SystemPrompt,
		Message:          req.Message,
		History:          req.History,
		Tools:            req.Tools,
		Config:           req.Config,
		MCPServerConfigs: req.MCPServerConfigs,
		TaskQueues:       *e.taskQueues,
	}

	_, err := e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:               workflowID,
		TaskQueue:        e.taskQueue,
		SearchAttributes: orchestration.SearchAttributesForRun(req.RunID, "running"),
	}, orchestration.StreamWorkflowName, &input)
	if err != nil {
		return fmt.Errorf("TemporalStreamExecutor - ExecuteStream - start workflow: %w", err)
	}

	return nil
}
