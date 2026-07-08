package framework

import (
	"context"
	"fmt"

	agenttool "github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// ToolPipeline wraps agentfw/tool.Pipeline as a repo.ToolExecutor.
type ToolPipeline struct {
	pipe *agenttool.Pipeline
}

// NewToolPipeline creates an adapter from a configured tool pipeline.
func NewToolPipeline(pipe *agenttool.Pipeline) *ToolPipeline {
	return &ToolPipeline{pipe: pipe}
}

// Execute converts entity.ToolRequest to agentfw types, runs the pipeline, and converts back.
func (a *ToolPipeline) Execute(ctx context.Context, req *entity.ToolRequest) (entity.ToolResult, error) {
	toolReq := agenttool.Request{
		RunID:          req.RunID,
		ToolCallID:     req.ToolCallID,
		ToolName:       req.ToolName,
		AccountID:      req.AccountID,
		ProjectID:      req.ProjectID,
		Tier:           agenttool.Tier(req.Tier),
		IdempotencyKey: req.IdempotencyKey,
		ConflictDomain: req.ConflictDomain,
		SideEffecting:  req.SideEffecting,
		Args:           req.Args,
	}

	result, err := a.pipe.Execute(ctx, &toolReq)
	if err != nil {
		return entity.ToolResult{}, fmt.Errorf("tool pipeline - %w", err)
	}

	return entity.ToolResult{
		RunID:              result.RunID,
		ToolCallID:         result.ToolCallID,
		ToolName:           result.ToolName,
		Output:             result.Output,
		PersistedRef:       result.PersistedRef,
		FromIdempotent:     result.FromIdempotent,
		Attempts:           result.Attempts,
		ExecutionIsolation: entity.ExecutionIsolation(result.ExecutionIsolation),
	}, nil
}
