package repo

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type (
	// WarmStateRepo persists operational state outside workflow history.
	WarmStateRepo interface {
		PersistMessage(ctx context.Context, record entity.MessageRecord) (string, error)
		PersistToolResult(ctx context.Context, record entity.ToolResultRecord) (string, error)
		GetMessage(ctx context.Context, ref string) (entity.MessageRecord, bool, error)
		GetToolResult(ctx context.Context, ref string) (entity.ToolResultRecord, bool, error)
	}
	// ColdStateRepo persists archival state outside workflow history.
	ColdStateRepo interface {
		PersistArchive(ctx context.Context, record entity.ArchiveRecord) (string, error)
		GetArchive(ctx context.Context, ref string) (entity.ArchiveRecord, bool, error)
	}
	// WorkflowStateRepo binds warm and cold adapters as boundary contracts for orchestration.
	WorkflowStateRepo interface {
		WarmStateRepo
		ColdStateRepo
	}
	// LLMProvider performs LLM inference.
	LLMProvider interface {
		Chat(ctx context.Context, req entity.LLMRequest) (entity.LLMResponse, error)
	}
	// ToolExecutor executes a single tool call.
	ToolExecutor interface {
		Execute(ctx context.Context, req entity.ToolRequest) (entity.ToolResult, error)
	}
	ExecutorRepo interface {
		StartExecution(ctx context.Context, req entity.ExecuteRequest) (entity.RunStatus, error)
		GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
		Pause(ctx context.Context, runID string) error
		Resume(ctx context.Context, runID string) error
		Cancel(ctx context.Context, runID string) error
	}
)
