package repo

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type (
	// WarmStateRepo persists and queries operational state outside workflow history.
	WarmStateRepo interface {
		PersistMessage(ctx context.Context, record entity.MessageRecord) (string, error)
		PersistToolResult(ctx context.Context, record entity.ToolResultRecord) (string, error)
		GetMessage(ctx context.Context, ref string) (entity.MessageRecord, bool, error)
		GetToolResult(ctx context.Context, ref string) (entity.ToolResultRecord, bool, error)
		ListMessagesByRun(ctx context.Context, runID string, limit, offset uint64) ([]entity.MessageRecord, error)
		ListToolResultsByRun(ctx context.Context, runID string, limit, offset uint64) ([]entity.ToolResultRecord, error)
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
	// AgentRepo persists agent definitions and version snapshots.
	AgentRepo interface {
		Create(ctx context.Context, req entity.CreateAgentRequest) (entity.AgentRecord, error)
		Get(ctx context.Context, agentID string) (entity.AgentRecord, bool, error)
		Update(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error)
		Delete(ctx context.Context, agentID string) error
		ListByAccount(ctx context.Context, accountID string) ([]entity.AgentRecord, error)
		CreateVersion(ctx context.Context, record entity.AgentVersionRecord) error
		GetVersion(ctx context.Context, versionID string) (entity.AgentVersionRecord, bool, error)
		ListVersions(ctx context.Context, agentID string) ([]entity.AgentVersionRecord, error)
	}
	// LLMProvider performs LLM inference.
	LLMProvider interface {
		Chat(ctx context.Context, req entity.LLMRequest) (entity.LLMResponse, error)
	}
	// LLMStreamProvider optionally streams chat responses token by token.
	LLMStreamProvider interface {
		ChatStream(ctx context.Context, req entity.LLMRequest) (<-chan entity.LLMStreamChunk, error)
	}
	// ToolExecutor executes a single tool call.
	ToolExecutor interface {
		Execute(ctx context.Context, req entity.ToolRequest) (entity.ToolResult, error)
	}
	// WALAppender appends entries to a write-ahead log for async persistence.
	// The agent usecase writes to the WAL; a BatchWriter flushes WAL → Postgres.
	WALAppender interface {
		AppendMessage(ctx context.Context, runID string, record entity.MessageRecord) error
		AppendToolResult(ctx context.Context, runID string, record entity.ToolResultRecord) error
	}
	// ContextCompressor reduces message token count when approaching context limits.
	ContextCompressor interface {
		Compress(ctx context.Context, messages []entity.Message, config entity.LLMConfig) ([]entity.Message, bool, error)
	}
	ExecutorRepo interface {
		StartExecution(ctx context.Context, req entity.ExecuteRequest) (entity.RunStatus, error)
		GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
		Pause(ctx context.Context, runID string) error
		Resume(ctx context.Context, runID string) error
		Cancel(ctx context.Context, runID string) error
		StartOrchestration(ctx context.Context, input entity.OrchestrationInput) (entity.RunStatus, error)
	}
)
