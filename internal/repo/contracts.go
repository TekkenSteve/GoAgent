// Package repo defines output port interfaces and provides infrastructure adapters
// that implement them. Output ports are called by use cases and implemented by
// repo sub-packages (persistent, webapi, pipeline, etc.).
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
		Create(ctx context.Context, req *entity.CreateAgentRequest) (entity.AgentRecord, error)
		Get(ctx context.Context, agentID string) (entity.AgentRecord, bool, error)
		Update(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error)
		Delete(ctx context.Context, agentID string) error
		ListByAccount(ctx context.Context, accountID string) ([]entity.AgentRecord, error)
		CreateVersion(ctx context.Context, record *entity.AgentVersionRecord) error
		GetVersion(ctx context.Context, versionID string) (entity.AgentVersionRecord, bool, error)
		ListVersions(ctx context.Context, agentID string) ([]entity.AgentVersionRecord, error)
	}
	// LLMProvider performs LLM inference.
	LLMProvider interface {
		Chat(ctx context.Context, req *entity.LLMRequest) (entity.LLMResponse, error)
	}
	// LLMStreamProvider optionally streams chat responses token by token.
	LLMStreamProvider interface {
		ChatStream(ctx context.Context, req *entity.LLMRequest) (<-chan entity.LLMStreamChunk, error)
	}
	// ToolExecutor executes a single tool call.
	ToolExecutor interface {
		Execute(ctx context.Context, req *entity.ToolRequest) (entity.ToolResult, error)
	}
	// WALAppender appends entries to a write-ahead log for async persistence.
	// The agent usecase writes to the WAL; a BatchWriter flushes WAL -> Postgres.
	WALAppender interface {
		AppendMessage(ctx context.Context, runID string, record entity.MessageRecord) error
		AppendToolResult(ctx context.Context, runID string, record entity.ToolResultRecord) error
	}
	// ContextCompressor reduces message token count when approaching context limits.
	ContextCompressor interface {
		Compress(ctx context.Context, messages []entity.Message, config entity.LLMConfig) ([]entity.Message, bool, error)
	}
	// ExecutorRepo starts, queries and controls agent and orchestration executions.
	ExecutorRepo interface {
		StartExecution(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error)
		GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
		Pause(ctx context.Context, runID string) error
		Resume(ctx context.Context, runID string) error
		Cancel(ctx context.Context, runID string) error
		StartOrchestration(ctx context.Context, input *entity.OrchestrationInput) (entity.RunStatus, error)
		GetOrchestrationStatus(ctx context.Context, runID string) (entity.RunStatus, error)
	}
	// WorkflowTemplateRepo persists workflow template definitions.
	WorkflowTemplateRepo interface {
		Create(ctx context.Context, req *entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		Get(ctx context.Context, templateID string) (entity.WorkflowTemplate, bool, error)
		Update(ctx context.Context, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		Delete(ctx context.Context, templateID string) error
		ListByAccount(ctx context.Context, accountID string) ([]entity.WorkflowTemplate, error)
	}
	// CreditManager manages account credit balances and transactions.
	CreditManager interface {
		GetBalance(ctx context.Context, accountID string) (entity.CreditAccount, error)
		Deduct(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error)
		AddCredits(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error)
		GetHistory(ctx context.Context, accountID string, limit, offset int) ([]entity.CreditTransaction, error)
	}
	// CostCalculator calculates the monetary cost of LLM usage from model and token counts.
	CostCalculator interface {
		Calculate(ctx context.Context, modelID string, usage entity.Usage) (entity.Money, error)
	}
	// UsageRecordRepo persists usage records for audit and billing history.
	UsageRecordRepo interface {
		CreateUsageRecord(ctx context.Context, record *entity.UsageRecord) error
	}
)
