// Package repo defines output port interfaces and provides infrastructure adapters
// that implement them. Output ports are called by use cases and implemented by
// repo sub-packages (persistent, webapi, etc.).
package repo

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type (
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
	// ContextCompressor reduces message token count when approaching context limits.
	ContextCompressor interface {
		Compress(ctx context.Context, messages []entity.Message, config entity.LLMConfig) ([]entity.Message, bool, error)
	}
	// WorkflowTemplateRepo persists workflow template definitions.
	WorkflowTemplateRepo interface {
		Create(ctx context.Context, req *entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		// The account is part of every by-id lookup: a template belongs to its
		// account, and a foreign id answers "not found" rather than being
		// fetched and checked afterwards.
		Get(ctx context.Context, accountID, templateID string) (entity.WorkflowTemplate, bool, error)
		Update(ctx context.Context, accountID, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		Delete(ctx context.Context, accountID, templateID string) error
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
