package usecase

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type (
	// AgentExecutor is the business interface for agent workflow execution.
	AgentExecutor interface {
		Execute(ctx context.Context, req entity.ExecuteRequest) (entity.RunStatus, error)
		GetStatus(ctx context.Context, runID string) (entity.RunStatus, error)
		Control(ctx context.Context, runID string, op entity.ControlOperation) error
	}
	// HistoryQuery is the business interface for querying agent run history.
	HistoryQuery interface {
		ListMessages(ctx context.Context, runID string, limit, offset uint64) ([]entity.MessageRecord, error)
		ListToolResults(ctx context.Context, runID string, limit, offset uint64) ([]entity.ToolResultRecord, error)
	}
	// OrchestrationExecutor starts and manages orchestration workflows.
	OrchestrationExecutor interface {
		ExecuteOrchestration(ctx context.Context, input entity.OrchestrationInput) (entity.RunStatus, error)
		GetOrchestrationStatus(ctx context.Context, runID string) (entity.RunStatus, error)
	}
	// StreamEventWriter is the destination for streaming events.
	// Implementations write to Redis Stream, channel, etc.
	StreamEventWriter interface {
		WriteEvent(ctx context.Context, event entity.StreamEvent) error
	}
	// StreamExecutor executes agent steps with streaming output.
	StreamExecutor interface {
		ExecuteStream(ctx context.Context, req entity.StreamRequest, writer StreamEventWriter) error
	}
	// ToolDefProvider supplies LLM function calling definitions for available tools.
	// The agent usecase calls this to auto-populate tool definitions when none are
	// explicitly provided in the request.
	ToolDefProvider interface {
		Definitions() []entity.ToolDef
	}
	// TriggerEventHandler processes incoming webhook events for event-type triggers.
	TriggerEventHandler interface {
		HandleEvent(ctx context.Context, eventSlug string, payload map[string]string) ([]entity.TriggerFireResult, error)
	}

	// TemplateManager manages workflow template CRUD and YAML import.
	TemplateManager interface {
		Create(ctx context.Context, req entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		CreateFromYAML(ctx context.Context, accountID string, yamlData []byte) (entity.WorkflowTemplate, error)
		Get(ctx context.Context, templateID string) (entity.WorkflowTemplate, error)
		Update(ctx context.Context, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		Delete(ctx context.Context, templateID string) error
		ListByAccount(ctx context.Context, accountID string) ([]entity.WorkflowTemplate, error)
	}
)
