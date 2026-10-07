// Package usecase defines the application service interfaces and entrypoints.
package usecase

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

type (
	// --- Input ports (implemented by use cases, called by controllers) ---.

	// StreamEventWriter is the destination for streaming events.
	// Implementations write to Redis Stream, channel, etc.
	StreamEventWriter interface {
		WriteEvent(ctx context.Context, event entity.StreamEvent) error
	}
	// ToolDefProvider supplies LLM function calling definitions for available tools.
	// The agent usecase calls this to auto-populate tool definitions when none are
	// explicitly provided in the request.
	ToolDefProvider interface {
		Definitions() []entity.ToolDef
	}
	// TemplateManager manages workflow template CRUD and YAML import.
	TemplateManager interface {
		Create(ctx context.Context, req *entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		CreateFromYAML(ctx context.Context, accountID string, yamlData []byte) (entity.WorkflowTemplate, error)
		// Get, Update and Delete name the owning account: a template is its
		// account's, so the account is part of the lookup rather than a check
		// applied afterwards.
		Get(ctx context.Context, accountID, templateID string) (entity.WorkflowTemplate, error)
		Update(ctx context.Context, accountID, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error)
		Delete(ctx context.Context, accountID, templateID string) error
		ListByAccount(ctx context.Context, accountID string) ([]entity.WorkflowTemplate, error)
	}
)
