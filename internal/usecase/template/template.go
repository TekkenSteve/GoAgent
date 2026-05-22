package template

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/usecase/loader"
)

var (
	ErrTemplateNameRequired    = errors.New("template name is required")
	ErrTemplateAccountRequired = errors.New("template account_id is required")
	ErrTemplateNotFound        = errors.New("template not found")
)

// UseCase handles workflow template management and orchestration input preparation.
type UseCase struct {
	templateRepo repo.WorkflowTemplateRepo
}

// New creates a template usecase.
func New(templateRepo repo.WorkflowTemplateRepo) *UseCase {
	return &UseCase{templateRepo: templateRepo}
}

// DefaultTemplate returns the hardcoded default single-agent workflow definition.
func DefaultTemplate() *entity.CreateWorkflowTemplateRequest {
	return &entity.CreateWorkflowTemplateRequest{
		AccountID:    "bootstrap",
		Name:         "default-agent",
		Description:  "Default single-agent workflow created by the system",
		SystemPrompt: "You are a helpful assistant.",
		DefaultModel: "gpt-4.1-mini",
		TeamSpec: entity.TeamSpec{
			Name: "default-agent",
			Agents: []entity.AgentSpec{
				{
					ID:           "default-agent",
					Name:         "Default Agent",
					ModelRef:     "gpt-4.1-mini",
					SystemPrompt: "You are a helpful assistant.",
					Tools:        []entity.ToolBinding{},
				},
			},
			Steps: []entity.StepTemplate{
				{
					ID:       "step-1",
					Type:     entity.StepAgent,
					AgentRef: "default-agent",
					Input:    map[string]any{"message": "{{.Message}}"},
				},
			},
		},
		IsEnabled: true,
	}
}

// EnsureDefault creates the default workflow template if none exists for the
// bootstrap account. A code-defined default
// that seeds the database on first run.
func (uc *UseCase) EnsureDefault(ctx context.Context) error {
	templates, err := uc.templateRepo.ListByAccount(ctx, "bootstrap")
	if err != nil {
		return fmt.Errorf("TemplateUseCase - EnsureDefault - list: %w", err)
	}

	if len(templates) > 0 {
		return nil
	}

	tpl := DefaultTemplate()

	_, err = uc.Create(ctx, tpl)
	if err != nil {
		return fmt.Errorf("TemplateUseCase - EnsureDefault - create: %w", err)
	}

	return nil
}

// Create inserts a new workflow template.
func (uc *UseCase) Create(ctx context.Context, req *entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error) {
	if req.Name == "" {
		return entity.WorkflowTemplate{}, fmt.Errorf("TemplateUseCase - Create - %w", ErrTemplateNameRequired)
	}

	if req.AccountID == "" {
		return entity.WorkflowTemplate{}, fmt.Errorf("TemplateUseCase - Create - %w", ErrTemplateAccountRequired)
	}

	return uc.templateRepo.Create(ctx, req)
}

// CreateFromYAML parses a YAML workflow definition and persists it as a template.
// The accountID identifies the owning account; the YAML name is used as template name.
func (uc *UseCase) CreateFromYAML(ctx context.Context, accountID string, yamlData []byte) (entity.WorkflowTemplate, error) {
	teamSpec, err := loader.ParseYAML(yamlData)
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("TemplateUseCase - CreateFromYAML - parse: %w", err)
	}

	return uc.Create(ctx, &entity.CreateWorkflowTemplateRequest{
		AccountID: accountID,
		Name:      teamSpec.Name,
		TeamSpec:  *teamSpec,
	})
}

// Get retrieves a workflow template by ID.
func (uc *UseCase) Get(ctx context.Context, templateID string) (entity.WorkflowTemplate, error) {
	record, exists, err := uc.templateRepo.Get(ctx, templateID)
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("TemplateUseCase - Get - repo: %w", err)
	}

	if !exists {
		return entity.WorkflowTemplate{}, fmt.Errorf("TemplateUseCase - Get - %w: %s", ErrTemplateNotFound, templateID)
	}

	return record, nil
}

// Update updates an existing workflow template.
func (uc *UseCase) Update(ctx context.Context, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error) {
	return uc.templateRepo.Update(ctx, templateID, req)
}

// Delete removes a workflow template by ID.
func (uc *UseCase) Delete(ctx context.Context, templateID string) error {
	return uc.templateRepo.Delete(ctx, templateID)
}

// ListByAccount retrieves all templates for an account.
func (uc *UseCase) ListByAccount(ctx context.Context, accountID string) ([]entity.WorkflowTemplate, error) {
	return uc.templateRepo.ListByAccount(ctx, accountID)
}

// ToOrchestrationInput converts a workflow template into an OrchestrationInput.
// It expands the TeamSpec into []Step via the caller (e.g., team.Expand)
// and wraps it in an OrchestrationInput ready for execution.
func (uc *UseCase) ToOrchestrationInput(tpl *entity.WorkflowTemplate, message string) entity.OrchestrationInput {
	return entity.OrchestrationInput{
		RunID:        tpl.ID,
		TeamSpec:     &tpl.TeamSpec,
		SystemPrompt: tpl.SystemPrompt,
		Message:      message,
	}
}
