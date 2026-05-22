package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// Sentinel errors.
var ErrTemplateNotFound = errors.New("template not found")

// WorkflowTemplateRepo implements repo.WorkflowTemplateRepo with Postgres.
type WorkflowTemplateRepo struct {
	*postgres.Postgres
}

// NewWorkflowTemplateRepo creates a Postgres-backed workflow template repository.
func NewWorkflowTemplateRepo(pg *postgres.Postgres) *WorkflowTemplateRepo {
	return &WorkflowTemplateRepo{pg}
}

// Create inserts a new workflow template.
func (r *WorkflowTemplateRepo) Create(ctx context.Context, req *entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error) {
	teamSpecJSON, err := json.Marshal(req.TeamSpec)
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Create - marshal team_spec: %w", err)
	}

	tags := ensureSlice(req.Tags)

	sql, args, err := r.Builder.
		Insert("workflow_templates").
		Columns("account_id", "name", "description", "team_spec", "system_prompt", "default_model", "tags", "is_enabled").
		Values(req.AccountID, req.Name, req.Description, string(teamSpecJSON), req.SystemPrompt, req.DefaultModel, tags, req.IsEnabled).
		Suffix("RETURNING id, account_id, name, description, team_spec, system_prompt, default_model, tags, is_enabled, created_at, updated_at").
		ToSql()
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Create - builder: %w", err)
	}

	var (
		record      entity.WorkflowTemplate
		teamSpecStr string
		tagsStr     []string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.ID, &record.AccountID, &record.Name, &record.Description,
		&teamSpecStr, &record.SystemPrompt, &record.DefaultModel, &tagsStr,
		&record.IsEnabled, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Create - query: %w", err)
	}

	if err := json.Unmarshal([]byte(teamSpecStr), &record.TeamSpec); err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Create - unmarshal team_spec: %w", err)
	}

	record.Tags = tagsStr

	return record, nil
}

// Get retrieves a workflow template by ID.
func (r *WorkflowTemplateRepo) Get(ctx context.Context, templateID string) (entity.WorkflowTemplate, bool, error) {
	sql, args, err := r.Builder.
		Select("id", "account_id", "name", "description", "team_spec", "system_prompt", "default_model", "tags", "is_enabled", "created_at", "updated_at").
		From("workflow_templates").
		Where(sq.Eq{"id": templateID}).
		ToSql()
	if err != nil {
		return entity.WorkflowTemplate{}, false, fmt.Errorf("WorkflowTemplateRepo - Get - builder: %w", err)
	}

	var (
		record      entity.WorkflowTemplate
		teamSpecStr string
		tagsStr     []string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.ID, &record.AccountID, &record.Name, &record.Description,
		&teamSpecStr, &record.SystemPrompt, &record.DefaultModel, &tagsStr,
		&record.IsEnabled, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.WorkflowTemplate{}, false, nil
		}

		return entity.WorkflowTemplate{}, false, fmt.Errorf("WorkflowTemplateRepo - Get - query: %w", err)
	}

	if err := json.Unmarshal([]byte(teamSpecStr), &record.TeamSpec); err != nil {
		return entity.WorkflowTemplate{}, false, fmt.Errorf("WorkflowTemplateRepo - Get - unmarshal team_spec: %w", err)
	}

	record.Tags = tagsStr

	return record, true, nil
}

// Update updates an existing workflow template.
func (r *WorkflowTemplateRepo) Update(ctx context.Context, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error) {
	builder, err := buildTemplateUpdateQuery(r.Builder, templateID, req)
	if err != nil {
		return entity.WorkflowTemplate{}, err
	}

	builder = builder.Set("updated_at", sq.Expr("NOW()"))
	builder = builder.Suffix("RETURNING id, account_id, name, description, team_spec, system_prompt, default_model, tags, is_enabled, created_at, updated_at")

	sql, args, err := builder.ToSql()
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - builder: %w", err)
	}

	var (
		record      entity.WorkflowTemplate
		teamSpecStr string
		tagsStr     []string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.ID, &record.AccountID, &record.Name, &record.Description,
		&teamSpecStr, &record.SystemPrompt, &record.DefaultModel, &tagsStr,
		&record.IsEnabled, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - %w: %s", ErrTemplateNotFound, templateID)
		}

		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - query: %w", err)
	}

	if err := json.Unmarshal([]byte(teamSpecStr), &record.TeamSpec); err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - unmarshal team_spec: %w", err)
	}

	record.Tags = tagsStr

	return record, nil
}

// buildTemplateUpdateQuery builds the update builder with non-nil fields from req.
func buildTemplateUpdateQuery(b sq.StatementBuilderType, templateID string, req entity.UpdateWorkflowTemplateRequest) (sq.UpdateBuilder, error) {
	builder := b.Update("workflow_templates").Where(sq.Eq{"id": templateID})

	if req.Name != nil {
		builder = builder.Set("name", *req.Name)
	}

	if req.Description != nil {
		builder = builder.Set("description", *req.Description)
	}

	if req.TeamSpec != nil {
		teamSpecJSON, err := json.Marshal(req.TeamSpec)
		if err != nil {
			return builder, fmt.Errorf("WorkflowTemplateRepo - Update - marshal team_spec: %w", err)
		}

		builder = builder.Set("team_spec", string(teamSpecJSON))
	}

	if req.SystemPrompt != nil {
		builder = builder.Set("system_prompt", *req.SystemPrompt)
	}

	if req.DefaultModel != nil {
		builder = builder.Set("default_model", *req.DefaultModel)
	}

	if req.Tags != nil {
		builder = builder.Set("tags", *req.Tags)
	}

	if req.IsEnabled != nil {
		builder = builder.Set("is_enabled", *req.IsEnabled)
	}

	return builder, nil
}

// Delete removes a workflow template by ID.
func (r *WorkflowTemplateRepo) Delete(ctx context.Context, templateID string) error {
	sql, args, err := r.Builder.Delete("workflow_templates").Where(sq.Eq{"id": templateID}).ToSql()
	if err != nil {
		return fmt.Errorf("WorkflowTemplateRepo - Delete - builder: %w", err)
	}

	_, err = r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("WorkflowTemplateRepo - Delete - exec: %w", err)
	}

	return nil
}

// ListByAccount retrieves all templates for a given account.
func (r *WorkflowTemplateRepo) ListByAccount(ctx context.Context, accountID string) ([]entity.WorkflowTemplate, error) {
	sql, args, err := r.Builder.
		Select("id", "account_id", "name", "description", "team_spec", "system_prompt", "default_model", "tags", "is_enabled", "created_at", "updated_at").
		From("workflow_templates").
		Where(sq.Eq{"account_id": accountID}).
		OrderBy("created_at DESC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("WorkflowTemplateRepo - ListByAccount - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("WorkflowTemplateRepo - ListByAccount - query: %w", err)
	}
	defer rows.Close()

	var records []entity.WorkflowTemplate

	for rows.Next() {
		var (
			record      entity.WorkflowTemplate
			teamSpecStr string
			tagsStr     []string
		)

		if err := rows.Scan(
			&record.ID, &record.AccountID, &record.Name, &record.Description,
			&teamSpecStr, &record.SystemPrompt, &record.DefaultModel, &tagsStr,
			&record.IsEnabled, &record.CreatedAt, &record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("WorkflowTemplateRepo - ListByAccount - scan: %w", err)
		}

		if err := json.Unmarshal([]byte(teamSpecStr), &record.TeamSpec); err != nil {
			return nil, fmt.Errorf("WorkflowTemplateRepo - ListByAccount - unmarshal team_spec: %w", err)
		}

		record.Tags = tagsStr
		records = append(records, record)
	}

	return records, nil
}

// ensureSlice returns a non-nil slice from a nil slice (Postgres text[] compatibility).
func ensureSlice(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}
