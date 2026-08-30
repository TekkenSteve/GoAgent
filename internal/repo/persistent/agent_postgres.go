// Package persistent implements repository storage on PostgreSQL.
package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// AgentRepo implements repo.AgentRepo with Postgres.
type AgentRepo struct {
	*postgres.Postgres
}

// NewAgentRepo creates a Postgres-backed agent repository.
func NewAgentRepo(pg *postgres.Postgres) *AgentRepo {
	return &AgentRepo{pg}
}

// Create inserts a new agent record.
func (r *AgentRepo) Create(ctx context.Context, req *entity.CreateAgentRequest) (entity.AgentRecord, error) {
	configJSON, err := json.Marshal(req.Config)
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Create - marshal config: %w", err)
	}

	sql, args, err := r.Builder.
		Insert("agents").
		Columns(_colAccountID, "name", "description", "system_prompt", "model_ref", "config", "is_default").
		Values(req.AccountID, req.Name, req.Description, req.SystemPrompt, req.ModelRef, string(configJSON), false).
		Suffix("RETURNING agent_id, account_id, name, description, system_prompt, model_ref, config, current_version, is_default, created_at, updated_at").
		ToSql()
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Create - builder: %w", err)
	}

	var (
		record    entity.AgentRecord
		configStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.AgentID, &record.AccountID, &record.Name, &record.Description,
		&record.SystemPrompt, &record.ModelRef, &configStr, &record.CurrentVersion,
		&record.IsDefault, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Create - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Create - unmarshal config: %w", err)
	}

	return record, nil
}

// Get retrieves an agent record by ID.
func (r *AgentRepo) Get(ctx context.Context, agentID string) (entity.AgentRecord, bool, error) {
	sql, args, err := r.Builder.
		Select("agent_id", _colAccountID, "name", "description", "system_prompt", "model_ref", "config", "current_version", "is_default", "created_at", "updated_at").
		From("agents").
		Where(sq.Eq{"agent_id": agentID}).
		ToSql()
	if err != nil {
		return entity.AgentRecord{}, false, fmt.Errorf("AgentRepo - Get - builder: %w", err)
	}

	var (
		record    entity.AgentRecord
		configStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.AgentID, &record.AccountID, &record.Name, &record.Description,
		&record.SystemPrompt, &record.ModelRef, &configStr, &record.CurrentVersion,
		&record.IsDefault, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.AgentRecord{}, false, nil
		}

		return entity.AgentRecord{}, false, fmt.Errorf("AgentRepo - Get - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.AgentRecord{}, false, fmt.Errorf("AgentRepo - Get - unmarshal config: %w", err)
	}

	return record, true, nil
}

// Update modifies an existing agent record.
// This is a plain data update. Version management (auto-snapshotting config
// changes) is handled by the usecase layer.
func (r *AgentRepo) Update(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
	builder, err := buildUpdateQuery(r.Builder, agentID, req)
	if err != nil {
		return entity.AgentRecord{}, err
	}

	sql, args, err := builder.
		Suffix("RETURNING agent_id, account_id, name, description, system_prompt, model_ref, config, current_version, is_default, created_at, updated_at").
		ToSql()
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Update - builder: %w", err)
	}

	var (
		record    entity.AgentRecord
		configStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.AgentID, &record.AccountID, &record.Name, &record.Description,
		&record.SystemPrompt, &record.ModelRef, &configStr, &record.CurrentVersion,
		&record.IsDefault, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Update - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Update - unmarshal config: %w", err)
	}

	return record, nil
}

// buildUpdateQuery builds the update builder with non-nil fields from req.
func buildUpdateQuery(b sq.StatementBuilderType, agentID string, req entity.UpdateAgentRequest) (sq.UpdateBuilder, error) {
	builder := b.Update("agents").Where(sq.Eq{"agent_id": agentID})

	if req.Name != nil {
		builder = builder.Set("name", *req.Name)
	}

	if req.Description != nil {
		builder = builder.Set("description", *req.Description)
	}

	if req.SystemPrompt != nil {
		builder = builder.Set("system_prompt", *req.SystemPrompt)
	}

	if req.ModelRef != nil {
		builder = builder.Set("model_ref", *req.ModelRef)
	}

	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return builder, fmt.Errorf("AgentRepo - Update - marshal config: %w", err)
		}

		builder = builder.Set("config", string(configJSON))
	}

	if req.IsDefault != nil {
		builder = builder.Set("is_default", *req.IsDefault)
	}

	if req.CurrentVersion != nil {
		builder = builder.Set("current_version", *req.CurrentVersion)
	}

	return builder, nil
}

// Delete removes an agent record by ID.
func (r *AgentRepo) Delete(ctx context.Context, agentID string) error {
	sql, args, err := r.Builder.
		Delete("agents").
		Where(sq.Eq{"agent_id": agentID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentRepo - Delete - builder: %w", err)
	}

	_, err = r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("AgentRepo - Delete - exec: %w", err)
	}

	return nil
}

// ListByAccount retrieves all agent records for an account.
func (r *AgentRepo) ListByAccount(ctx context.Context, accountID string) ([]entity.AgentRecord, error) {
	sql, args, err := r.Builder.
		Select("agent_id", _colAccountID, "name", "description", "system_prompt", "model_ref", "config", "current_version", "is_default", "created_at", "updated_at").
		From("agents").
		Where(sq.Eq{_colAccountID: accountID}).
		OrderBy("created_at DESC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentRepo - ListByAccount - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentRepo - ListByAccount - query: %w", err)
	}
	defer rows.Close()

	var records []entity.AgentRecord

	for rows.Next() {
		var (
			record    entity.AgentRecord
			configStr string
		)

		if err := rows.Scan(
			&record.AgentID, &record.AccountID, &record.Name, &record.Description,
			&record.SystemPrompt, &record.ModelRef, &configStr, &record.CurrentVersion,
			&record.IsDefault, &record.CreatedAt, &record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("AgentRepo - ListByAccount - scan: %w", err)
		}

		if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
			return nil, fmt.Errorf("AgentRepo - ListByAccount - unmarshal config: %w", err)
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentRepo - ListByAccount - rows: %w", err)
	}

	return records, nil
}

// CreateVersion stores a new agent version snapshot.
func (r *AgentRepo) CreateVersion(ctx context.Context, record *entity.AgentVersionRecord) error {
	configJSON, err := json.Marshal(record.Config)
	if err != nil {
		return fmt.Errorf("AgentRepo - CreateVersion - marshal config: %w", err)
	}

	toolBindingsJSON, err := json.Marshal(record.ToolBindings)
	if err != nil {
		return fmt.Errorf("AgentRepo - CreateVersion - marshal tool_bindings: %w", err)
	}

	sql, args, err := r.Builder.
		Insert("agent_versions").
		Columns("version_id", "agent_id", "version_name", "system_prompt", "model_ref", "config", "tool_bindings", "change_description").
		Values(record.VersionID, record.AgentID, record.VersionName, record.SystemPrompt, record.ModelRef, string(configJSON), string(toolBindingsJSON), record.ChangeDescription).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentRepo - CreateVersion - builder: %w", err)
	}

	_, err = r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("AgentRepo - CreateVersion - exec: %w", err)
	}

	return nil
}

// GetVersion retrieves a version snapshot by ID.
func (r *AgentRepo) GetVersion(ctx context.Context, versionID string) (entity.AgentVersionRecord, bool, error) {
	sql, args, err := r.Builder.
		Select("version_id", "agent_id", "version_name", "system_prompt", "model_ref", "config", "tool_bindings", "change_description", "created_at").
		From("agent_versions").
		Where(sq.Eq{"version_id": versionID}).
		ToSql()
	if err != nil {
		return entity.AgentVersionRecord{}, false, fmt.Errorf("AgentRepo - GetVersion - builder: %w", err)
	}

	var (
		record                     entity.AgentVersionRecord
		configStr, toolBindingsStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.VersionID, &record.AgentID, &record.VersionName, &record.SystemPrompt,
		&record.ModelRef, &configStr, &toolBindingsStr, &record.ChangeDescription, &record.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.AgentVersionRecord{}, false, nil
		}

		return entity.AgentVersionRecord{}, false, fmt.Errorf("AgentRepo - GetVersion - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.AgentVersionRecord{}, false, fmt.Errorf("AgentRepo - GetVersion - unmarshal config: %w", err)
	}

	if err := json.Unmarshal([]byte(toolBindingsStr), &record.ToolBindings); err != nil {
		return entity.AgentVersionRecord{}, false, fmt.Errorf("AgentRepo - GetVersion - unmarshal tool_bindings: %w", err)
	}

	return record, true, nil
}

// ListVersions retrieves all version snapshots for an agent.
func (r *AgentRepo) ListVersions(ctx context.Context, agentID string) ([]entity.AgentVersionRecord, error) {
	sql, args, err := r.Builder.
		Select("version_id", "agent_id", "version_name", "system_prompt", "model_ref", "config", "tool_bindings", "change_description", "created_at").
		From("agent_versions").
		Where(sq.Eq{"agent_id": agentID}).
		OrderBy("created_at DESC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentRepo - ListVersions - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentRepo - ListVersions - query: %w", err)
	}
	defer rows.Close()

	var records []entity.AgentVersionRecord

	for rows.Next() {
		var (
			record                     entity.AgentVersionRecord
			configStr, toolBindingsStr string
		)

		if err := rows.Scan(
			&record.VersionID, &record.AgentID, &record.VersionName, &record.SystemPrompt,
			&record.ModelRef, &configStr, &toolBindingsStr, &record.ChangeDescription, &record.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("AgentRepo - ListVersions - scan: %w", err)
		}

		if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
			return nil, fmt.Errorf("AgentRepo - ListVersions - unmarshal config: %w", err)
		}

		if err := json.Unmarshal([]byte(toolBindingsStr), &record.ToolBindings); err != nil {
			return nil, fmt.Errorf("AgentRepo - ListVersions - unmarshal tool_bindings: %w", err)
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentRepo - ListVersions - rows: %w", err)
	}

	return records, nil
}
