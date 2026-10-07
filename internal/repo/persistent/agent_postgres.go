// Package persistent implements repository storage on PostgreSQL.
package persistent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
)

// AgentRepo implements repo.AgentRepo with Postgres. Statements and bindings
// come from queries/agent.sql.
type AgentRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewAgentRepo creates a Postgres-backed agent repository.
func NewAgentRepo(pg *postgres.Postgres) *AgentRepo {
	return &AgentRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// Create inserts a new agent record.
func (r *AgentRepo) Create(ctx context.Context, req *entity.CreateAgentRequest) (entity.AgentRecord, error) {
	configJSON, err := json.Marshal(req.Config)
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Create - marshal config: %w", err)
	}

	row, err := r.queries.InsertAgent(ctx, sqlcgen.InsertAgentParams{
		AccountID:    req.AccountID,
		Name:         req.Name,
		Description:  req.Description,
		SystemPrompt: req.SystemPrompt,
		ModelRef:     req.ModelRef,
		Config:       configJSON,
	})
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Create - query: %w", err)
	}

	return agentRecordFromRow("Create", &row)
}

// Get retrieves an agent record by ID.
func (r *AgentRepo) Get(ctx context.Context, agentID string) (entity.AgentRecord, bool, error) {
	row, err := r.queries.GetAgent(ctx, agentID)
	if missingRow(err) {
		return entity.AgentRecord{}, false, nil
	}

	if err != nil {
		return entity.AgentRecord{}, false, fmt.Errorf("AgentRepo - Get - query: %w", err)
	}

	record, err := agentRecordFromRow("Get", &row)
	if err != nil {
		return entity.AgentRecord{}, false, err
	}

	return record, true, nil
}

// Update modifies an existing agent record.
// This is a plain data update. Version management (auto-snapshotting config
// changes) is handled by the usecase layer.
func (r *AgentRepo) Update(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
	params := sqlcgen.UpdateAgentParams{
		AgentID:        agentID,
		Name:           optionalText(req.Name),
		Description:    optionalText(req.Description),
		SystemPrompt:   optionalText(req.SystemPrompt),
		ModelRef:       optionalText(req.ModelRef),
		IsDefault:      optionalBool(req.IsDefault),
		CurrentVersion: optionalText(req.CurrentVersion),
	}

	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Update - marshal config: %w", err)
		}

		params.Config = configJSON
	}

	row, err := r.queries.UpdateAgent(ctx, params)
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("AgentRepo - Update - query: %w", err)
	}

	return agentRecordFromRow("Update", &row)
}

// Delete removes an agent record by ID.
func (r *AgentRepo) Delete(ctx context.Context, agentID string) error {
	if err := r.queries.DeleteAgent(ctx, agentID); err != nil {
		return fmt.Errorf("AgentRepo - Delete - exec: %w", err)
	}

	return nil
}

// ListByAccount retrieves all agent records for an account.
func (r *AgentRepo) ListByAccount(ctx context.Context, accountID string) ([]entity.AgentRecord, error) {
	rows, err := r.queries.ListAgentsByAccount(ctx, accountID)

	return listRecords("AgentRepo - ListByAccount", rows, err, func(row *sqlcgen.Agent) (entity.AgentRecord, error) {
		return agentRecordFromRow("ListByAccount", row)
	})
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

	err = r.queries.InsertAgentVersion(ctx, sqlcgen.InsertAgentVersionParams{
		VersionID:         record.VersionID,
		AgentID:           record.AgentID,
		VersionName:       record.VersionName,
		SystemPrompt:      record.SystemPrompt,
		ModelRef:          record.ModelRef,
		Config:            configJSON,
		ToolBindings:      toolBindingsJSON,
		ChangeDescription: record.ChangeDescription,
	})
	if err != nil {
		return fmt.Errorf("AgentRepo - CreateVersion - exec: %w", err)
	}

	return nil
}

// GetVersion retrieves a version snapshot by ID.
func (r *AgentRepo) GetVersion(ctx context.Context, versionID string) (entity.AgentVersionRecord, bool, error) {
	row, err := r.queries.GetAgentVersion(ctx, versionID)
	if missingRow(err) {
		return entity.AgentVersionRecord{}, false, nil
	}

	if err != nil {
		return entity.AgentVersionRecord{}, false, fmt.Errorf("AgentRepo - GetVersion - query: %w", err)
	}

	record, err := agentVersionFromRow("GetVersion", &row)
	if err != nil {
		return entity.AgentVersionRecord{}, false, err
	}

	return record, true, nil
}

// ListVersions retrieves all version snapshots for an agent.
func (r *AgentRepo) ListVersions(ctx context.Context, agentID string) ([]entity.AgentVersionRecord, error) {
	rows, err := r.queries.ListAgentVersions(ctx, agentID)

	return listRecords("AgentRepo - ListVersions", rows, err, func(row *sqlcgen.AgentVersion) (entity.AgentVersionRecord, error) {
		return agentVersionFromRow("ListVersions", row)
	})
}

// agentRecordFromRow shapes a generated row into the entity, decoding the
// config JSON document.
func agentRecordFromRow(name string, row *sqlcgen.Agent) (entity.AgentRecord, error) {
	record := agentRecordColumns(row)

	if err := decodeRecordJSON("AgentRepo - "+name, "config", row.Config, &record.Config); err != nil {
		return entity.AgentRecord{}, err
	}

	return record, nil
}

// agentRecordColumns copies the stored columns of one agent row; the JSON
// document column is decoded by the caller.
func agentRecordColumns(row *sqlcgen.Agent) entity.AgentRecord {
	return entity.AgentRecord{
		AgentID:        row.AgentID,
		AccountID:      row.AccountID,
		Name:           row.Name,
		Description:    row.Description,
		SystemPrompt:   row.SystemPrompt,
		ModelRef:       row.ModelRef,
		CurrentVersion: row.CurrentVersion,
		IsDefault:      row.IsDefault,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
	}
}

// agentVersionFromRow shapes a generated row into the entity, decoding the
// config and tool_bindings JSON documents.
func agentVersionFromRow(name string, row *sqlcgen.AgentVersion) (entity.AgentVersionRecord, error) {
	record := entity.AgentVersionRecord{
		VersionID:         row.VersionID,
		AgentID:           row.AgentID,
		VersionName:       row.VersionName,
		SystemPrompt:      row.SystemPrompt,
		ModelRef:          row.ModelRef,
		ChangeDescription: row.ChangeDescription,
		CreatedAt:         row.CreatedAt,
	}

	if err := decodeRecordJSON("AgentRepo - "+name, "config", row.Config, &record.Config); err != nil {
		return entity.AgentVersionRecord{}, err
	}

	if err := decodeRecordJSON("AgentRepo - "+name, "tool_bindings", row.ToolBindings, &record.ToolBindings); err != nil {
		return entity.AgentVersionRecord{}, err
	}

	return record, nil
}
