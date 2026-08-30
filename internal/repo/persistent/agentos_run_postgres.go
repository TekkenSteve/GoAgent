package persistent

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// AgentOSRunRepo persists AgentOS control-plane run routing.
type AgentOSRunRepo struct {
	*postgres.Postgres
}

// NewAgentOSRunRepo creates a Postgres-backed AgentOS run repository.
func NewAgentOSRunRepo(pg *postgres.Postgres) *AgentOSRunRepo {
	return &AgentOSRunRepo{pg}
}

// Bind stores the backend reference for a run.
func (r *AgentOSRunRepo) Bind(ctx context.Context, spec *agentos.RunSpec) error {
	return r.Upsert(ctx, &entity.AgentOSRunRecord{
		RunID:          spec.RunID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		BackendKind:    string(spec.Backend.Kind),
		BackendName:    spec.Backend.Name,
		LifecycleState: "created",
	})
}

// Resolve returns the backend reference that owns a run.
func (r *AgentOSRunRepo) Resolve(ctx context.Context, runID string) (agentos.BackendRef, error) {
	record, exists, err := r.Get(ctx, runID)
	if err != nil {
		return agentos.BackendRef{}, err
	}

	if !exists {
		return agentos.BackendRef{}, fmt.Errorf("%w: %s", agentoscore.ErrRunRouteNotFound, runID)
	}

	return agentos.BackendRef{
		Kind: agentos.BackendKind(record.BackendKind),
		Name: record.BackendName,
	}, nil
}

// Upsert creates or updates the control-plane run record.
func (r *AgentOSRunRepo) Upsert(ctx context.Context, record *entity.AgentOSRunRecord) error {
	if record.RunID == "" {
		return fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	if record.BackendKind == "" || record.BackendName == "" {
		return fmt.Errorf("%w: kind and name are required", agentoscore.ErrInvalidBackendRef)
	}

	if record.LifecycleState == "" {
		record.LifecycleState = "created"
	}

	sql, args, err := r.Builder.
		Insert("agentos_runs").
		Columns(
			_colRunID,
			"thread_id",
			_colAccountID,
			_colProjectID,
			"backend_kind",
			"backend_name",
			"external_workflow_id",
			"external_run_id",
			"lifecycle_state",
		).
		Values(
			record.RunID,
			record.ThreadID,
			record.AccountID,
			record.ProjectID,
			record.BackendKind,
			record.BackendName,
			record.ExternalWorkflowID,
			record.ExternalRunID,
			record.LifecycleState,
		).
		Suffix(`
ON CONFLICT (run_id) DO UPDATE SET
    thread_id = EXCLUDED.thread_id,
    account_id = EXCLUDED.account_id,
    project_id = EXCLUDED.project_id,
    backend_kind = EXCLUDED.backend_kind,
    backend_name = EXCLUDED.backend_name,
    external_workflow_id = EXCLUDED.external_workflow_id,
    external_run_id = EXCLUDED.external_run_id,
    lifecycle_state = EXCLUDED.lifecycle_state,
    updated_at = NOW()`).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentOSRunRepo - Upsert - builder: %w", err)
	}

	if _, err := r.Pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("AgentOSRunRepo - Upsert - exec: %w", err)
	}

	return nil
}

// Get returns one AgentOS run record.
func (r *AgentOSRunRepo) Get(ctx context.Context, runID string) (entity.AgentOSRunRecord, bool, error) {
	sql, args, err := r.Builder.
		Select(
			_colRunID,
			"thread_id",
			_colAccountID,
			_colProjectID,
			"backend_kind",
			"backend_name",
			"external_workflow_id",
			"external_run_id",
			"lifecycle_state",
			"created_at",
			"updated_at",
		).
		From("agentos_runs").
		Where(sq.Eq{_colRunID: runID}).
		ToSql()
	if err != nil {
		return entity.AgentOSRunRecord{}, false, fmt.Errorf("AgentOSRunRepo - Get - builder: %w", err)
	}

	var record entity.AgentOSRunRecord

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.RunID,
		&record.ThreadID,
		&record.AccountID,
		&record.ProjectID,
		&record.BackendKind,
		&record.BackendName,
		&record.ExternalWorkflowID,
		&record.ExternalRunID,
		&record.LifecycleState,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.AgentOSRunRecord{}, false, nil
		}

		return entity.AgentOSRunRecord{}, false, fmt.Errorf("AgentOSRunRepo - Get - query: %w", err)
	}

	return record, true, nil
}
