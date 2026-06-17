package persistent

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// RunBackendIndexRepo persists run ownership for AgentOS routing.
type RunBackendIndexRepo struct {
	*postgres.Postgres
}

// NewRunBackendIndexRepo creates a Postgres-backed run backend index.
func NewRunBackendIndexRepo(pg *postgres.Postgres) *RunBackendIndexRepo {
	return &RunBackendIndexRepo{pg}
}

func (r *RunBackendIndexRepo) Bind(ctx context.Context, spec agentos.RunSpec) error {
	return r.upsert(ctx, entity.RunBackendIndexRecord{
		RunID:          spec.RunID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		BackendKind:    string(spec.Backend.Kind),
		BackendName:    spec.Backend.Name,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: "created",
	})
}

func (r *RunBackendIndexRepo) BindPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error {
	runID := status.RunID
	if runID == "" {
		runID = spec.RunID
	}
	lifecycle := status.LifecycleState
	if lifecycle == "" {
		lifecycle = "created"
	}

	return r.upsert(ctx, entity.RunBackendIndexRecord{
		RunID:          runID,
		PlanID:         planID,
		NodeID:         nodeID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		BackendKind:    string(spec.Backend.Kind),
		BackendName:    spec.Backend.Name,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: lifecycle,
	})
}

func (r *RunBackendIndexRepo) Resolve(ctx context.Context, runID string) (agentos.BackendRef, error) {
	record, exists, err := r.Get(ctx, runID)
	if err != nil {
		return agentos.BackendRef{}, err
	}
	if !exists {
		return agentos.BackendRef{}, fmt.Errorf("%w: %s", agentos.ErrRunRouteNotFound, runID)
	}

	return agentos.BackendRef{
		Kind: agentos.BackendKind(record.BackendKind),
		Name: record.BackendName,
	}, nil
}

func (r *RunBackendIndexRepo) Get(ctx context.Context, runID string) (entity.RunBackendIndexRecord, bool, error) {
	sql, args, err := r.Builder.
		Select(
			"run_id",
			"plan_id",
			"node_id",
			"thread_id",
			"account_id",
			"project_id",
			"backend_kind",
			"backend_name",
			"idempotency_key",
			"lifecycle_state",
			"created_at",
			"updated_at",
		).
		From("run_backend_index").
		Where(sq.Eq{"run_id": runID}).
		ToSql()
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - builder: %w", err)
	}

	var record entity.RunBackendIndexRecord
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.RunID,
		&record.PlanID,
		&record.NodeID,
		&record.ThreadID,
		&record.AccountID,
		&record.ProjectID,
		&record.BackendKind,
		&record.BackendName,
		&record.IdempotencyKey,
		&record.LifecycleState,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.RunBackendIndexRecord{}, false, nil
		}

		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - query: %w", err)
	}

	return record, true, nil
}

func (r *RunBackendIndexRepo) upsert(ctx context.Context, record entity.RunBackendIndexRecord) error {
	if record.RunID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if record.BackendKind == "" || record.BackendName == "" {
		return fmt.Errorf("%w: kind and name are required", agentos.ErrInvalidBackendRef)
	}
	if record.LifecycleState == "" {
		record.LifecycleState = "created"
	}

	sql, args, err := r.Builder.
		Insert("run_backend_index").
		Columns(
			"run_id",
			"plan_id",
			"node_id",
			"thread_id",
			"account_id",
			"project_id",
			"backend_kind",
			"backend_name",
			"idempotency_key",
			"lifecycle_state",
		).
		Values(
			record.RunID,
			record.PlanID,
			record.NodeID,
			record.ThreadID,
			record.AccountID,
			record.ProjectID,
			record.BackendKind,
			record.BackendName,
			record.IdempotencyKey,
			record.LifecycleState,
		).
		Suffix(`
ON CONFLICT (run_id) DO UPDATE SET
    plan_id = EXCLUDED.plan_id,
    node_id = EXCLUDED.node_id,
    thread_id = EXCLUDED.thread_id,
    account_id = EXCLUDED.account_id,
    project_id = EXCLUDED.project_id,
    backend_kind = EXCLUDED.backend_kind,
    backend_name = EXCLUDED.backend_name,
    idempotency_key = EXCLUDED.idempotency_key,
    lifecycle_state = EXCLUDED.lifecycle_state,
    updated_at = NOW()`).
		ToSql()
	if err != nil {
		return fmt.Errorf("RunBackendIndexRepo - upsert - builder: %w", err)
	}
	if _, err := r.Pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("RunBackendIndexRepo - upsert - exec: %w", err)
	}

	return nil
}
