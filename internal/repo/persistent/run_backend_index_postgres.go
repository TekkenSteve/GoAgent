package persistent

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
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
	return r.upsert(ctx, agentosruntime.RunBackendIndexRecordFromRunSpec(spec), true)
}

func (r *RunBackendIndexRepo) BindPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error {
	return r.upsert(ctx, agentosruntime.RunBackendIndexRecordFromPlanNode(planID, nodeID, spec, status), true)
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
		Select(runBackendIndexColumns()...).
		From("run_backend_index").
		Where(sq.Eq{"run_id": runID}).
		ToSql()
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - builder: %w", err)
	}

	record, exists, err := scanRunBackendIndexRecord(r.Pool.QueryRow(ctx, sql, args...))
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - query: %w", err)
	}

	return record, exists, nil
}

func (r *RunBackendIndexRepo) GetRunBackend(ctx context.Context, runID string) (agentos.RunBackendOwnership, bool, error) {
	record, exists, err := r.Get(ctx, runID)
	if err != nil || !exists {
		return agentos.RunBackendOwnership{}, exists, err
	}

	return agentosruntime.RunBackendOwnershipFromRecord(record), true, nil
}

func (r *RunBackendIndexRepo) runByIdempotencyKey(ctx context.Context, record entity.RunBackendIndexRecord) (entity.RunBackendIndexRecord, bool, error) {
	idempotencyKey := record.IdempotencyKey
	if idempotencyKey == "" {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("%w: run backend idempotency key is required", agentos.ErrInvalidRunSpec)
	}
	sql, args, err := r.Builder.
		Select(runBackendIndexColumns()...).
		From("run_backend_index").
		Where(sq.Eq{
			"account_id":      record.AccountID,
			"project_id":      record.ProjectID,
			"idempotency_key": idempotencyKey,
		}).
		ToSql()
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - runByIdempotencyKey - builder: %w", err)
	}

	record, exists, err := scanRunBackendIndexRecord(r.Pool.QueryRow(ctx, sql, args...))
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - runByIdempotencyKey - query: %w", err)
	}

	return record, exists, nil
}

func (r *RunBackendIndexRepo) upsert(ctx context.Context, record entity.RunBackendIndexRecord, requireIdempotencyKey bool) error {
	record = agentosruntime.NormalizeRunBackendIndexRecord(record)
	if err := agentosruntime.ValidateRunBackendIndexRecord(record, requireIdempotencyKey); err != nil {
		return err
	}
	if record.IdempotencyKey != "" {
		existing, exists, err := r.runByIdempotencyKey(ctx, record)
		if err != nil {
			return err
		}
		if exists {
			if err := agentosruntime.ValidateRunBackendIndexIdempotency(existing, record); err != nil {
				return err
			}

			return r.updateLifecycle(ctx, existing, record)
		}
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
		Suffix("ON CONFLICT (run_id) DO NOTHING").
		ToSql()
	if err != nil {
		return fmt.Errorf("RunBackendIndexRepo - upsert - builder: %w", err)
	}
	tag, err := r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		if isPostgresUniqueViolation(err) && record.IdempotencyKey != "" {
			existing, exists, lookupErr := r.runByIdempotencyKey(ctx, record)
			if lookupErr != nil {
				return lookupErr
			}
			if exists {
				return agentosruntime.ValidateRunBackendIndexIdempotency(existing, record)
			}
		}

		return fmt.Errorf("RunBackendIndexRepo - upsert - exec: %w", err)
	}
	if tag.RowsAffected() == 0 {
		existing, exists, err := r.Get(ctx, record.RunID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: run %q conflict did not leave an ownership record", agentos.ErrRunRouteNotFound, record.RunID)
		}

		if err := agentosruntime.ValidateRunBackendIndexIdempotency(existing, record); err != nil {
			return err
		}

		return r.updateLifecycle(ctx, existing, record)
	}

	return nil
}

func (r *RunBackendIndexRepo) updateLifecycle(ctx context.Context, existing entity.RunBackendIndexRecord, requested entity.RunBackendIndexRecord) error {
	lifecycle := requested.LifecycleState
	if lifecycle == agentosruntime.RunBackendLifecycleClaiming && existing.LifecycleState != agentosruntime.RunBackendLifecycleClaiming {
		return nil
	}
	if lifecycle == existing.LifecycleState {
		return nil
	}

	_, err := r.Pool.Exec(ctx, `
UPDATE run_backend_index
SET lifecycle_state = $2,
    updated_at = NOW()
WHERE run_id = $1`, existing.RunID, lifecycle)
	if err != nil {
		return fmt.Errorf("RunBackendIndexRepo - updateLifecycle - exec: %w", err)
	}

	return nil
}

func runBackendIndexColumns() []string {
	return []string{
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
	}
}

type runBackendIndexScanner interface {
	Scan(dest ...any) error
}

func scanRunBackendIndexRecord(scanner runBackendIndexScanner) (entity.RunBackendIndexRecord, bool, error) {
	var record entity.RunBackendIndexRecord
	err := scanner.Scan(
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

		return entity.RunBackendIndexRecord{}, false, err
	}

	return record, true, nil
}
