package persistent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/jackc/pgx/v5"
)

// RunBackendIndexRepo persists run ownership for AgentOS routing.
type RunBackendIndexRepo struct {
	*postgres.Postgres
}

var errRunBackendIndexRecordRequired = errors.New("run backend index record is required")

// NewRunBackendIndexRepo creates a Postgres-backed run backend index.
func NewRunBackendIndexRepo(pg *postgres.Postgres) *RunBackendIndexRepo {
	return &RunBackendIndexRepo{pg}
}

// Bind records run backend ownership for a run.
func (r *RunBackendIndexRepo) Bind(ctx context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) error {
	record := agentosruntime.RunBackendIndexRecordFromRunSpec(spec, status)

	return r.upsert(ctx, &record, true)
}

// BindPlanNode records run backend ownership for a plan node run after validating node ownership.
func (r *RunBackendIndexRepo) BindPlanNode(ctx context.Context, planID, nodeID string, spec *agentos.RunSpec, status *agentos.RunStatus) error {
	record := agentosruntime.RunBackendIndexRecordFromPlanNode(planID, nodeID, spec, status)
	if err := r.validatePlanNodeOwnership(ctx, &record); err != nil {
		return err
	}

	return r.upsert(ctx, &record, true)
}

// Resolve returns the backend that owns the given run ID.
func (r *RunBackendIndexRepo) Resolve(ctx context.Context, runID string) (agentos.BackendRef, error) {
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

// Get loads the run backend index record for the given run ID.
func (r *RunBackendIndexRepo) Get(ctx context.Context, runID string) (entity.RunBackendIndexRecord, bool, error) {
	query, args, err := r.Builder.
		Select(runBackendIndexColumns()...).
		From("run_backend_index").
		Where(sq.Eq{_colRunID: runID}).
		ToSql()
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - builder: %w", err)
	}

	record, exists, err := scanRunBackendIndexRecord(r.Pool.QueryRow(ctx, query, args...))
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - query: %w", err)
	}

	return record, exists, nil
}

// GetRunBackend loads the backend ownership for the given run ID.
func (r *RunBackendIndexRepo) GetRunBackend(ctx context.Context, runID string) (agentos.RunBackendOwnership, bool, error) {
	record, exists, err := r.Get(ctx, runID)
	if err != nil || !exists {
		return agentos.RunBackendOwnership{}, exists, err
	}

	return agentosruntime.RunBackendOwnershipFromRecord(&record), true, nil
}

func (r *RunBackendIndexRepo) runByIdempotencyKey(ctx context.Context, record *entity.RunBackendIndexRecord) (entity.RunBackendIndexRecord, bool, error) {
	idempotencyKey := record.IdempotencyKey
	if idempotencyKey == "" {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("%w: run backend idempotency key is required", agentoscore.ErrInvalidRunSpec)
	}

	query, args, err := r.Builder.
		Select(runBackendIndexColumns()...).
		From("run_backend_index").
		Where(sq.Eq{
			_colAccountID:      record.AccountID,
			_colProjectID:      record.ProjectID,
			_colIDempotencyKey: idempotencyKey,
		}).
		ToSql()
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - runByIdempotencyKey - builder: %w", err)
	}

	found, exists, err := scanRunBackendIndexRecord(r.Pool.QueryRow(ctx, query, args...))
	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - runByIdempotencyKey - query: %w", err)
	}

	return found, exists, nil
}

func (r *RunBackendIndexRepo) upsert(ctx context.Context, record *entity.RunBackendIndexRecord, requireIdempotencyKey bool) error {
	if record == nil {
		return errRunBackendIndexRecordRequired
	}

	normalized := agentosruntime.NormalizeRunBackendIndexRecord(record)
	record = &normalized

	if err := agentosruntime.ValidateRunBackendIndexRecord(record, requireIdempotencyKey); err != nil {
		return err
	}

	if err := r.upsertByIdempotencyKey(ctx, record); err != nil {
		return err
	}

	query, args, err := buildRunBackendIndexUpsertSQL(r.Builder, record)
	if err != nil {
		return fmt.Errorf("RunBackendIndexRepo - upsert - builder: %w", err)
	}

	tag, execErr := r.Pool.Exec(ctx, query, args...)
	if execErr != nil {
		return r.handleUpsertExecError(ctx, execErr, record)
	}

	if tag.RowsAffected() == 0 {
		return r.handleUpsertConflict(ctx, record)
	}

	return nil
}

func (r *RunBackendIndexRepo) upsertByIdempotencyKey(ctx context.Context, record *entity.RunBackendIndexRecord) error {
	if record.IdempotencyKey == "" {
		return nil
	}

	existing, exists, err := r.runByIdempotencyKey(ctx, record)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	if err := agentosruntime.ValidateRunBackendIndexIdempotency(&existing, record); err != nil {
		return err
	}

	return r.updateLifecycle(ctx, &existing, record)
}

func buildRunBackendIndexUpsertSQL(builder sq.StatementBuilderType, record *entity.RunBackendIndexRecord) (query string, args []any, err error) {
	return builder.
		Insert("run_backend_index").
		Columns(
			_colRunID,
			_colPlanID,
			"node_id",
			"thread_id",
			_colAccountID,
			_colProjectID,
			"backend_kind",
			"backend_name",
			_colIDempotencyKey,
			"lifecycle_state",
		).
		Values(
			record.RunID,
			nullableString(record.PlanID),
			nullableString(record.NodeID),
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
}

func (r *RunBackendIndexRepo) handleUpsertExecError(ctx context.Context, execErr error, record *entity.RunBackendIndexRecord) error {
	if !isPostgresUniqueViolation(execErr) || record.IdempotencyKey == "" {
		return fmt.Errorf("RunBackendIndexRepo - upsert - exec: %w", execErr)
	}

	existing, exists, lookupErr := r.runByIdempotencyKey(ctx, record)
	if lookupErr != nil {
		return lookupErr
	}

	if exists {
		return agentosruntime.ValidateRunBackendIndexIdempotency(&existing, record)
	}

	return fmt.Errorf("RunBackendIndexRepo - upsert - exec: %w", execErr)
}

func (r *RunBackendIndexRepo) handleUpsertConflict(ctx context.Context, record *entity.RunBackendIndexRecord) error {
	existing, exists, err := r.Get(ctx, record.RunID)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: run %q conflict did not leave an ownership record", agentoscore.ErrRunRouteNotFound, record.RunID)
	}

	if err := agentosruntime.ValidateRunBackendIndexIdempotency(&existing, record); err != nil {
		return err
	}

	return r.updateLifecycle(ctx, &existing, record)
}

func (r *RunBackendIndexRepo) validatePlanNodeOwnership(ctx context.Context, record *entity.RunBackendIndexRecord) error {
	normalized := agentosruntime.NormalizeRunBackendIndexRecord(record)
	record = &normalized

	if err := validatePlanNodeOwnershipRecord(record); err != nil {
		return err
	}

	owner, err := r.planNodeOwner(ctx, record.PlanID, record.NodeID)
	if err != nil {
		return err
	}

	return owner.validate(record)
}

func validatePlanNodeOwnershipRecord(record *entity.RunBackendIndexRecord) error {
	if err := agentosruntime.ValidateRunBackendIndexRecord(record, true); err != nil {
		return err
	}

	if record.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if record.NodeID == "" {
		return fmt.Errorf("%w: node id is required", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}

type planNodeOwner struct {
	accountID string
	projectID string
	nodeID    sql.NullString
	runID     sql.NullString
}

func (r *RunBackendIndexRepo) planNodeOwner(ctx context.Context, planID, nodeID string) (planNodeOwner, error) {
	var owner planNodeOwner

	err := r.Pool.QueryRow(ctx, `
SELECT p.account_id, p.project_id, n.node_id, n.run_id
FROM plans p
LEFT JOIN plan_nodes n
    ON n.plan_id = p.plan_id
   AND n.node_id = $2
WHERE p.plan_id = $1`, planID, nodeID).Scan(&owner.accountID, &owner.projectID, &owner.nodeID, &owner.runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return planNodeOwner{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, planID)
		}

		return planNodeOwner{}, fmt.Errorf("RunBackendIndexRepo - validatePlanNodeOwnership - query: %w", err)
	}

	return owner, nil
}

func (o *planNodeOwner) validate(record *entity.RunBackendIndexRecord) error {
	if o.accountID != record.AccountID || o.projectID != record.ProjectID {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, record.PlanID)
	}

	if !o.nodeID.Valid || o.nodeID.String == "" {
		return fmt.Errorf("%w: plan node %q is not durable", agentoscore.ErrInvalidRunPlan, record.NodeID)
	}

	if o.runID.Valid && o.runID.String != "" && o.runID.String != record.RunID {
		return fmt.Errorf("%w: plan node %q has durable run id %q, got %q", agentoscore.ErrInvalidRunSpec, record.NodeID, o.runID.String, record.RunID)
	}

	return nil
}

func (r *RunBackendIndexRepo) updateLifecycle(ctx context.Context, existing, requested *entity.RunBackendIndexRecord) error {
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
		_colRunID,
		"COALESCE(plan_id, '') AS plan_id",
		"COALESCE(node_id, '') AS node_id",
		"thread_id",
		_colAccountID,
		_colProjectID,
		"backend_kind",
		"backend_name",
		_colIDempotencyKey,
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

func nullableString(value string) any {
	if value == "" {
		return nil
	}

	return value
}
