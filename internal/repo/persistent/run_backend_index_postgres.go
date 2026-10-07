package persistent

import (
	"context"
	"errors"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/jackc/pgx/v5/pgtype"
)

// RunBackendIndexRepo persists run ownership for AgentOS routing. Statements
// and bindings come from queries/run_backend_index.sql; this file owns the
// idempotency protocol around them, not the SQL.
type RunBackendIndexRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

var errRunBackendIndexRecordRequired = errors.New("run backend index record is required")

// NewRunBackendIndexRepo creates a Postgres-backed run backend index.
func NewRunBackendIndexRepo(pg *postgres.Postgres) *RunBackendIndexRepo {
	return &RunBackendIndexRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
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
	row, err := r.queries.GetRunBackendIndex(ctx, runID)
	if missingRow(err) {
		return entity.RunBackendIndexRecord{}, false, nil
	}

	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - Get - query: %w", err)
	}

	return runBackendIndexRecordFromRow(&row), true, nil
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

	row, err := r.queries.GetRunBackendIndexByIdempotencyKey(ctx, sqlcgen.GetRunBackendIndexByIdempotencyKeyParams{
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		IdempotencyKey: idempotencyKey,
	})
	if missingRow(err) {
		return entity.RunBackendIndexRecord{}, false, nil
	}

	if err != nil {
		return entity.RunBackendIndexRecord{}, false, fmt.Errorf("RunBackendIndexRepo - runByIdempotencyKey - query: %w", err)
	}

	return runBackendIndexRecordFromIdempotencyRow(&row), true, nil
}

// runBackendIndexRecordFromIdempotencyRow is the by-key twin of
// runBackendIndexRecordFromRow: the two lookups project the same columns but
// generate distinct row types.
func runBackendIndexRecordFromIdempotencyRow(row *sqlcgen.GetRunBackendIndexByIdempotencyKeyRow) entity.RunBackendIndexRecord {
	return entity.RunBackendIndexRecord{
		RunID:          row.RunID,
		PlanID:         row.PlanID,
		NodeID:         row.NodeID,
		ThreadID:       row.ThreadID,
		AccountID:      row.AccountID,
		ProjectID:      row.ProjectID,
		BackendKind:    row.BackendKind,
		BackendName:    row.BackendName,
		IdempotencyKey: row.IdempotencyKey,
		LifecycleState: row.LifecycleState,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
	}
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

	inserted, err := r.queries.InsertRunBackendIndex(ctx, sqlcgen.InsertRunBackendIndexParams{
		RunID:          record.RunID,
		PlanID:         nullableText(record.PlanID),
		NodeID:         nullableText(record.NodeID),
		ThreadID:       record.ThreadID,
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		BackendKind:    record.BackendKind,
		BackendName:    record.BackendName,
		IdempotencyKey: record.IdempotencyKey,
		LifecycleState: record.LifecycleState,
	})
	if err != nil {
		return r.handleUpsertExecError(ctx, err, record)
	}

	if inserted == 0 {
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
	nodeID    pgtype.Text
	runID     pgtype.Text
}

func (r *RunBackendIndexRepo) planNodeOwner(ctx context.Context, planID, nodeID string) (planNodeOwner, error) {
	row, err := r.queries.GetPlanNodeOwner(ctx, sqlcgen.GetPlanNodeOwnerParams{
		PlanID: planID,
		NodeID: nodeID,
	})
	if missingRow(err) {
		return planNodeOwner{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, planID)
	}

	if err != nil {
		return planNodeOwner{}, fmt.Errorf("RunBackendIndexRepo - validatePlanNodeOwnership - query: %w", err)
	}

	return planNodeOwner{accountID: row.AccountID, projectID: row.ProjectID, nodeID: row.NodeID, runID: row.RunID}, nil
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

	err := r.queries.UpdateRunBackendLifecycle(ctx, sqlcgen.UpdateRunBackendLifecycleParams{
		RunID:          existing.RunID,
		LifecycleState: lifecycle,
	})
	if err != nil {
		return fmt.Errorf("RunBackendIndexRepo - updateLifecycle - exec: %w", err)
	}

	return nil
}

// runBackendIndexRecordFromRow shapes a generated by-run-id row into the
// domain record. The nullable plan/node columns read as empty strings — the
// record is a flat ownership fact, and "no plan" is the same fact as "empty
// plan".
func runBackendIndexRecordFromRow(row *sqlcgen.GetRunBackendIndexRow) entity.RunBackendIndexRecord {
	return entity.RunBackendIndexRecord{
		RunID:          row.RunID,
		PlanID:         row.PlanID,
		NodeID:         row.NodeID,
		ThreadID:       row.ThreadID,
		AccountID:      row.AccountID,
		ProjectID:      row.ProjectID,
		BackendKind:    row.BackendKind,
		BackendName:    row.BackendName,
		IdempotencyKey: row.IdempotencyKey,
		LifecycleState: row.LifecycleState,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
	}
}

// nullableText turns an empty string into SQL NULL for the index's nullable
// plan/node columns: standalone runs have no plan row to point at.
func nullableText(value string) pgtype.Text {
	return pgtype.Text{String: value, Valid: value != ""}
}
