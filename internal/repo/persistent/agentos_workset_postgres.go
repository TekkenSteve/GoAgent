package persistent

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosbatch"
	"github.com/jackc/pgx/v5"
)

// AgentOSWorksetRepo persists batch workset specs, chunk keys, and lifecycle projections.
type AgentOSWorksetRepo struct {
	*postgres.Postgres
}

// NewAgentOSWorksetRepo creates a Postgres-backed workset store.
func NewAgentOSWorksetRepo(pg *postgres.Postgres) *AgentOSWorksetRepo {
	return &AgentOSWorksetRepo{Postgres: pg}
}

// CreateWorkset persists a new workset from its spec and status, returning the stored status and whether the workset was newly created.
func (r *AgentOSWorksetRepo) CreateWorkset(ctx context.Context, spec *agentos.WorksetSpec, status *agentos.WorksetStatus) (agentos.WorksetStatus, bool, error) {
	if err := validatePostgresCreateWorkset(spec, status); err != nil {
		return agentos.WorksetStatus{}, false, err
	}

	existingSpec, existingStatus, exists, err := r.worksetByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil {
		return agentos.WorksetStatus{}, false, err
	}

	if exists {
		return existingStatus, false, agentosbatch.ValidateWorksetStartIdempotency(&existingSpec, spec)
	}

	existingSpec, existingStatus, exists, err = r.worksetByID(ctx, spec.WorksetID)
	if err != nil {
		return agentos.WorksetStatus{}, false, err
	}

	if exists {
		return existingStatus, false, agentosbatch.ValidateWorksetStartIdempotency(&existingSpec, spec)
	}

	return r.insertWorkset(ctx, spec, status)
}

// GetWorkset loads a workset spec and status by tenant-scoped reference.
func (r *AgentOSWorksetRepo) GetWorkset(ctx context.Context, ref agentos.WorksetRef) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error) {
	if err := agentos.ValidateWorksetRef(ref); err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, err
	}

	return getProcessPlatformProjection[agentos.WorksetSpec, agentos.WorksetStatus](
		ctx,
		processPlatformTenantRef{ID: ref.WorksetID, AccountID: ref.AccountID, ProjectID: ref.ProjectID},
		r.worksetByID,
		validatePostgresWorksetTenant(ref),
	)
}

// ListWorksets returns workset statuses matching the given scope filters.
func (r *AgentOSWorksetRepo) ListWorksets(ctx context.Context, scope *agentos.WorksetScope) ([]agentos.WorksetStatus, error) {
	if err := agentos.ValidateWorksetScope(scope); err != nil {
		return nil, err
	}

	query := worksetStatusListQuery(scope)

	return listProcessPlatformStatuses[agentos.WorksetStatus](ctx, r.Postgres, &query)
}

// ApplyChunkResult idempotently applies a chunk result and returns the resulting workset status.
func (r *AgentOSWorksetRepo) ApplyChunkResult(ctx context.Context, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, status *agentos.WorksetStatus) (agentos.WorksetStatus, error) {
	if err := validatePostgresApplyChunkResult(ref, result, status); err != nil {
		return agentos.WorksetStatus{}, err
	}

	existing, exists, err := r.worksetChunkStatusByKey(ctx, ref, result.ChunkID, result.IdempotencyKey)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if exists {
		return existing, nil
	}

	spec, _, exists, err := r.worksetByID(ctx, ref.WorksetID)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !exists {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q not found", agentoscore.ErrInvalidWorksetScope, ref.WorksetID)
	}

	if spec.AccountID != ref.AccountID || spec.ProjectID != ref.ProjectID {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q is outside tenant scope", agentoscore.ErrInvalidWorksetScope, ref.WorksetID)
	}

	normalized := normalizePostgresWorksetStatus(&spec, status)

	return r.applyWorksetChunkResult(ctx, ref, result, &normalized)
}

// UpdateWorksetStatus applies a workset status update idempotently and returns the resulting status.
func (r *AgentOSWorksetRepo) UpdateWorksetStatus(ctx context.Context, status *agentos.WorksetStatus, idempotencyKey string) (agentos.WorksetStatus, error) {
	if err := validatePostgresWorksetStatus(status, idempotencyKey); err != nil {
		return agentos.WorksetStatus{}, err
	}

	existing, exists, err := r.worksetStatusByIdempotencyKey(ctx, status.WorksetID, status.AccountID, status.ProjectID, idempotencyKey)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if exists {
		return existing, nil
	}

	spec, _, exists, err := r.worksetByID(ctx, status.WorksetID)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !exists {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q not found", agentoscore.ErrInvalidWorksetScope, status.WorksetID)
	}

	if spec.AccountID != status.AccountID || spec.ProjectID != status.ProjectID {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q status is outside tenant scope", agentoscore.ErrInvalidWorksetScope, status.WorksetID)
	}

	normalized := normalizePostgresWorksetStatus(&spec, status)

	return r.applyWorksetStatus(ctx, &normalized, idempotencyKey)
}

func (r *AgentOSWorksetRepo) insertWorkset(ctx context.Context, spec *agentos.WorksetSpec, status *agentos.WorksetStatus) (agentos.WorksetStatus, bool, error) {
	return createProcessPlatformProjection(ctx, r.Postgres, spec, status, processPlatformProjectionCreateConfig[agentos.WorksetSpec, agentos.WorksetStatus]{
		SpecMarshalName:   "AgentOSWorksetRepo - CreateWorkset spec",
		StatusMarshalName: "AgentOSWorksetRepo - CreateWorkset status",
		Normalize:         normalizePostgresWorksetStatus,
		BuildRow:          worksetProjectionInsertRow,
		ResolveInsertErr:  r.resolveWorksetInsertErr,
	})
}

func (r *AgentOSWorksetRepo) applyWorksetStatus(ctx context.Context, status *agentos.WorksetStatus, idempotencyKey string) (agentos.WorksetStatus, error) {
	statusJSON, err := marshalProcessPlatformJSON("AgentOSWorksetRepo - UpdateWorksetStatus status", status)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.WorksetStatus{}, fmt.Errorf("AgentOSWorksetRepo - UpdateWorksetStatus - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	claimed, existing, err := claimWorksetStatusUpdate(ctx, tx, status, idempotencyKey, statusJSON)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !claimed {
		return existing, nil
	}

	if err := updateWorksetProjection(ctx, tx, status, statusJSON); err != nil {
		return agentos.WorksetStatus{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.WorksetStatus{}, fmt.Errorf("AgentOSWorksetRepo - UpdateWorksetStatus - commit: %w", err)
	}

	return *status, nil
}

func (r *AgentOSWorksetRepo) applyWorksetChunkResult(ctx context.Context, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, status *agentos.WorksetStatus) (agentos.WorksetStatus, error) {
	statusJSON, err := marshalProcessPlatformJSON("AgentOSWorksetRepo - ApplyChunkResult status", status)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	resultJSON, err := marshalProcessPlatformJSON("AgentOSWorksetRepo - ApplyChunkResult result", result)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.WorksetStatus{}, fmt.Errorf("AgentOSWorksetRepo - ApplyChunkResult - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	claimed, existing, err := claimWorksetChunkResult(ctx, tx, ref, result, resultJSON, statusJSON)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !claimed {
		return existing, nil
	}

	if err := updateWorksetProjection(ctx, tx, status, statusJSON); err != nil {
		return agentos.WorksetStatus{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.WorksetStatus{}, fmt.Errorf("AgentOSWorksetRepo - ApplyChunkResult - commit: %w", err)
	}

	return *status, nil
}

func updateWorksetProjection(ctx context.Context, tx pgx.Tx, status *agentos.WorksetStatus, statusJSON []byte) error {
	tag, err := tx.Exec(
		ctx, `
UPDATE worksets
SET lifecycle_state = $1, status_json = $2, updated_at = $3
WHERE workset_id = $4 AND account_id = $5 AND project_id = $6`,
		status.LifecycleState,
		statusJSON,
		status.UpdatedAt,
		status.WorksetID,
		status.AccountID,
		status.ProjectID,
	)
	if err != nil {
		return fmt.Errorf("AgentOSWorksetRepo - update projection: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: workset %q not found", agentoscore.ErrInvalidWorksetScope, status.WorksetID)
	}

	return nil
}

func claimWorksetStatusUpdate(ctx context.Context, tx pgx.Tx, status *agentos.WorksetStatus, idempotencyKey string, statusJSON []byte) (bool, agentos.WorksetStatus, error) {
	return claimProcessPlatformStatus[agentos.WorksetStatus](ctx, tx, &processPlatformStatusClaim{
		ClaimName:       "AgentOSWorksetRepo - claimWorksetStatusUpdate",
		ExistingName:    "AgentOSWorksetRepo - existingWorksetStatusUpdate",
		InsertErrPrefix: "AgentOSWorksetRepo - UpdateWorksetStatus - insert status key",
		SelectErrPrefix: "AgentOSWorksetRepo - UpdateWorksetStatus - existing status key",
		InsertSQL: `
INSERT INTO workset_status_updates (workset_id, account_id, project_id, idempotency_key, status_json)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (account_id, project_id, workset_id, idempotency_key) DO NOTHING
RETURNING status_json`,
		InsertArgs: []any{status.WorksetID, status.AccountID, status.ProjectID, idempotencyKey, statusJSON},
		SelectSQL: `
SELECT status_json
FROM workset_status_updates
WHERE workset_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4`,
		SelectArgs: []any{status.WorksetID, status.AccountID, status.ProjectID, idempotencyKey},
	})
}

func claimWorksetChunkResult(ctx context.Context, tx pgx.Tx, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, resultJSON, statusJSON []byte) (bool, agentos.WorksetStatus, error) {
	return claimProcessPlatformStatus[agentos.WorksetStatus](ctx, tx, &processPlatformStatusClaim{
		ClaimName:       "AgentOSWorksetRepo - claimWorksetChunkResult",
		ExistingName:    "AgentOSWorksetRepo - existingWorksetChunkResult",
		InsertErrPrefix: "AgentOSWorksetRepo - ApplyChunkResult - insert chunk key",
		SelectErrPrefix: "AgentOSWorksetRepo - ApplyChunkResult - existing chunk key",
		InsertSQL: `
INSERT INTO workset_chunk_results (workset_id, account_id, project_id, chunk_id, idempotency_key, result_json, status_json)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (account_id, project_id, workset_id, chunk_id, idempotency_key) DO NOTHING
RETURNING status_json`,
		InsertArgs: []any{ref.WorksetID, ref.AccountID, ref.ProjectID, result.ChunkID, result.IdempotencyKey, resultJSON, statusJSON},
		SelectSQL: `
SELECT status_json
FROM workset_chunk_results
WHERE workset_id = $1 AND account_id = $2 AND project_id = $3 AND chunk_id = $4 AND idempotency_key = $5`,
		SelectArgs: []any{ref.WorksetID, ref.AccountID, ref.ProjectID, result.ChunkID, result.IdempotencyKey},
	})
}

func (r *AgentOSWorksetRepo) resolveWorksetInsertErr(ctx context.Context, insertErr error, spec *agentos.WorksetSpec) (agentos.WorksetStatus, error) {
	return resolveProcessPlatformProjectionInsertErr(
		ctx,
		insertErr,
		spec.WorksetID,
		agentoscore.ErrInvalidWorkset,
		"AgentOSWorksetRepo - CreateWorkset - insert",
		"workset",
		func(ctx context.Context) (agentos.WorksetStatus, bool, error) {
			_, existing, exists, err := r.worksetByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)

			return existing, exists, err
		},
	)
}

func (r *AgentOSWorksetRepo) worksetByIdempotencyKey(ctx context.Context, accountID, projectID, idempotencyKey string) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error) {
	query, args, err := r.Builder.
		Select("spec_json", "status_json").
		From("worksets").
		Where(sq.Eq{_colAccountID: accountID, _colProjectID: projectID, _colIDempotencyKey: idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, fmt.Errorf("AgentOSWorksetRepo - worksetByIdempotencyKey - builder: %w", err)
	}

	return r.scanWorkset(ctx, query, args, "AgentOSWorksetRepo - worksetByIdempotencyKey")
}

func (r *AgentOSWorksetRepo) worksetByID(ctx context.Context, worksetID string) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error) {
	query, args, err := r.Builder.Select("spec_json", "status_json").From("worksets").Where(sq.Eq{"workset_id": worksetID}).ToSql()
	if err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, fmt.Errorf("AgentOSWorksetRepo - worksetByID - builder: %w", err)
	}

	return r.scanWorkset(ctx, query, args, "AgentOSWorksetRepo - worksetByID")
}

func (r *AgentOSWorksetRepo) worksetStatusByIdempotencyKey(ctx context.Context, worksetID, accountID, projectID, idempotencyKey string) (agentos.WorksetStatus, bool, error) {
	query, args, err := r.Builder.
		Select("status_json").
		From("workset_status_updates").
		Where(sq.Eq{"workset_id": worksetID, _colAccountID: accountID, _colProjectID: projectID, _colIDempotencyKey: idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.WorksetStatus{}, false, fmt.Errorf("AgentOSWorksetRepo - worksetStatusByIdempotencyKey - builder: %w", err)
	}

	return r.scanWorksetStatus(ctx, query, args, "AgentOSWorksetRepo - worksetStatusByIdempotencyKey")
}

func (r *AgentOSWorksetRepo) worksetChunkStatusByKey(ctx context.Context, ref agentos.WorksetRef, chunkID, idempotencyKey string) (agentos.WorksetStatus, bool, error) {
	query, args, err := r.Builder.
		Select("status_json").
		From("workset_chunk_results").
		Where(sq.Eq{
			"workset_id":       ref.WorksetID,
			_colAccountID:      ref.AccountID,
			_colProjectID:      ref.ProjectID,
			"chunk_id":         chunkID,
			_colIDempotencyKey: idempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentos.WorksetStatus{}, false, fmt.Errorf("AgentOSWorksetRepo - worksetChunkStatusByKey - builder: %w", err)
	}

	return r.scanWorksetStatus(ctx, query, args, "AgentOSWorksetRepo - worksetChunkStatusByKey")
}

func (r *AgentOSWorksetRepo) scanWorkset(ctx context.Context, query string, args []any, name string) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error) {
	return scanSpecStatusJSON[agentos.WorksetSpec, agentos.WorksetStatus](r.Pool.QueryRow(ctx, query, args...), name)
}

func (r *AgentOSWorksetRepo) scanWorksetStatus(ctx context.Context, query string, args []any, name string) (agentos.WorksetStatus, bool, error) {
	status, exists, err := scanOptionalJSON[agentos.WorksetStatus](r.Pool.QueryRow(ctx, query, args...), name)
	if err != nil {
		return agentos.WorksetStatus{}, false, err
	}

	return status, exists, nil
}

func validatePostgresCreateWorkset(spec *agentos.WorksetSpec, status *agentos.WorksetStatus) error {
	if err := agentos.ValidateWorksetSpec(spec); err != nil {
		return err
	}

	if status == nil {
		return fmt.Errorf("%w: workset status is required", agentoscore.ErrInvalidWorkset)
	}

	return nil
}

func validatePostgresApplyChunkResult(ref agentos.WorksetRef, result *agentos.WorksetChunkResult, status *agentos.WorksetStatus) error {
	if err := agentos.ValidateWorksetRef(ref); err != nil {
		return err
	}

	if result == nil {
		return fmt.Errorf("%w: chunk result is required", agentoscore.ErrInvalidWorkset)
	}

	switch {
	case result.ChunkID == "":
		return fmt.Errorf("%w: chunk id is required", agentoscore.ErrInvalidWorkset)
	case result.IdempotencyKey == "":
		return fmt.Errorf("%w: chunk idempotency key is required", agentoscore.ErrInvalidWorkset)
	case result.CompletedItems < 0 || result.FailedItems < 0:
		return fmt.Errorf("%w: chunk item counts must be non-negative", agentoscore.ErrInvalidWorkset)
	default:
		return validatePostgresWorksetStatus(status, result.IdempotencyKey)
	}
}

func validatePostgresWorksetStatus(status *agentos.WorksetStatus, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: workset status is required", agentoscore.ErrInvalidWorkset)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: workset status idempotency key is required", agentoscore.ErrInvalidWorkset)
	}

	if status.WorksetID == "" || status.AccountID == "" || status.ProjectID == "" {
		return fmt.Errorf("%w: workset status scope is required", agentoscore.ErrInvalidWorkset)
	}

	return nil
}

func validatePostgresWorksetTenant(ref agentos.WorksetRef) func(agentos.WorksetSpec) error {
	return func(spec agentos.WorksetSpec) error {
		if spec.AccountID != ref.AccountID || spec.ProjectID != ref.ProjectID {
			return fmt.Errorf("%w: workset %q is outside tenant scope", agentoscore.ErrInvalidWorksetScope, ref.WorksetID)
		}

		return nil
	}
}

func normalizePostgresWorksetStatus(spec *agentos.WorksetSpec, status *agentos.WorksetStatus) agentos.WorksetStatus {
	normalized := *status
	normalized.WorksetID = spec.WorksetID
	normalized.AccountID = spec.AccountID
	normalized.ProjectID = spec.ProjectID
	normalized.ProcessID = spec.ProcessID
	normalized.Resource = spec.Resource
	normalized.Kind = spec.Kind

	return normalized
}

func worksetStatusListQuery(scope *agentos.WorksetScope) processPlatformStatusListQuery {
	return processPlatformStatusListQuery{
		Name:           "AgentOSWorksetRepo - ListWorksets",
		Table:          "worksets",
		OrderColumn:    "workset_id",
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		ProcessID:      scope.ProcessID,
		ResourceKind:   string(scope.Resource.Kind),
		ResourceID:     scope.Resource.ResourceID,
		Kind:           string(scope.Kind),
		LifecycleState: scope.LifecycleState,
		Limit:          scope.Limit,
	}
}

func worksetProjectionInsertRow(spec *agentos.WorksetSpec, status *agentos.WorksetStatus, specJSON, statusJSON []byte) processPlatformProjectionInsert {
	return buildProcessPlatformProjectionInsert(
		"AgentOSWorksetRepo - CreateWorkset",
		"worksets",
		"workset_id",
		spec.Resource,
		specJSON,
		statusJSON,
		&processPlatformProjectionValues{
			ID:             spec.WorksetID,
			AccountID:      spec.AccountID,
			ProjectID:      spec.ProjectID,
			ProcessID:      spec.ProcessID,
			Kind:           string(spec.Kind),
			LifecycleState: status.LifecycleState,
			IdempotencyKey: spec.IdempotencyKey,
			RequestedAt:    spec.RequestedAt,
			UpdatedAt:      status.UpdatedAt,
		},
	)
}

var _ agentosbatch.Store = (*AgentOSWorksetRepo)(nil)
