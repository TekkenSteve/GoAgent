package persistent

import (
	"context"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	agentosbatch "github.com/TekkenSteve/GoAgent/internal/usecase/agentosbatch"
	"github.com/jackc/pgx/v5"
)

// worksetTable is the worksets half of the platform projection protocol: every
// method wraps one generated statement, and nothing else. Marshaling,
// idempotent replay, conflict resolution and transaction handling live in the
// shared protocols, so this adapter is the only place the workset SQL surface
// appears.
type worksetTable struct {
	queries *sqlcgen.Queries
}

func (t worksetTable) withTx(tx pgx.Tx) platformTable[agentos.WorksetSpec, agentos.WorksetStatus] {
	return t.txTable(tx)
}

// txTable is withTx for callers that need workset-specific statements (the
// chunk-result key) alongside the shared protocol surface.
func (t worksetTable) txTable(tx pgx.Tx) worksetTable {
	return worksetTable{queries: t.queries.WithTx(tx)}
}

func (t worksetTable) selectRecordByKey(ctx context.Context, scope platformScope) (specJSON, statusJSON []byte, queryErr error) {
	row, err := t.queries.GetWorksetByIdempotencyKey(ctx, sqlcgen.GetWorksetByIdempotencyKeyParams{
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: scope.IdempotencyKey,
	})
	if err != nil {
		return nil, nil, err
	}

	return row.SpecJson, row.StatusJson, nil
}

func (t worksetTable) selectRecordByID(ctx context.Context, scope platformScope) (specJSON, statusJSON []byte, queryErr error) {
	row, err := t.queries.GetWorksetByID(ctx, scope.RecordID)
	if err != nil {
		return nil, nil, err
	}

	return row.SpecJson, row.StatusJson, nil
}

func (t worksetTable) insertRecord(ctx context.Context, spec *agentos.WorksetSpec, status *agentos.WorksetStatus, specJSON, statusJSON []byte) ([]byte, error) {
	return t.queries.InsertWorkset(ctx, worksetInsertParams(spec, status, specJSON, statusJSON))
}

// worksetInsertParams builds the stored row's columns from the record's
// identity, its denormalized filter columns and the two documents. The
// governed-action table carries the same column set, so its binding is
// structurally identical: sqlc generates one parameter struct per statement,
// and Go cannot map two generated structs through one builder. The duplication
// is the price of the tables being separate; it is not shared logic.
//
//nolint:dupl // one parameter binding per platform table, mirrored by actionInsertParams
func worksetInsertParams(spec *agentos.WorksetSpec, status *agentos.WorksetStatus, specJSON, statusJSON []byte) sqlcgen.InsertWorksetParams {
	return sqlcgen.InsertWorksetParams{
		WorksetID:      spec.WorksetID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		ProcessID:      spec.ProcessID,
		ResourceKind:   string(spec.Resource.Kind),
		ResourceID:     spec.Resource.ResourceID,
		Kind:           string(spec.Kind),
		LifecycleState: status.LifecycleState,
		IdempotencyKey: spec.IdempotencyKey,
		SpecJson:       specJSON,
		StatusJson:     statusJSON,
		RequestedAt:    optionalTimestamptz(spec.RequestedAt),
		UpdatedAt:      status.UpdatedAt,
	}
}

func (t worksetTable) updateRecordStatus(ctx context.Context, scope platformScope, status *agentos.WorksetStatus, statusJSON []byte) (int64, error) {
	return t.queries.UpdateWorksetProjection(ctx, sqlcgen.UpdateWorksetProjectionParams{
		LifecycleState: status.LifecycleState,
		StatusJson:     statusJSON,
		UpdatedAt:      status.UpdatedAt,
		WorksetID:      scope.RecordID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
	})
}

func (t worksetTable) claimStatusKey(ctx context.Context, scope platformScope, statusJSON []byte) ([]byte, error) {
	return t.queries.ClaimWorksetStatusUpdate(ctx, sqlcgen.ClaimWorksetStatusUpdateParams{
		WorksetID:      scope.RecordID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: scope.IdempotencyKey,
		StatusJson:     statusJSON,
	})
}

func (t worksetTable) readStatusKey(ctx context.Context, scope platformScope) (storedJSON []byte, queryErr error) {
	return t.queries.GetWorksetStatusUpdate(ctx, sqlcgen.GetWorksetStatusUpdateParams{
		WorksetID:      scope.RecordID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: scope.IdempotencyKey,
	})
}

func (t worksetTable) recordScope(spec *agentos.WorksetSpec) platformScope {
	return platformScope{
		RecordID:       spec.WorksetID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func (t worksetTable) statusScope(status *agentos.WorksetStatus, idempotencyKey string) platformScope {
	return platformScope{
		RecordID:       status.WorksetID,
		AccountID:      status.AccountID,
		ProjectID:      status.ProjectID,
		IdempotencyKey: idempotencyKey,
	}
}

func (t worksetTable) normalizeStatus(spec *agentos.WorksetSpec, status *agentos.WorksetStatus) *agentos.WorksetStatus {
	normalized := normalizePostgresWorksetStatus(spec, status)

	return &normalized
}

func (t worksetTable) notFound(scope platformScope) error {
	return fmt.Errorf("%w: workset %q not found", agentoscore.ErrInvalidWorksetScope, scope.RecordID)
}

func (t worksetTable) alreadyExists(scope platformScope) error {
	return fmt.Errorf("%w: workset %q already exists", agentoscore.ErrInvalidWorkset, scope.RecordID)
}

func (t worksetTable) tenantMismatch(spec *agentos.WorksetSpec, scope platformScope) error {
	return platformTenantMismatch(agentoscore.ErrInvalidWorksetScope, "workset", "is outside tenant scope", spec.AccountID, spec.ProjectID, scope)
}

func (t worksetTable) statusTenantMismatch(spec *agentos.WorksetSpec, scope platformScope) error {
	return platformTenantMismatch(agentoscore.ErrInvalidWorksetScope, "workset", "status is outside tenant scope", spec.AccountID, spec.ProjectID, scope)
}

// claimChunkResult and readChunkResult are the chunk-result write's own key
// table: its claim carries a result document as well as the status, so it sits
// outside the shared status-key protocol.
func (t worksetTable) claimChunkResult(ctx context.Context, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, resultJSON, statusJSON []byte) ([]byte, error) {
	return t.queries.ClaimWorksetChunkResult(ctx, sqlcgen.ClaimWorksetChunkResultParams{
		WorksetID:      ref.WorksetID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		ChunkID:        result.ChunkID,
		IdempotencyKey: result.IdempotencyKey,
		ResultJson:     resultJSON,
		StatusJson:     statusJSON,
	})
}

func (t worksetTable) readChunkResult(ctx context.Context, ref agentos.WorksetRef, chunkID, idempotencyKey string) ([]byte, error) {
	return t.queries.GetWorksetChunkStatus(ctx, sqlcgen.GetWorksetChunkStatusParams{
		WorksetID:      ref.WorksetID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		ChunkID:        chunkID,
		IdempotencyKey: idempotencyKey,
	})
}

// AgentOSWorksetRepo persists generic AgentOS workset projections.
type AgentOSWorksetRepo struct {
	*postgres.Postgres

	table worksetTable
}

// NewAgentOSWorksetRepo creates a Postgres-backed workset repository.
func NewAgentOSWorksetRepo(pg *postgres.Postgres) *AgentOSWorksetRepo {
	return &AgentOSWorksetRepo{Postgres: pg, table: worksetTable{queries: sqlcgen.New(pg.Pool)}}
}

// CreateWorkset persists a new workset from its spec and status, returning the stored status and whether the workset was newly created.
func (r *AgentOSWorksetRepo) CreateWorkset(ctx context.Context, spec *agentos.WorksetSpec, status *agentos.WorksetStatus) (agentos.WorksetStatus, bool, error) {
	if err := validatePostgresCreateWorkset(spec, status); err != nil {
		return agentos.WorksetStatus{}, false, err
	}

	return createPlatformRecord(ctx, r.table, "AgentOSWorksetRepo - CreateWorkset", spec, status, agentosbatch.ValidateWorksetStartIdempotency)
}

// GetWorkset loads a workset spec and status by tenant-scoped reference.
func (r *AgentOSWorksetRepo) GetWorkset(ctx context.Context, ref agentos.WorksetRef) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error) {
	if err := agentos.ValidateWorksetRef(ref); err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, err
	}

	scope := platformScope{RecordID: ref.WorksetID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}

	return getPlatformRecord[agentos.WorksetSpec, agentos.WorksetStatus](ctx, r.table, "AgentOSWorksetRepo - GetWorkset", scope)
}

// ListWorksets returns workset statuses matching the given scope filters.
func (r *AgentOSWorksetRepo) ListWorksets(ctx context.Context, scope *agentos.WorksetScope) ([]agentos.WorksetStatus, error) {
	if err := agentos.ValidateWorksetScope(scope); err != nil {
		return nil, err
	}

	rows, err := r.table.queries.ListWorksets(ctx, worksetListParams(scope))

	return listRecords("AgentOSWorksetRepo - ListWorksets", rows, err, func(row *[]byte) (agentos.WorksetStatus, error) {
		return decodeProcessPlatformJSON[agentos.WorksetStatus]("AgentOSWorksetRepo - ListWorksets", *row)
	})
}

// ApplyChunkResult idempotently applies a chunk result and returns the resulting workset status.
func (r *AgentOSWorksetRepo) ApplyChunkResult(ctx context.Context, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, status *agentos.WorksetStatus) (agentos.WorksetStatus, error) {
	if err := validatePostgresApplyChunkResult(ref, result, status); err != nil {
		return agentos.WorksetStatus{}, err
	}

	spec, _, exists, err := selectPlatformRecord[agentos.WorksetSpec, agentos.WorksetStatus](ctx,
		"AgentOSWorksetRepo - ApplyChunkResult", r.table.selectRecordByID, platformScope{RecordID: ref.WorksetID})
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !exists {
		return agentos.WorksetStatus{}, r.table.notFound(platformScope{RecordID: ref.WorksetID})
	}

	if err := r.table.tenantMismatch(&spec, platformScope{RecordID: ref.WorksetID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}); err != nil {
		return agentos.WorksetStatus{}, err
	}

	normalized := normalizePostgresWorksetStatus(&spec, status)

	return r.applyChunkResult(ctx, ref, result, &normalized)
}

// UpdateWorksetStatus applies a workset status update idempotently and returns the resulting status.
func (r *AgentOSWorksetRepo) UpdateWorksetStatus(ctx context.Context, status *agentos.WorksetStatus, idempotencyKey string) (agentos.WorksetStatus, error) {
	if err := validatePostgresWorksetStatus(status, idempotencyKey); err != nil {
		return agentos.WorksetStatus{}, err
	}

	return updatePlatformStatus(ctx, r.Postgres, r.table, "AgentOSWorksetRepo - UpdateWorksetStatus", status, idempotencyKey)
}

// applyChunkResult runs the chunk-result claim and its projection write in one
// transaction: the claim carries the result document, which is why it drives
// the general claim protocol rather than the status-key one.
func (r *AgentOSWorksetRepo) applyChunkResult(ctx context.Context, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, status *agentos.WorksetStatus) (agentos.WorksetStatus, error) {
	name := "AgentOSWorksetRepo - ApplyChunkResult"

	statusJSON, err := marshalProcessPlatformJSON(name+" status", status)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	resultJSON, err := marshalProcessPlatformJSON(name+" result", result)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	scope := platformScope{
		RecordID:       ref.WorksetID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: result.IdempotencyKey,
	}

	return applyPlatformClaim(ctx, r.Postgres, name,
		func(ctx context.Context, tx pgx.Tx) (bool, agentos.WorksetStatus, error) {
			table := r.table.txTable(tx)

			return claimPlatformDocument[agentos.WorksetStatus](ctx, name,
				func(ctx context.Context) ([]byte, error) {
					return table.claimChunkResult(ctx, ref, result, resultJSON, statusJSON)
				},
				func(ctx context.Context) ([]byte, error) {
					return table.readChunkResult(ctx, ref, result.ChunkID, result.IdempotencyKey)
				},
			)
		},
		func(ctx context.Context, tx pgx.Tx) error {
			_, err := r.table.withTx(tx).updateRecordStatus(ctx, scope, status, statusJSON)

			return err
		},
	)
}

// worksetListParams turns a scope into the generated list bindings: every
// optional filter is empty-string-means-absent, and a non-positive limit
// binds NULL, which LIMIT reads as ALL.
func worksetListParams(scope *agentos.WorksetScope) sqlcgen.ListWorksetsParams {
	return sqlcgen.ListWorksetsParams{
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		ProcessID:      scope.ProcessID,
		ResourceKind:   string(scope.Resource.Kind),
		ResourceID:     scope.Resource.ResourceID,
		Kind:           string(scope.Kind),
		LifecycleState: scope.LifecycleState,
		RowLimit:       optionalInt8(scope.Limit),
	}
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

var _ agentosbatch.Store = (*AgentOSWorksetRepo)(nil)
