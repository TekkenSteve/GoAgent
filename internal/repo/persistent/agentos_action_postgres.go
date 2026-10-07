package persistent

import (
	"context"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosaction"
	"github.com/jackc/pgx/v5"
)

// actionTable is the governed-actions half of the platform projection protocol:
// every method wraps one generated statement, and nothing else. Marshaling,
// idempotent replay, conflict resolution and transaction handling live in the
// shared protocols, so this adapter is the only place the action SQL surface
// appears.
type actionTable struct {
	queries *sqlcgen.Queries
}

func (t actionTable) withTx(tx pgx.Tx) platformTable[agentos.GovernedActionSpec, agentos.GovernedActionStatus] {
	return actionTable{queries: t.queries.WithTx(tx)}
}

func (t actionTable) selectRecordByKey(ctx context.Context, scope platformScope) (specJSON, statusJSON []byte, queryErr error) {
	row, err := t.queries.GetGovernedActionByIdempotencyKey(ctx, sqlcgen.GetGovernedActionByIdempotencyKeyParams{
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: scope.IdempotencyKey,
	})
	if err != nil {
		return nil, nil, err
	}

	return row.SpecJson, row.StatusJson, nil
}

func (t actionTable) selectRecordByID(ctx context.Context, scope platformScope) (specJSON, statusJSON []byte, queryErr error) {
	row, err := t.queries.GetGovernedActionByID(ctx, scope.RecordID)
	if err != nil {
		return nil, nil, err
	}

	return row.SpecJson, row.StatusJson, nil
}

func (t actionTable) insertRecord(ctx context.Context, spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus, specJSON, statusJSON []byte) ([]byte, error) {
	return t.queries.InsertGovernedAction(ctx, actionInsertParams(spec, status, specJSON, statusJSON))
}

// actionInsertParams builds the stored row's columns from the record's
// identity, its denormalized filter columns and the two documents. The workset
// table carries the same column set, so its binding is structurally identical:
// sqlc generates one parameter struct per statement, and Go cannot map two
// generated structs through one builder. The duplication is the price of the
// tables being separate; it is not shared logic.
//
//nolint:dupl // one parameter binding per platform table, mirrored by worksetInsertParams
func actionInsertParams(spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus, specJSON, statusJSON []byte) sqlcgen.InsertGovernedActionParams {
	return sqlcgen.InsertGovernedActionParams{
		ActionID:       spec.ActionID,
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

func (t actionTable) updateRecordStatus(ctx context.Context, scope platformScope, status *agentos.GovernedActionStatus, statusJSON []byte) (int64, error) {
	return t.queries.UpdateGovernedActionProjection(ctx, sqlcgen.UpdateGovernedActionProjectionParams{
		LifecycleState: status.LifecycleState,
		StatusJson:     statusJSON,
		UpdatedAt:      status.UpdatedAt,
		ActionID:       scope.RecordID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
	})
}

func (t actionTable) claimStatusKey(ctx context.Context, scope platformScope, statusJSON []byte) ([]byte, error) {
	return t.queries.ClaimGovernedActionStatusUpdate(ctx, sqlcgen.ClaimGovernedActionStatusUpdateParams{
		ActionID:       scope.RecordID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: scope.IdempotencyKey,
		StatusJson:     statusJSON,
	})
}

func (t actionTable) readStatusKey(ctx context.Context, scope platformScope) (storedJSON []byte, queryErr error) {
	return t.queries.GetGovernedActionStatusUpdate(ctx, sqlcgen.GetGovernedActionStatusUpdateParams{
		ActionID:       scope.RecordID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: scope.IdempotencyKey,
	})
}

func (t actionTable) recordScope(spec *agentos.GovernedActionSpec) platformScope {
	return platformScope{
		RecordID:       spec.ActionID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func (t actionTable) statusScope(status *agentos.GovernedActionStatus, idempotencyKey string) platformScope {
	return platformScope{
		RecordID:       status.ActionID,
		AccountID:      status.AccountID,
		ProjectID:      status.ProjectID,
		IdempotencyKey: idempotencyKey,
	}
}

func (t actionTable) normalizeStatus(spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) *agentos.GovernedActionStatus {
	normalized := normalizePostgresActionStatus(spec, status)

	return &normalized
}

func (t actionTable) notFound(scope platformScope) error {
	return fmt.Errorf("%w: action %q not found", agentoscore.ErrInvalidGovernedActionScope, scope.RecordID)
}

func (t actionTable) alreadyExists(scope platformScope) error {
	return fmt.Errorf("%w: action %q already exists", agentoscore.ErrInvalidGovernedAction, scope.RecordID)
}

func (t actionTable) tenantMismatch(spec *agentos.GovernedActionSpec, scope platformScope) error {
	return platformTenantMismatch(agentoscore.ErrInvalidGovernedActionScope, "action", "is outside tenant scope", spec.AccountID, spec.ProjectID, scope)
}

func (t actionTable) statusTenantMismatch(spec *agentos.GovernedActionSpec, scope platformScope) error {
	return platformTenantMismatch(agentoscore.ErrInvalidGovernedActionScope, "action", "status is outside tenant scope", spec.AccountID, spec.ProjectID, scope)
}

// AgentOSActionRepo persists governed action projections.
type AgentOSActionRepo struct {
	*postgres.Postgres

	table actionTable
}

// NewAgentOSActionRepo creates a Postgres-backed governed action repository.
func NewAgentOSActionRepo(pg *postgres.Postgres) *AgentOSActionRepo {
	return &AgentOSActionRepo{Postgres: pg, table: actionTable{queries: sqlcgen.New(pg.Pool)}}
}

// CreateAction validates and persists a governed action spec with its initial status, returning the stored status and whether the action was newly created.
func (r *AgentOSActionRepo) CreateAction(ctx context.Context, spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) (agentos.GovernedActionStatus, bool, error) {
	if err := agentos.ValidateGovernedActionSpec(spec); err != nil {
		return agentos.GovernedActionStatus{}, false, err
	}

	if status == nil {
		return agentos.GovernedActionStatus{}, false, fmt.Errorf("%w: action status is required", agentoscore.ErrInvalidGovernedAction)
	}

	return createPlatformRecord(ctx, r.table, "AgentOSActionRepo - CreateAction", spec, status, agentosaction.ValidateActionStartIdempotency)
}

// GetAction loads a governed action by reference, returning its spec, status, and whether it exists.
func (r *AgentOSActionRepo) GetAction(ctx context.Context, ref agentos.ActionRef) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error) {
	if err := agentos.ValidateActionRef(ref); err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, err
	}

	scope := platformScope{RecordID: ref.ActionID, AccountID: ref.AccountID, ProjectID: ref.ProjectID}

	return getPlatformRecord[agentos.GovernedActionSpec, agentos.GovernedActionStatus](ctx, r.table, "AgentOSActionRepo - GetAction", scope)
}

// ListActions returns governed action statuses matching the given scope filters.
func (r *AgentOSActionRepo) ListActions(ctx context.Context, scope *agentos.ActionScope) ([]agentos.GovernedActionStatus, error) {
	if err := agentos.ValidateActionScope(scope); err != nil {
		return nil, err
	}

	rows, err := r.table.queries.ListGovernedActions(ctx, actionListParams(scope))

	return listRecords("AgentOSActionRepo - ListActions", rows, err, func(row *[]byte) (agentos.GovernedActionStatus, error) {
		return decodeProcessPlatformJSON[agentos.GovernedActionStatus]("AgentOSActionRepo - ListActions", *row)
	})
}

// UpdateActionStatus applies a governed action status update idempotently and returns the resulting status.
func (r *AgentOSActionRepo) UpdateActionStatus(ctx context.Context, status *agentos.GovernedActionStatus, idempotencyKey string) (agentos.GovernedActionStatus, error) {
	if err := validatePostgresActionStatus(status, idempotencyKey); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	return updatePlatformStatus(ctx, r.Postgres, r.table, "AgentOSActionRepo - UpdateActionStatus", status, idempotencyKey)
}

func validatePostgresActionStatus(status *agentos.GovernedActionStatus, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: action status is required", agentoscore.ErrInvalidGovernedAction)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: action status idempotency key is required", agentoscore.ErrInvalidGovernedAction)
	}

	if status.ActionID == "" || status.AccountID == "" || status.ProjectID == "" {
		return fmt.Errorf("%w: action status scope is required", agentoscore.ErrInvalidGovernedAction)
	}

	return nil
}

// actionListParams turns a scope into the generated list bindings: every
// optional filter is empty-string-means-absent, and a non-positive limit
// binds NULL, which LIMIT reads as ALL.
func actionListParams(scope *agentos.ActionScope) sqlcgen.ListGovernedActionsParams {
	return sqlcgen.ListGovernedActionsParams{
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

func normalizePostgresActionStatus(spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) agentos.GovernedActionStatus {
	normalized := *status
	normalized.ActionID = spec.ActionID
	normalized.AccountID = spec.AccountID
	normalized.ProjectID = spec.ProjectID
	normalized.ProcessID = spec.ProcessID
	normalized.Resource = spec.Resource
	normalized.Kind = spec.Kind

	return normalized
}

var _ agentosaction.Store = (*AgentOSActionRepo)(nil)
