package persistent

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosaction"
	"github.com/jackc/pgx/v5"
)

// AgentOSActionRepo persists governed action specs and lifecycle projections.
type AgentOSActionRepo struct {
	*postgres.Postgres
}

// NewAgentOSActionRepo creates a Postgres-backed governed action store.
func NewAgentOSActionRepo(pg *postgres.Postgres) *AgentOSActionRepo {
	return &AgentOSActionRepo{Postgres: pg}
}

// CreateAction validates and persists a governed action spec with its initial status, returning the stored status and whether the action was newly created.
func (r *AgentOSActionRepo) CreateAction(ctx context.Context, spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) (agentos.GovernedActionStatus, bool, error) {
	if err := agentos.ValidateGovernedActionSpec(spec); err != nil {
		return agentos.GovernedActionStatus{}, false, err
	}

	if status == nil {
		return agentos.GovernedActionStatus{}, false, fmt.Errorf("%w: action status is required", agentoscore.ErrInvalidGovernedAction)
	}

	existingSpec, existingStatus, exists, err := r.actionByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil {
		return agentos.GovernedActionStatus{}, false, err
	}

	if exists {
		return existingStatus, false, agentosaction.ValidateActionStartIdempotency(&existingSpec, spec)
	}

	existingSpec, existingStatus, exists, err = r.actionByID(ctx, spec.ActionID)
	if err != nil {
		return agentos.GovernedActionStatus{}, false, err
	}

	if exists {
		return existingStatus, false, agentosaction.ValidateActionStartIdempotency(&existingSpec, spec)
	}

	return r.insertAction(ctx, spec, status)
}

// GetAction loads a governed action by reference, returning its spec, status, and whether it exists.
func (r *AgentOSActionRepo) GetAction(ctx context.Context, ref agentos.ActionRef) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error) {
	if err := agentos.ValidateActionRef(ref); err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, err
	}

	return getProcessPlatformProjection[agentos.GovernedActionSpec, agentos.GovernedActionStatus](
		ctx,
		processPlatformTenantRef{ID: ref.ActionID, AccountID: ref.AccountID, ProjectID: ref.ProjectID},
		r.actionByID,
		validatePostgresActionTenant(ref),
	)
}

// ListActions returns governed action statuses matching the given scope filters.
func (r *AgentOSActionRepo) ListActions(ctx context.Context, scope *agentos.ActionScope) ([]agentos.GovernedActionStatus, error) {
	if err := agentos.ValidateActionScope(scope); err != nil {
		return nil, err
	}

	query := actionStatusListQuery(scope)

	return listProcessPlatformStatuses[agentos.GovernedActionStatus](ctx, r.Postgres, &query)
}

// UpdateActionStatus applies a governed action status update idempotently and returns the resulting status.
func (r *AgentOSActionRepo) UpdateActionStatus(ctx context.Context, status *agentos.GovernedActionStatus, idempotencyKey string) (agentos.GovernedActionStatus, error) {
	if err := validatePostgresActionStatus(status, idempotencyKey); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	existing, exists, err := r.actionStatusByIdempotencyKey(ctx, status.ActionID, status.AccountID, status.ProjectID, idempotencyKey)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if exists {
		return existing, nil
	}

	spec, _, exists, err := r.actionByID(ctx, status.ActionID)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if !exists {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q not found", agentoscore.ErrInvalidGovernedActionScope, status.ActionID)
	}

	if spec.AccountID != status.AccountID || spec.ProjectID != status.ProjectID {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q status is outside tenant scope", agentoscore.ErrInvalidGovernedActionScope, status.ActionID)
	}

	normalized := *status
	normalized.ProcessID = spec.ProcessID
	normalized.Resource = spec.Resource
	normalized.Kind = spec.Kind

	return r.applyActionStatus(ctx, &normalized, idempotencyKey)
}

func (r *AgentOSActionRepo) insertAction(ctx context.Context, spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) (agentos.GovernedActionStatus, bool, error) {
	return createProcessPlatformProjection(ctx, r.Postgres, spec, status, processPlatformProjectionCreateConfig[agentos.GovernedActionSpec, agentos.GovernedActionStatus]{
		SpecMarshalName:   "AgentOSActionRepo - CreateAction spec",
		StatusMarshalName: "AgentOSActionRepo - CreateAction status",
		Normalize:         normalizePostgresActionStatus,
		BuildRow:          actionProjectionInsertRow,
		ResolveInsertErr:  r.resolveActionInsertErr,
	})
}

func (r *AgentOSActionRepo) applyActionStatus(ctx context.Context, status *agentos.GovernedActionStatus, idempotencyKey string) (agentos.GovernedActionStatus, error) {
	statusJSON, err := marshalProcessPlatformJSON("AgentOSActionRepo - UpdateActionStatus status", status)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.GovernedActionStatus{}, fmt.Errorf("AgentOSActionRepo - UpdateActionStatus - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	claimed, existing, err := claimActionStatusUpdate(ctx, tx, status, idempotencyKey, statusJSON)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if !claimed {
		return existing, nil
	}

	tag, err := tx.Exec(
		ctx, `
UPDATE governed_actions
SET lifecycle_state = $1, status_json = $2, updated_at = $3
WHERE action_id = $4 AND account_id = $5 AND project_id = $6`,
		status.LifecycleState,
		statusJSON,
		status.UpdatedAt,
		status.ActionID,
		status.AccountID,
		status.ProjectID,
	)
	if err != nil {
		return agentos.GovernedActionStatus{}, fmt.Errorf("AgentOSActionRepo - UpdateActionStatus - update: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q not found", agentoscore.ErrInvalidGovernedActionScope, status.ActionID)
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.GovernedActionStatus{}, fmt.Errorf("AgentOSActionRepo - UpdateActionStatus - commit: %w", err)
	}

	return *status, nil
}

func (r *AgentOSActionRepo) resolveActionInsertErr(ctx context.Context, insertErr error, spec *agentos.GovernedActionSpec) (agentos.GovernedActionStatus, error) {
	return resolveProcessPlatformProjectionInsertErr(
		ctx,
		insertErr,
		spec.ActionID,
		agentoscore.ErrInvalidGovernedAction,
		"AgentOSActionRepo - CreateAction - insert",
		"action",
		func(ctx context.Context) (agentos.GovernedActionStatus, bool, error) {
			_, existing, exists, err := r.actionByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)

			return existing, exists, err
		},
	)
}

func (r *AgentOSActionRepo) actionByIdempotencyKey(ctx context.Context, accountID, projectID, idempotencyKey string) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error) {
	query, args, err := r.Builder.
		Select("spec_json", "status_json").
		From("governed_actions").
		Where(sq.Eq{_colAccountID: accountID, _colProjectID: projectID, _colIDempotencyKey: idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, fmt.Errorf("AgentOSActionRepo - actionByIdempotencyKey - builder: %w", err)
	}

	return r.scanAction(ctx, query, args, "AgentOSActionRepo - actionByIdempotencyKey")
}

func (r *AgentOSActionRepo) actionByID(ctx context.Context, actionID string) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error) {
	query, args, err := r.Builder.Select("spec_json", "status_json").From("governed_actions").Where(sq.Eq{"action_id": actionID}).ToSql()
	if err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, fmt.Errorf("AgentOSActionRepo - actionByID - builder: %w", err)
	}

	return r.scanAction(ctx, query, args, "AgentOSActionRepo - actionByID")
}

func (r *AgentOSActionRepo) actionStatusByIdempotencyKey(ctx context.Context, actionID, accountID, projectID, idempotencyKey string) (agentos.GovernedActionStatus, bool, error) {
	query, args, err := r.Builder.
		Select("status_json").
		From("governed_action_status_updates").
		Where(sq.Eq{"action_id": actionID, _colAccountID: accountID, _colProjectID: projectID, _colIDempotencyKey: idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.GovernedActionStatus{}, false, fmt.Errorf("AgentOSActionRepo - actionStatusByIdempotencyKey - builder: %w", err)
	}

	status, exists, err := scanOptionalJSON[agentos.GovernedActionStatus](r.Pool.QueryRow(ctx, query, args...), "AgentOSActionRepo - actionStatusByIdempotencyKey")
	if err != nil {
		return agentos.GovernedActionStatus{}, false, err
	}

	return status, exists, nil
}

func (r *AgentOSActionRepo) scanAction(ctx context.Context, query string, args []any, name string) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error) {
	return scanSpecStatusJSON[agentos.GovernedActionSpec, agentos.GovernedActionStatus](r.Pool.QueryRow(ctx, query, args...), name)
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

func validatePostgresActionTenant(ref agentos.ActionRef) func(agentos.GovernedActionSpec) error {
	return func(spec agentos.GovernedActionSpec) error {
		if spec.AccountID != ref.AccountID || spec.ProjectID != ref.ProjectID {
			return fmt.Errorf("%w: action %q is outside tenant scope", agentoscore.ErrInvalidGovernedActionScope, ref.ActionID)
		}

		return nil
	}
}

func claimActionStatusUpdate(ctx context.Context, tx pgx.Tx, status *agentos.GovernedActionStatus, idempotencyKey string, statusJSON []byte) (bool, agentos.GovernedActionStatus, error) {
	return claimProcessPlatformStatus[agentos.GovernedActionStatus](ctx, tx, &processPlatformStatusClaim{
		ClaimName:       "AgentOSActionRepo - claimActionStatusUpdate",
		ExistingName:    "AgentOSActionRepo - existingActionStatusUpdate",
		InsertErrPrefix: "AgentOSActionRepo - UpdateActionStatus - insert status key",
		SelectErrPrefix: "AgentOSActionRepo - UpdateActionStatus - existing status key",
		InsertSQL: `
INSERT INTO governed_action_status_updates (action_id, account_id, project_id, idempotency_key, status_json)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (account_id, project_id, action_id, idempotency_key) DO NOTHING
RETURNING status_json`,
		InsertArgs: []any{status.ActionID, status.AccountID, status.ProjectID, idempotencyKey, statusJSON},
		SelectSQL: `
SELECT status_json
FROM governed_action_status_updates
WHERE action_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4`,
		SelectArgs: []any{status.ActionID, status.AccountID, status.ProjectID, idempotencyKey},
	})
}

func actionStatusListQuery(scope *agentos.ActionScope) processPlatformStatusListQuery {
	return processPlatformStatusListQuery{
		Name:           "AgentOSActionRepo - ListActions",
		Table:          "governed_actions",
		OrderColumn:    "action_id",
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

func actionProjectionInsertRow(spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus, specJSON, statusJSON []byte) processPlatformProjectionInsert {
	return buildProcessPlatformProjectionInsert(
		"AgentOSActionRepo - CreateAction",
		"governed_actions",
		"action_id",
		spec.Resource,
		specJSON,
		statusJSON,
		&processPlatformProjectionValues{
			ID:             spec.ActionID,
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
