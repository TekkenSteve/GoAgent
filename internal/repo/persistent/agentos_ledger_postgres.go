package persistent

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosledger"
	"github.com/jackc/pgx/v5"
)

// AgentOSLedgerRepo persists append-only generic AgentOS ledger entries.
type AgentOSLedgerRepo struct {
	*postgres.Postgres
}

// NewAgentOSLedgerRepo creates a Postgres-backed ledger store.
func NewAgentOSLedgerRepo(pg *postgres.Postgres) *AgentOSLedgerRepo {
	return &AgentOSLedgerRepo{Postgres: pg}
}

// AppendLedgerEntry validates and appends a new ledger entry with a tenant-scoped sequence number, returning the stored entry.
func (r *AgentOSLedgerRepo) AppendLedgerEntry(ctx context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error) {
	if err := agentos.ValidateLedgerEntrySpec(spec); err != nil {
		return agentos.LedgerEntry{}, err
	}

	existing, exists, err := r.ledgerEntryByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil {
		return agentos.LedgerEntry{}, err
	}

	if exists {
		requested := agentos.LedgerEntry{LedgerEntrySpec: *spec, Sequence: existing.Sequence, CreatedAt: existing.CreatedAt}
		if requested.OccurredAt.IsZero() {
			requested.OccurredAt = existing.CreatedAt
		}

		if err := agentosledger.ValidateLedgerEntryIdempotency(&existing, &requested); err != nil {
			return agentos.LedgerEntry{}, err
		}

		return existing, nil
	}

	if existing, exists, err = r.ledgerEntryByID(ctx, spec.EntryID); err != nil || exists {
		if err != nil {
			return agentos.LedgerEntry{}, err
		}

		return agentos.LedgerEntry{}, fmt.Errorf("%w: ledger entry %q already exists with idempotency key %q", agentoscore.ErrInvalidLedgerEntry, existing.EntryID, existing.IdempotencyKey)
	}

	return r.insertLedgerEntry(ctx, spec)
}

// ListLedgerEntries returns ledger entries matching the given scope, ordered by sequence ascending.
func (r *AgentOSLedgerRepo) ListLedgerEntries(ctx context.Context, scope *agentos.LedgerScope) ([]agentos.LedgerEntry, error) {
	if err := agentos.ValidateLedgerScope(scope); err != nil {
		return nil, err
	}

	builder := r.Builder.
		Select("entry_json").
		From("ledger_entries").
		Where(sq.Eq{"account_id": scope.AccountID, "project_id": scope.ProjectID}).
		Where(sq.Gt{"sequence": scope.AfterSequence}).
		OrderBy("sequence ASC")

	if scope.ProcessID != "" {
		builder = builder.Where(sq.Eq{"process_id": scope.ProcessID})
	}

	if scope.Resource.Kind != "" {
		builder = builder.Where(sq.Eq{"resource_kind": string(scope.Resource.Kind), "resource_id": scope.Resource.ResourceID})
	}

	if scope.Kind != "" {
		builder = builder.Where(sq.Eq{"kind": string(scope.Kind)})
	}

	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSLedgerRepo - ListLedgerEntries - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSLedgerRepo - ListLedgerEntries - query: %w", err)
	}
	defer rows.Close()

	return scanJSONRows[agentos.LedgerEntry](rows, "AgentOSLedgerRepo - ListLedgerEntries")
}

func (r *AgentOSLedgerRepo) insertLedgerEntry(ctx context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error) {
	resourceKind, resourceID := processPlatformResourceColumns(spec.Resource)

	entryJSON, err := marshalProcessPlatformJSON("AgentOSLedgerRepo - AppendLedgerEntry entry", spec)
	if err != nil {
		return agentos.LedgerEntry{}, err
	}

	row := r.Pool.QueryRow(
		ctx, `
WITH tenant_lock AS (
    SELECT pg_advisory_xact_lock(hashtextextended($2 || ':' || $3, 0))
),
next_sequence AS (
    SELECT COALESCE(MAX(ledger_entries.sequence), 0) + 1 AS sequence
    FROM tenant_lock
    LEFT JOIN ledger_entries
        ON ledger_entries.account_id = $2 AND ledger_entries.project_id = $3
)
INSERT INTO ledger_entries (
    entry_id,
    account_id,
    project_id,
    process_id,
    resource_kind,
    resource_id,
    kind,
    sequence,
    idempotency_key,
    entry_json,
    occurred_at
) SELECT $1,$2,$3,$4,$5,$6,$7,next_sequence.sequence,$8,
         jsonb_set(
             jsonb_set(
                 jsonb_set($9::jsonb, '{sequence}', to_jsonb(next_sequence.sequence), true),
                 '{created_at}', to_jsonb(COALESCE($10::timestamptz, NOW())), true
             ),
             '{occurred_at}', to_jsonb(COALESCE($10::timestamptz, NOW())), true
         ),
         $10
FROM next_sequence
RETURNING entry_json`,
		spec.EntryID,
		spec.AccountID,
		spec.ProjectID,
		spec.ProcessID,
		resourceKind,
		resourceID,
		string(spec.Kind),
		spec.IdempotencyKey,
		entryJSON,
		processPlatformNullableTime(spec.OccurredAt),
	)

	inserted, err := scanSingleJSON[agentos.LedgerEntry](row, "AgentOSLedgerRepo - AppendLedgerEntry")
	if err != nil {
		return r.resolveLedgerInsertErr(ctx, err, spec)
	}

	return inserted, nil
}

func (r *AgentOSLedgerRepo) resolveLedgerInsertErr(ctx context.Context, insertErr error, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error) {
	if !isPostgresUniqueViolation(insertErr) {
		return agentos.LedgerEntry{}, fmt.Errorf("AgentOSLedgerRepo - AppendLedgerEntry - insert: %w", insertErr)
	}

	existing, exists, err := r.ledgerEntryByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil {
		return agentos.LedgerEntry{}, err
	}

	if exists {
		return existing, nil
	}

	return agentos.LedgerEntry{}, fmt.Errorf("%w: ledger entry %q already exists", agentoscore.ErrInvalidLedgerEntry, spec.EntryID)
}

func (r *AgentOSLedgerRepo) ledgerEntryByIdempotencyKey(ctx context.Context, accountID, projectID, idempotencyKey string) (agentos.LedgerEntry, bool, error) {
	query, args, err := r.Builder.
		Select("entry_json").
		From("ledger_entries").
		Where(sq.Eq{"account_id": accountID, "project_id": projectID, "idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.LedgerEntry{}, false, fmt.Errorf("AgentOSLedgerRepo - ledgerEntryByIdempotencyKey - builder: %w", err)
	}

	return r.scanLedgerEntry(ctx, query, args, "ledgerEntryByIdempotencyKey")
}

func (r *AgentOSLedgerRepo) ledgerEntryByID(ctx context.Context, entryID string) (agentos.LedgerEntry, bool, error) {
	query, args, err := r.Builder.Select("entry_json").From("ledger_entries").Where(sq.Eq{"entry_id": entryID}).ToSql()
	if err != nil {
		return agentos.LedgerEntry{}, false, fmt.Errorf("AgentOSLedgerRepo - ledgerEntryByID - builder: %w", err)
	}

	return r.scanLedgerEntry(ctx, query, args, "ledgerEntryByID")
}

func (r *AgentOSLedgerRepo) scanLedgerEntry(ctx context.Context, query string, args []any, name string) (agentos.LedgerEntry, bool, error) {
	entry, err := scanSingleJSON[agentos.LedgerEntry](r.Pool.QueryRow(ctx, query, args...), "AgentOSLedgerRepo - "+name)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentos.LedgerEntry{}, false, nil
	}

	if err != nil {
		return agentos.LedgerEntry{}, false, err
	}

	return entry, true, nil
}

var _ agentosledger.Store = (*AgentOSLedgerRepo)(nil)
