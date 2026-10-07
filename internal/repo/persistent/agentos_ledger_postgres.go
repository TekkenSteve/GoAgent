package persistent

import (
	"context"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosledger"
)

// AgentOSLedgerRepo persists append-only generic AgentOS ledger entries.
// Statements and bindings come from queries/ledger.sql; this file owns the
// append-idempotency protocol around them.
type AgentOSLedgerRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewAgentOSLedgerRepo creates a Postgres-backed ledger store.
func NewAgentOSLedgerRepo(pg *postgres.Postgres) *AgentOSLedgerRepo {
	return &AgentOSLedgerRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
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

	params := sqlcgen.ListLedgerEntriesParams{
		AccountID:    scope.AccountID,
		ProjectID:    scope.ProjectID,
		Sequence:     scope.AfterSequence,
		ProcessID:    scope.ProcessID,
		ResourceKind: string(scope.Resource.Kind),
		ResourceID:   scope.Resource.ResourceID,
		Kind:         string(scope.Kind),
		RowLimit:     optionalInt8(scope.Limit),
	}

	rows, err := r.queries.ListLedgerEntries(ctx, params)

	return listRecords("AgentOSLedgerRepo - ListLedgerEntries", rows, err, func(row *[]byte) (agentos.LedgerEntry, error) {
		return decodeProcessPlatformJSON[agentos.LedgerEntry]("AgentOSLedgerRepo - ListLedgerEntries", *row)
	})
}

func (r *AgentOSLedgerRepo) insertLedgerEntry(ctx context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error) {
	entryJSON, err := marshalProcessPlatformJSON("AgentOSLedgerRepo - AppendLedgerEntry entry", spec)
	if err != nil {
		return agentos.LedgerEntry{}, err
	}

	storedJSON, err := r.queries.AppendLedgerEntry(ctx, sqlcgen.AppendLedgerEntryParams{
		EntryID:        spec.EntryID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		ProcessID:      spec.ProcessID,
		ResourceKind:   string(spec.Resource.Kind),
		ResourceID:     spec.Resource.ResourceID,
		Kind:           string(spec.Kind),
		IdempotencyKey: spec.IdempotencyKey,
		EntryJson:      entryJSON,
		OccurredAt:     optionalTimestamptz(spec.OccurredAt),
	})
	if err != nil {
		return r.resolveLedgerInsertErr(ctx, err, spec)
	}

	inserted, err := decodeProcessPlatformJSON[agentos.LedgerEntry]("AgentOSLedgerRepo - AppendLedgerEntry", storedJSON)
	if err != nil {
		return agentos.LedgerEntry{}, err
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
	storedJSON, err := r.queries.GetLedgerEntryByIdempotencyKey(ctx, sqlcgen.GetLedgerEntryByIdempotencyKeyParams{
		AccountID:      accountID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
	})

	return getJSONRecord[agentos.LedgerEntry]("AgentOSLedgerRepo - ledgerEntryByIdempotencyKey", storedJSON, err)
}

func (r *AgentOSLedgerRepo) ledgerEntryByID(ctx context.Context, entryID string) (agentos.LedgerEntry, bool, error) {
	storedJSON, err := r.queries.GetLedgerEntryByID(ctx, entryID)

	return getJSONRecord[agentos.LedgerEntry]("AgentOSLedgerRepo - ledgerEntryByID", storedJSON, err)
}

var _ agentosledger.Store = (*AgentOSLedgerRepo)(nil)
