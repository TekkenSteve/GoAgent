package agentosledger

import (
	"context"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// Runtime coordinates the generic AgentOS ledger boundary.
type Runtime struct {
	store Store
}

// NewRuntime creates a generic append-only ledger use case.
func NewRuntime(store Store) (*Runtime, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: ledger store is required", agentoscore.ErrInvalidLedgerScope)
	}

	return &Runtime{store: store}, nil
}

// AppendLedgerEntry records a durable audit fact.
func (r *Runtime) AppendLedgerEntry(ctx context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error) {
	if err := agentos.ValidateLedgerEntrySpec(spec); err != nil {
		return agentos.LedgerEntry{}, err
	}

	return r.store.AppendLedgerEntry(ctx, spec)
}

// ListLedgerEntries reads ledger entries through a generic query scope.
func (r *Runtime) ListLedgerEntries(ctx context.Context, scope *agentos.LedgerScope) ([]agentos.LedgerEntry, error) {
	if err := agentos.ValidateLedgerScope(scope); err != nil {
		return nil, err
	}

	return r.store.ListLedgerEntries(ctx, scope)
}
