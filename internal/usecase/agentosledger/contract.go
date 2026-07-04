package agentosledger

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Store persists append-only ledger entries.
type Store interface {
	AppendLedgerEntry(ctx context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error)
	ListLedgerEntries(ctx context.Context, scope *agentos.LedgerScope) ([]agentos.LedgerEntry, error)
}
