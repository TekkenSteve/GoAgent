// Package agentosledger implements the AgentOS process ledger use cases.
package agentosledger

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// Store persists append-only ledger entries.
type Store interface {
	AppendLedgerEntry(ctx context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error)
	ListLedgerEntries(ctx context.Context, scope *agentos.LedgerScope) ([]agentos.LedgerEntry, error)
}
