package agentosledger

import (
	"bytes"
	"encoding/json"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// ValidateLedgerEntryIdempotency verifies that a repeated append request is the
// same durable ledger fact that originally claimed the idempotency key.
func ValidateLedgerEntryIdempotency(existing, requested *agentos.LedgerEntry) error {
	if existing.EntryID != requested.EntryID {
		return fmt.Errorf("%w: ledger idempotency key belongs to entry %q", agentoscore.ErrInvalidLedgerEntry, existing.EntryID)
	}

	if existing.Sequence != requested.Sequence {
		return fmt.Errorf("%w: ledger idempotency key belongs to sequence %d", agentoscore.ErrInvalidLedgerEntry, existing.Sequence)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing ledger entry: %w", agentoscore.ErrInvalidLedgerEntry, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested ledger entry: %w", agentoscore.ErrInvalidLedgerEntry, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: ledger idempotency key was reused with a different entry", agentoscore.ErrInvalidLedgerEntry)
	}

	return nil
}
