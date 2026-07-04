package agentosledger

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateLedgerEntryIdempotency verifies that a repeated append request is the
// same durable ledger fact that originally claimed the idempotency key.
func ValidateLedgerEntryIdempotency(existing, requested *agentos.LedgerEntry) error {
	if existing.EntryID != requested.EntryID {
		return fmt.Errorf("%w: ledger idempotency key belongs to entry %q", agentos.ErrInvalidLedgerEntry, existing.EntryID)
	}

	if existing.Sequence != requested.Sequence {
		return fmt.Errorf("%w: ledger idempotency key belongs to sequence %d", agentos.ErrInvalidLedgerEntry, existing.Sequence)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing ledger entry: %w", agentos.ErrInvalidLedgerEntry, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested ledger entry: %w", agentos.ErrInvalidLedgerEntry, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: ledger idempotency key was reused with a different entry", agentos.ErrInvalidLedgerEntry)
	}

	return nil
}
