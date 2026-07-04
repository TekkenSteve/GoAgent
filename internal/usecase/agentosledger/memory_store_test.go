package agentosledger

import (
	"errors"
	"fmt"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

const (
	secondLedgerEntryID  = "ledger-2"
	secondLedgerEntryKey = "ledger-key-2"
)

func TestMemoryStoreAppendLedgerEntryIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleLedgerEntrySpec()

	first, err := store.AppendLedgerEntry(t.Context(), &spec)
	if err != nil {
		t.Fatalf("AppendLedgerEntry: %v", err)
	}

	second, err := store.AppendLedgerEntry(t.Context(), &spec)
	if err != nil {
		t.Fatalf("AppendLedgerEntry replay: %v", err)
	}

	if second.EntryID != first.EntryID || second.Sequence != first.Sequence {
		t.Fatalf("replayed entry = (%q, %d), want (%q, %d)", second.EntryID, second.Sequence, first.EntryID, first.Sequence)
	}
}

func TestMemoryStoreRejectsLedgerKeyReuseWithDifferentEntry(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleLedgerEntrySpec()

	if _, err := store.AppendLedgerEntry(t.Context(), &spec); err != nil {
		t.Fatalf("AppendLedgerEntry: %v", err)
	}

	changed := spec
	changed.EntryID = secondLedgerEntryID

	_, err := store.AppendLedgerEntry(t.Context(), &changed)
	if !errors.Is(err, agentoscore.ErrInvalidLedgerEntry) {
		t.Fatalf("AppendLedgerEntry changed error = %v, want ErrInvalidLedgerEntry", err)
	}
}

func TestMemoryStoreRejectsEntryIDReuseWithDifferentKey(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleLedgerEntrySpec()

	if _, err := store.AppendLedgerEntry(t.Context(), &spec); err != nil {
		t.Fatalf("AppendLedgerEntry: %v", err)
	}

	changed := spec
	changed.IdempotencyKey = secondLedgerEntryKey

	_, err := store.AppendLedgerEntry(t.Context(), &changed)
	if !errors.Is(err, agentoscore.ErrInvalidLedgerEntry) {
		t.Fatalf("AppendLedgerEntry changed error = %v, want ErrInvalidLedgerEntry", err)
	}
}

func TestMemoryStoreListLedgerEntriesFiltersAndLimits(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleLedgerEntrySpec()

	for i, kind := range []agentos.LedgerEntryKind{
		agentos.LedgerEntryDecision,
		agentos.LedgerEntryEvidence,
		agentos.LedgerEntryEvidence,
	} {
		entry := spec
		entry.EntryID = fmt.Sprintf("ledger-%d", i+1)
		entry.IdempotencyKey = fmt.Sprintf("ledger-key-%d", i+1)
		entry.Kind = kind

		if _, err := store.AppendLedgerEntry(t.Context(), &entry); err != nil {
			t.Fatalf("AppendLedgerEntry %d: %v", i, err)
		}
	}

	entries, err := store.ListLedgerEntries(t.Context(), &agentos.LedgerScope{
		AccountID:     spec.AccountID,
		ProjectID:     spec.ProjectID,
		Kind:          agentos.LedgerEntryEvidence,
		AfterSequence: 1,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("ListLedgerEntries: %v", err)
	}

	if len(entries) != 1 || entries[0].Sequence != 2 {
		t.Fatalf("entries = %#v, want one evidence entry at sequence 2", entries)
	}
}
