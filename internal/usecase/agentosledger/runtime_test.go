package agentosledger

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRuntimeImplementsLedgerRuntime(t *testing.T) {
	t.Parallel()

	var _ agentos.LedgerRuntime = (*Runtime)(nil)
}

func TestRuntimeAppendLedgerEntry(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleLedgerEntrySpec()

	entry, err := runtime.AppendLedgerEntry(t.Context(), &spec)
	if err != nil {
		t.Fatalf("AppendLedgerEntry: %v", err)
	}

	if entry.Sequence != 1 {
		t.Fatalf("sequence = %d, want 1", entry.Sequence)
	}
}

func TestRuntimeListLedgerEntriesFiltersByScope(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	decision := sampleLedgerEntrySpec()
	evidence := sampleLedgerEntrySpec()
	evidence.EntryID = "ledger-2"
	evidence.IdempotencyKey = "ledger-key-2"
	evidence.Kind = agentos.LedgerEntryEvidence

	if _, err := runtime.AppendLedgerEntry(t.Context(), &decision); err != nil {
		t.Fatalf("AppendLedgerEntry decision: %v", err)
	}

	if _, err := runtime.AppendLedgerEntry(t.Context(), &evidence); err != nil {
		t.Fatalf("AppendLedgerEntry evidence: %v", err)
	}

	entries, err := runtime.ListLedgerEntries(t.Context(), &agentos.LedgerScope{
		AccountID: decision.AccountID,
		ProjectID: decision.ProjectID,
		Kind:      agentos.LedgerEntryEvidence,
	})
	if err != nil {
		t.Fatalf("ListLedgerEntries: %v", err)
	}

	if len(entries) != 1 || entries[0].EntryID != evidence.EntryID {
		t.Fatalf("entries = %#v, want only evidence entry", entries)
	}
}

func TestRuntimeRejectsInvalidLedgerEntry(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleLedgerEntrySpec()
	spec.IdempotencyKey = ""

	_, err := runtime.AppendLedgerEntry(t.Context(), &spec)
	if !errors.Is(err, agentos.ErrInvalidLedgerEntry) {
		t.Fatalf("AppendLedgerEntry error = %v, want ErrInvalidLedgerEntry", err)
	}
}

func newSampleRuntime(t *testing.T) *Runtime {
	t.Helper()

	runtime, err := NewRuntime(NewMemoryStore())
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	return runtime
}

func sampleLedgerEntrySpec() agentos.LedgerEntrySpec {
	return agentos.LedgerEntrySpec{
		EntryID:        "ledger-1",
		IdempotencyKey: "ledger-key-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		ProcessID:      "process-1",
		Resource: agentos.ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		Kind:       agentos.LedgerEntryDecision,
		Actor:      agentos.ActorRef{Kind: "agent", ActorID: "agent-1"},
		OccurredAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		Summary:    "resource reviewed",
		DataRefs: []agentos.LedgerDataRef{{
			Kind:      "evidence",
			URI:       "s3://bucket/evidence.json",
			MediaType: "application/json",
		}},
	}
}
