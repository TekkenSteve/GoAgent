package agentosledger

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// MemoryStore is an explicit in-process ledger store for unit tests and
// embedded demos.
type MemoryStore struct {
	mu        sync.RWMutex
	entries   []agentos.LedgerEntry
	entryByID map[string]agentos.LedgerEntry
	keys      map[ledgerIdempotencyKey]agentos.LedgerEntry
}

type ledgerIdempotencyKey struct {
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

// NewMemoryStore creates an empty in-memory ledger store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entryByID: make(map[string]agentos.LedgerEntry),
		keys:      make(map[ledgerIdempotencyKey]agentos.LedgerEntry),
	}
}

// AppendLedgerEntry appends a durable ledger entry and assigns store-owned
// ordering.
func (s *MemoryStore) AppendLedgerEntry(_ context.Context, spec *agentos.LedgerEntrySpec) (agentos.LedgerEntry, error) {
	if err := agentos.ValidateLedgerEntrySpec(spec); err != nil {
		return agentos.LedgerEntry{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := ledgerKeyFromSpec(spec)
	if existing, exists := s.keys[key]; exists {
		requested := prepareLedgerEntry(spec, existing.Sequence, existing.CreatedAt)
		if err := ValidateLedgerEntryIdempotency(&existing, &requested); err != nil {
			return agentos.LedgerEntry{}, err
		}

		return cloneLedgerEntry(&existing), nil
	}

	if existing, exists := s.entryByID[spec.EntryID]; exists {
		return agentos.LedgerEntry{}, fmt.Errorf("%w: ledger entry %q already exists with idempotency key %q", agentos.ErrInvalidLedgerEntry, existing.EntryID, existing.IdempotencyKey)
	}

	createdAt := spec.OccurredAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	entry := prepareLedgerEntry(spec, int64(len(s.entries)+1), createdAt)
	s.entries = append(s.entries, entry)
	s.entryByID[entry.EntryID] = entry
	s.keys[key] = entry

	return cloneLedgerEntry(&entry), nil
}

// ListLedgerEntries returns tenant-scoped ledger entries in sequence order.
func (s *MemoryStore) ListLedgerEntries(_ context.Context, scope *agentos.LedgerScope) ([]agentos.LedgerEntry, error) {
	if err := agentos.ValidateLedgerScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	filtered := make([]agentos.LedgerEntry, 0, len(s.entries))
	for i := range s.entries {
		entry := s.entries[i]
		if !ledgerEntryMatchesScope(&entry, scope) {
			continue
		}

		filtered = append(filtered, cloneLedgerEntry(&entry))
	}

	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].Sequence < filtered[j].Sequence
	})

	if scope.Limit > 0 && len(filtered) > scope.Limit {
		filtered = filtered[:scope.Limit]
	}

	return filtered, nil
}

func ledgerKeyFromSpec(spec *agentos.LedgerEntrySpec) ledgerIdempotencyKey {
	return ledgerIdempotencyKey{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func prepareLedgerEntry(spec *agentos.LedgerEntrySpec, sequence int64, createdAt time.Time) agentos.LedgerEntry {
	entry := agentos.LedgerEntry{
		LedgerEntrySpec: cloneLedgerEntrySpec(spec),
		Sequence:        sequence,
		CreatedAt:       createdAt,
	}

	if entry.OccurredAt.IsZero() {
		entry.OccurredAt = createdAt
	}

	return entry
}

func ledgerEntryMatchesScope(entry *agentos.LedgerEntry, scope *agentos.LedgerScope) bool {
	if entry.AccountID != scope.AccountID || entry.ProjectID != scope.ProjectID {
		return false
	}

	if entry.Sequence <= scope.AfterSequence {
		return false
	}

	if scope.ProcessID != "" && entry.ProcessID != scope.ProcessID {
		return false
	}

	if scope.Kind != "" && entry.Kind != scope.Kind {
		return false
	}

	if scope.Resource.Kind != "" && entry.Resource != scope.Resource {
		return false
	}

	return true
}

func cloneLedgerEntry(entry *agentos.LedgerEntry) agentos.LedgerEntry {
	if entry == nil {
		return agentos.LedgerEntry{}
	}

	clone := *entry
	clone.LedgerEntrySpec = cloneLedgerEntrySpec(&entry.LedgerEntrySpec)

	return clone
}

func cloneLedgerEntrySpec(spec *agentos.LedgerEntrySpec) agentos.LedgerEntrySpec {
	if spec == nil {
		return agentos.LedgerEntrySpec{}
	}

	clone := *spec
	clone.DataRefs = cloneLedgerDataRefs(spec.DataRefs)
	clone.ArtifactRefs = cloneArtifactRefs(spec.ArtifactRefs)
	clone.Metadata = maps.Clone(spec.Metadata)

	return clone
}

func cloneLedgerDataRefs(refs []agentos.LedgerDataRef) []agentos.LedgerDataRef {
	if len(refs) == 0 {
		return nil
	}

	clone := make([]agentos.LedgerDataRef, len(refs))
	for i := range refs {
		clone[i] = refs[i]
		clone[i].Metadata = maps.Clone(refs[i].Metadata)
	}

	return clone
}

func cloneArtifactRefs(refs []agentos.ArtifactRef) []agentos.ArtifactRef {
	if len(refs) == 0 {
		return nil
	}

	clone := make([]agentos.ArtifactRef, len(refs))
	for i := range refs {
		clone[i] = refs[i]
		clone[i].Metadata = maps.Clone(refs[i].Metadata)
	}

	return clone
}
