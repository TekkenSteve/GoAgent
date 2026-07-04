package agentosbatch

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const alternateWorksetID = "workset-2"

func TestMemoryStoreCreateWorksetIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleWorksetSpec()
	status := initialWorksetStatus(&spec)

	first, created, err := store.CreateWorkset(t.Context(), &spec, &status)
	if err != nil {
		t.Fatalf("CreateWorkset: %v", err)
	}

	if !created {
		t.Fatal("created = false, want true")
	}

	second, created, err := store.CreateWorkset(t.Context(), &spec, &status)
	if err != nil {
		t.Fatalf("CreateWorkset replay: %v", err)
	}

	if created {
		t.Fatal("created replay = true, want false")
	}

	if second.LifecycleState != first.LifecycleState {
		t.Fatalf("replayed lifecycle = %q, want %q", second.LifecycleState, first.LifecycleState)
	}
}

func TestMemoryStoreRejectsWorksetKeyReuseWithDifferentRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleWorksetSpec()
	status := initialWorksetStatus(&spec)

	if _, _, err := store.CreateWorkset(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreateWorkset: %v", err)
	}

	changed := spec
	changed.WorksetID = alternateWorksetID
	changedStatus := initialWorksetStatus(&changed)

	_, _, err := store.CreateWorkset(t.Context(), &changed, &changedStatus)
	if !errors.Is(err, agentos.ErrInvalidWorkset) {
		t.Fatalf("CreateWorkset changed error = %v, want ErrInvalidWorkset", err)
	}
}

func TestMemoryStoreApplyChunkResultIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleWorkset(t, store)
	ref := worksetRefFromSpec(&spec)
	status := initialWorksetStatus(&spec)
	status.Progress.CompletedChunks = 1
	status.Progress.CompletedItems = 5

	result := agentos.WorksetChunkResult{ChunkID: "chunk-1", IdempotencyKey: "chunk-key-1", Succeeded: true}

	first, err := store.ApplyChunkResult(t.Context(), ref, &result, &status)
	if err != nil {
		t.Fatalf("ApplyChunkResult: %v", err)
	}

	status.Progress.CompletedItems = 10

	second, err := store.ApplyChunkResult(t.Context(), ref, &result, &status)
	if err != nil {
		t.Fatalf("ApplyChunkResult replay: %v", err)
	}

	if second.Progress.CompletedItems != first.Progress.CompletedItems {
		t.Fatalf("replayed completed items = %d, want %d", second.Progress.CompletedItems, first.Progress.CompletedItems)
	}
}

func TestMemoryStoreGetWorksetEnforcesTenantScope(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleWorkset(t, store)

	_, _, exists, err := store.GetWorkset(t.Context(), agentos.WorksetRef{
		WorksetID: spec.WorksetID,
		AccountID: "acct-other",
		ProjectID: spec.ProjectID,
	})
	if exists {
		t.Fatal("GetWorkset exists = true, want false for tenant mismatch")
	}

	if !errors.Is(err, agentos.ErrInvalidWorksetScope) {
		t.Fatalf("GetWorkset tenant error = %v, want ErrInvalidWorksetScope", err)
	}
}

func createSampleWorkset(t *testing.T, store *MemoryStore) agentos.WorksetSpec {
	t.Helper()

	spec := sampleWorksetSpec()
	status := initialWorksetStatus(&spec)

	if _, _, err := store.CreateWorkset(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreateWorkset: %v", err)
	}

	return spec
}
