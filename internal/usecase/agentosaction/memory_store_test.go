package agentosaction

import (
	"errors"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

const alternateActionID = "action-2"

func TestMemoryStoreCreateActionIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleActionSpec()
	status := initialActionStatus(&spec)

	first, created, err := store.CreateAction(t.Context(), &spec, &status)
	if err != nil {
		t.Fatalf("CreateAction: %v", err)
	}

	if !created {
		t.Fatal("created = false, want true")
	}

	second, created, err := store.CreateAction(t.Context(), &spec, &status)
	if err != nil {
		t.Fatalf("CreateAction replay: %v", err)
	}

	if created {
		t.Fatal("created replay = true, want false")
	}

	if second.LifecycleState != first.LifecycleState {
		t.Fatalf("replayed lifecycle = %q, want %q", second.LifecycleState, first.LifecycleState)
	}
}

func TestMemoryStoreRejectsActionKeyReuseWithDifferentRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleActionSpec()
	status := initialActionStatus(&spec)

	if _, _, err := store.CreateAction(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreateAction: %v", err)
	}

	changed := spec
	changed.ActionID = alternateActionID
	changedStatus := initialActionStatus(&changed)

	_, _, err := store.CreateAction(t.Context(), &changed, &changedStatus)
	if !errors.Is(err, agentoscore.ErrInvalidGovernedAction) {
		t.Fatalf("CreateAction changed error = %v, want ErrInvalidGovernedAction", err)
	}
}

func TestMemoryStoreUpdateActionStatusIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleAction(t, store)
	status := initialActionStatus(&spec)
	status.LifecycleState = agentos.ActionCanceled

	first, err := store.UpdateActionStatus(t.Context(), &status, "cancel-1")
	if err != nil {
		t.Fatalf("UpdateActionStatus: %v", err)
	}

	status.LifecycleState = agentos.ActionFailed

	second, err := store.UpdateActionStatus(t.Context(), &status, "cancel-1")
	if err != nil {
		t.Fatalf("UpdateActionStatus replay: %v", err)
	}

	if second.LifecycleState != first.LifecycleState {
		t.Fatalf("replayed lifecycle = %q, want %q", second.LifecycleState, first.LifecycleState)
	}
}

func TestMemoryStoreGetActionEnforcesTenantScope(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleAction(t, store)

	_, _, exists, err := store.GetAction(t.Context(), agentos.ActionRef{
		ActionID:  spec.ActionID,
		AccountID: "acct-other",
		ProjectID: spec.ProjectID,
	})
	if exists {
		t.Fatal("GetAction exists = true, want false for tenant mismatch")
	}

	if !errors.Is(err, agentoscore.ErrInvalidGovernedActionScope) {
		t.Fatalf("GetAction tenant error = %v, want ErrInvalidGovernedActionScope", err)
	}
}

func createSampleAction(t *testing.T, store *MemoryStore) agentos.GovernedActionSpec {
	t.Helper()

	spec := sampleActionSpec()
	status := initialActionStatus(&spec)

	if _, _, err := store.CreateAction(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreateAction: %v", err)
	}

	return spec
}
