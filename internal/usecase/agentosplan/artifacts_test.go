package agentosplan

import (
	"context"
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

const FIRST = "first"

func TestMemoryArtifactStoreRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()

	_, err := putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		Name: "summary",
		Kind: agentoscore.ArtifactKindObject,
	}, map[string]any{"ok": true}, "")
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("Put error = %v, want ErrInvalidArtifact", err)
	}
}

func TestMemoryArtifactStoreRequiresPlanID(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()

	_, err := putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		Name: "summary",
		Kind: agentoscore.ArtifactKindObject,
	}, map[string]any{"ok": true}, "artifact-key")
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("Put error = %v, want ErrInvalidArtifact", err)
	}
}

func TestMemoryArtifactStorePutIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()
	ctx := context.Background()

	first := putSummaryArtifactForTest(ctx, t, store, "plan-1", "plan-1:key", FIRST)
	second := putSummaryArtifactForTest(ctx, t, store, "plan-1", "plan-1:key", FIRST)

	if second.ArtifactID != first.ArtifactID {
		t.Fatalf("idempotent Put returned artifact id %q, want %q", second.ArtifactID, first.ArtifactID)
	}

	if first.MediaType != "application/json" || first.SizeBytes == 0 || first.Digest == "" {
		t.Fatalf("artifact payload metadata = %#v", first)
	}

	scope := agentos.PlanArtifactScope{
		PlanID:     "plan-1",
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		ArtifactID: first.ArtifactID,
	}
	requireMemoryArtifactPayloadValue(ctx, t, store, &scope, FIRST)
}

func putSummaryArtifactForTest(ctx context.Context, t *testing.T, store *MemoryArtifactStore, planID, idempotencyKey, value string) agentoscore.ArtifactRef {
	t.Helper()

	ref, err := putArtifact(ctx, store, &agentoscore.ArtifactRef{
		PlanID: planID,
		Name:   "summary",
		Kind:   agentoscore.ArtifactKindObject,
	}, map[string]any{"value": value}, idempotencyKey)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	return ref
}

func requireMemoryArtifactPayloadValue(ctx context.Context, t *testing.T, store *MemoryArtifactStore, scope *agentos.PlanArtifactScope, want string) {
	t.Helper()

	_, payload, err := store.Get(ctx, scope)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	m, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("payload is not a map: %T", payload)
	}

	if m["value"] != want {
		t.Fatalf("payload = %#v, want value %q", payload, want)
	}
}

func TestMemoryArtifactStoreScopesIdempotencyKeyByPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()

	first, err := putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "shared-key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	second, err := putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		PlanID: "plan-2",
		Name:   "summary",
		Kind:   agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "second"}, "shared-key")
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}

	if second.ArtifactID == first.ArtifactID {
		t.Fatalf("artifact id = %q for both plans", first.ArtifactID)
	}
}

func TestMemoryArtifactStoreRejectsDifferentIdempotencyReplay(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()

	_, err := putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	_, err = putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "second"}, "plan-1:key")
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("changed payload error = %v, want ErrInvalidArtifact", err)
	}

	_, err = putArtifact(context.Background(), store, &agentoscore.ArtifactRef{
		ArtifactID: "different-artifact",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("changed artifact error = %v, want ErrInvalidArtifact", err)
	}
}

func TestMemoryArtifactStoreIdempotentRefPublishKeepsPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryArtifactStore()

	first, err := putArtifact(ctx, store, &agentoscore.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	replayed, err := store.Put(ctx, &first, nil, "plan-1:key")
	if err != nil {
		t.Fatalf("ref publish replay: %v", err)
	}

	if replayed.ArtifactID != first.ArtifactID || replayed.Digest != first.Digest || replayed.SizeBytes != first.SizeBytes {
		t.Fatalf("ref publish replay = %#v, want %#v", replayed, first)
	}

	scope := agentos.PlanArtifactScope{
		PlanID:     first.PlanID,
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		ArtifactID: first.ArtifactID,
	}

	_, payload, err := store.Get(ctx, &scope)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	m, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("payload is not a map: %T", payload)
	}

	value, ok := m["value"]
	if !ok || value != FIRST {
		t.Fatalf("payload = %#v, want original payload", payload)
	}
}

func TestMemoryArtifactStoreRejectsArtifactIDReuseWithDifferentKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryArtifactStore()

	first, err := putArtifact(ctx, store, &agentoscore.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	_, err = store.Put(ctx, &first, nil, "plan-1:other-key")
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("artifact id reuse error = %v, want ErrInvalidArtifact", err)
	}
}

func TestMemoryArtifactStoreRejectsNewRefOnlyPayloadMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryArtifactStore()
	base := agentoscore.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentoscore.ArtifactKindObject,
	}

	tests := map[string]agentoscore.ArtifactRef{
		"uri": func() agentoscore.ArtifactRef {
			ref := base
			ref.URI = "local://artifact/plan-1/artifact-1"

			return ref
		}(),
		"size": func() agentoscore.ArtifactRef {
			ref := base
			ref.SizeBytes = 12

			return ref
		}(),
		"digest": func() agentoscore.ArtifactRef {
			ref := base
			ref.Digest = "sha256:abc"

			return ref
		}(),
	}

	for name, ref := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := store.Put(ctx, &ref, nil, "plan-1:"+name)
			if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
				t.Fatalf("Put error = %v, want ErrInvalidArtifact", err)
			}
		})
	}
}

func TestMemoryArtifactStoreRequiresScopedReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryArtifactStore()

	ref, err := putArtifact(ctx, store, &agentoscore.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentoscore.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, scope := range []agentos.PlanArtifactScope{
		{PlanID: "plan-1", ArtifactID: ref.ArtifactID},
		{PlanID: "plan-1", AccountID: "acct-1", ArtifactID: ref.ArtifactID},
		{PlanID: "plan-1", ProjectID: "proj-1", ArtifactID: ref.ArtifactID},
	} {
		if _, _, err := store.Get(ctx, &scope); !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
			t.Fatalf("Get scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}

		listScope := agentos.PlanArtifactScope{
			PlanID:    scope.PlanID,
			AccountID: scope.AccountID,
			ProjectID: scope.ProjectID,
		}
		if _, err := store.List(ctx, &listScope); !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
			t.Fatalf("List scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}
}
