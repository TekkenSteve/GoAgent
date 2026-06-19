package agentosplan

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestMemoryArtifactStoreRequiresIdempotencyKey(t *testing.T) {
	store := NewMemoryArtifactStore()

	_, err := store.Put(context.Background(), agentos.ArtifactRef{
		Name: "summary",
		Kind: agentos.ArtifactKindObject,
	}, map[string]any{"ok": true}, "")
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("Put error = %v, want ErrInvalidArtifact", err)
	}
}

func TestMemoryArtifactStorePutIsIdempotent(t *testing.T) {
	store := NewMemoryArtifactStore()
	first, err := store.Put(context.Background(), agentos.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentos.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	second, err := store.Put(context.Background(), agentos.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentos.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if second.ArtifactID != first.ArtifactID {
		t.Fatalf("idempotent Put returned artifact id %q, want %q", second.ArtifactID, first.ArtifactID)
	}
	if first.MediaType != "application/json" || first.SizeBytes == 0 || first.Digest == "" {
		t.Fatalf("artifact payload metadata = %#v", first)
	}

	_, payload, err := store.Get(context.Background(), first.ArtifactID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	value, ok := payload.(map[string]any)["value"]
	if !ok || value != "first" {
		t.Fatalf("payload = %#v, want first payload", payload)
	}
}

func TestMemoryArtifactStoreRejectsDifferentIdempotencyReplay(t *testing.T) {
	store := NewMemoryArtifactStore()
	_, err := store.Put(context.Background(), agentos.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentos.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	_, err = store.Put(context.Background(), agentos.ArtifactRef{
		PlanID: "plan-1",
		Name:   "summary",
		Kind:   agentos.ArtifactKindObject,
	}, map[string]any{"value": "second"}, "plan-1:key")
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("changed payload error = %v, want ErrInvalidArtifact", err)
	}

	_, err = store.Put(context.Background(), agentos.ArtifactRef{
		ArtifactID: "different-artifact",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("changed artifact error = %v, want ErrInvalidArtifact", err)
	}
}

func TestMemoryArtifactStoreIdempotentRefPublishKeepsPayload(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryArtifactStore()
	first, err := store.Put(ctx, agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	replayed, err := store.Put(ctx, first, nil, "plan-1:key")
	if err != nil {
		t.Fatalf("ref publish replay: %v", err)
	}
	if replayed.ArtifactID != first.ArtifactID || replayed.Digest != first.Digest || replayed.SizeBytes != first.SizeBytes {
		t.Fatalf("ref publish replay = %#v, want %#v", replayed, first)
	}

	_, payload, err := store.Get(ctx, first.ArtifactID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	value, ok := payload.(map[string]any)["value"]
	if !ok || value != "first" {
		t.Fatalf("payload = %#v, want original payload", payload)
	}
}

func TestMemoryArtifactStoreRejectsArtifactIDReuseWithDifferentKey(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryArtifactStore()
	first, err := store.Put(ctx, agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		PlanID:     "plan-1",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"value": "first"}, "plan-1:key")
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	_, err = store.Put(ctx, first, nil, "plan-1:other-key")
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("artifact id reuse error = %v, want ErrInvalidArtifact", err)
	}
}
