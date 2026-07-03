package agentosplan

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func putArtifact(ctx context.Context, store ArtifactStore, artifact *agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	return store.Put(ctx, artifact, payload, idempotencyKey)
}
