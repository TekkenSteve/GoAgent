package temporal

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func putArtifact(ctx context.Context, store agentosplan.ArtifactStore, artifact *agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	return store.Put(ctx, artifact, payload, idempotencyKey)
}
