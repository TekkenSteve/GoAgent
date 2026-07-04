package agentosplan

import (
	"context"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func putArtifact(ctx context.Context, store ArtifactStore, artifact *agentoscore.ArtifactRef, payload any, idempotencyKey string) (agentoscore.ArtifactRef, error) {
	return store.Put(ctx, artifact, payload, idempotencyKey)
}
