package temporal

import (
	"context"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func putArtifact(ctx context.Context, store agentosplan.ArtifactStore, artifact *agentoscore.ArtifactRef, payload any, idempotencyKey string) (agentoscore.ArtifactRef, error) {
	return store.Put(ctx, artifact, payload, idempotencyKey)
}
