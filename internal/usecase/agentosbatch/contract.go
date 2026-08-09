// Package agentosbatch implements batched AgentOS operation use cases.
package agentosbatch

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// Store persists workset specs, latest projections, and processed chunk keys.
type Store interface {
	CreateWorkset(ctx context.Context, spec *agentos.WorksetSpec, status *agentos.WorksetStatus) (agentos.WorksetStatus, bool, error)
	GetWorkset(ctx context.Context, ref agentos.WorksetRef) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error)
	ListWorksets(ctx context.Context, scope *agentos.WorksetScope) ([]agentos.WorksetStatus, error)
	ApplyChunkResult(ctx context.Context, ref agentos.WorksetRef, result *agentos.WorksetChunkResult, status *agentos.WorksetStatus) (agentos.WorksetStatus, error)
	UpdateWorksetStatus(ctx context.Context, status *agentos.WorksetStatus, idempotencyKey string) (agentos.WorksetStatus, error)
}
