package agentosaction

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Store persists governed action specs and latest lifecycle projections.
type Store interface {
	CreateAction(ctx context.Context, spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) (agentos.GovernedActionStatus, bool, error)
	GetAction(ctx context.Context, ref agentos.ActionRef) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error)
	ListActions(ctx context.Context, scope *agentos.ActionScope) ([]agentos.GovernedActionStatus, error)
	UpdateActionStatus(ctx context.Context, status *agentos.GovernedActionStatus, idempotencyKey string) (agentos.GovernedActionStatus, error)
}
