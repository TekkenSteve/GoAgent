package agentosplan

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func appendPlanEvent(ctx context.Context, store PlanEventStore, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	return store.AppendPlanEvent(ctx, event, idempotencyKey)
}

func savePlanState(ctx context.Context, store PlanStateStore, snapshot *PlanStateSnapshot) error {
	return store.SavePlanState(ctx, snapshot)
}

func savePlanMetricCheckpoint(ctx context.Context, store PlanMetricCheckpointStore, checkpoint *PlanMetricCheckpoint) error {
	return store.SavePlanMetricCheckpoint(ctx, checkpoint)
}

func persistPlanTransition(ctx context.Context, store PlanTransitionStore, snapshot *PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	return store.PersistPlanTransition(ctx, snapshot, event, idempotencyKey)
}
