package agentos

import "context"

// Runtime is the stable embedded AgentOS boundary for external callers.
type Runtime interface {
	Start(ctx context.Context, spec RunSpec) (RunStatus, error)
	Signal(ctx context.Context, runID string, signal Signal) error
	Status(ctx context.Context, runID string) (RunStatus, error)
	Control(ctx context.Context, runID string, control ControlRequest) error
	Subscribe(ctx context.Context, scope StreamScope) (Subscription, error)
	Close() error
}

// PlanRuntime is the durable cross-backend AgentOS planning boundary.
type PlanRuntime interface {
	StartPlan(ctx context.Context, spec RunPlanSpec) (RunPlanStatus, error)
	StatusPlan(ctx context.Context, planID string) (RunPlanStatus, error)
	SignalPlan(ctx context.Context, planID string, signal Signal) error
	ControlPlan(ctx context.Context, planID string, control ControlRequest) error
	SubscribePlan(ctx context.Context, scope PlanStreamScope) (Subscription, error)
}

// Subscription is a stream of run events.
type Subscription interface {
	Events() <-chan Event
	Close() error
}
