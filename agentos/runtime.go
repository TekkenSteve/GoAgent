package agentos

import "context"

// Runtime is the stable embedded AgentOS boundary for external callers.
type Runtime interface {
	Start(ctx context.Context, spec RunSpec) (RunStatus, error)
	Signal(ctx context.Context, runID string, signal Signal) error
	Status(ctx context.Context, runID string) (RunStatus, error)
	Control(ctx context.Context, runID string, op ControlOperation) error
	Subscribe(ctx context.Context, scope StreamScope) (Subscription, error)
	Close() error
}

// Subscription is a stream of run events.
type Subscription interface {
	Events() <-chan Event
	Close() error
}
