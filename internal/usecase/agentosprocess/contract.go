package agentosprocess

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// ProcessIndex stores durable process identity and the latest projection.
type ProcessIndex interface {
	CreateProcess(ctx context.Context, spec *agentos.Spec, status *agentos.Status) (agentos.Status, bool, error)
	GetProcessByRef(ctx context.Context, ref agentos.Ref) (agentos.Spec, agentos.Status, bool, error)
	ListProcesses(ctx context.Context, scope *agentos.Scope) ([]agentos.Status, error)
	UpdateProcessStatus(ctx context.Context, status *agentos.Status, idempotencyKey string) (agentos.Status, error)
}

// ProcessEventStore is the durable event source for process timelines.
type ProcessEventStore interface {
	AppendProcessEvent(ctx context.Context, event *agentos.Event, idempotencyKey string) (agentos.Event, error)
	ListProcessEvents(ctx context.Context, scope *agentos.EventScope) ([]agentos.Event, error)
}

// ProcessEventSubscription is the typed live tail for process events.
type ProcessEventSubscription interface {
	Events() <-chan agentos.Event
	Close() error
}

// ProcessEventSubscriber subscribes to the live process event tail.
type ProcessEventSubscriber interface {
	SubscribeProcessEvents(ctx context.Context, scope *agentos.StreamScope) (ProcessEventSubscription, error)
}
