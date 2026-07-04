package agentosprocess

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ProcessIndex stores durable process identity and the latest projection.
type ProcessIndex interface {
	CreateProcess(ctx context.Context, spec *agentos.ProcessSpec, status *agentos.ProcessStatus) (agentos.ProcessStatus, bool, error)
	GetProcessByRef(ctx context.Context, ref agentos.ProcessRef) (agentos.ProcessSpec, agentos.ProcessStatus, bool, error)
}

// ProcessEventStore is the durable event source for process timelines.
type ProcessEventStore interface {
	AppendProcessEvent(ctx context.Context, event *agentos.ProcessEvent, idempotencyKey string) (agentos.ProcessEvent, error)
	ListProcessEvents(ctx context.Context, scope *agentos.ProcessEventScope) ([]agentos.ProcessEvent, error)
}
