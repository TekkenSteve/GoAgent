package agentosruntime

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// AgentBackend is the execution contract shared by native and external agent runtimes.
type AgentBackend interface {
	Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error)
	Signal(ctx context.Context, runID string, signal *agentos.Signal) error
	Control(ctx context.Context, runID string, control *agentos.ControlRequest) error
	Status(ctx context.Context, runID string) (agentos.RunStatus, error)
	Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error)
}

// EventSubscriber exposes the normalized AgentOS event stream shared by backends.
type EventSubscriber interface {
	SubscribeAgentOS(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error)
}

// BackendCapabilities exposes optional backend features without forcing every
// implementation to support every advanced AgentOS operation.
type BackendCapabilities struct {
	SupportsSignal               bool
	SupportsSignalUserMessage    bool
	SupportsPause                bool
	SupportsResume               bool
	SupportsCancel               bool
	SupportsStreaming            bool
	SupportsArtifacts            bool
	SupportsCheckpoint           bool
	SupportsResumeFromCheckpoint bool
}

// CapableBackend is implemented by backends that can describe their optional features.
type CapableBackend interface {
	Capabilities() BackendCapabilities
}
