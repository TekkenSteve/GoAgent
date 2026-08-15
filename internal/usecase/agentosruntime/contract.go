// Package agentosruntime hosts AgentOS runtimes for the use-case layer.
package agentosruntime

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// AgentBackend is the execution contract shared by native and external agent runtimes.
type AgentBackend interface {
	Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error)
	Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error
	Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) error
	Status(ctx context.Context, runID string) (agentos.RunStatus, error)
	Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error)
}

// EventSubscriber exposes the normalized AgentOS event stream shared by backends.
type EventSubscriber interface {
	SubscribeAgentOS(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error)
}

// LifecyclePublisher mirrors a backend-owned run's observable lifecycle onto
// the data plane — the write-side twin of EventSubscriber. One-shot backends
// (HTTP / gRPC / temporal_external) own no local byte stream, so the only
// AG-UI events they can produce are the lifecycle milestones they observe:
// Start success → RUN_STARTED, a terminal Status → RUN_FINISHED / RUN_ERROR /
// RUN_CANCELLED. Publishing is fail-open: a bus hiccup never fails the run.
// Statuses pass by pointer because RunStatus is heavy (~112 bytes); a nil
// status is a no-op.
type LifecyclePublisher interface {
	PublishStarted(ctx context.Context, spec *agentos.RunSpec, status *agentos.RunStatus)
	PublishStatus(ctx context.Context, runID string, status *agentos.RunStatus)
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
