package agentosplan

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ExpressionCompiler validates and compiles deterministic AgentOS expressions.
type ExpressionCompiler interface {
	Compile(expression string) (Expression, error)
}

// Expression evaluates a deterministic AgentOS expression.
type Expression interface {
	Evaluate(ctx context.Context, vars map[string]any) (bool, error)
}

// CapabilityCatalog exposes backend capabilities available to RunPlan nodes.
type CapabilityCatalog interface {
	GetCapability(ctx context.Context, backend agentos.BackendRef, name string) (agentos.Capability, bool, error)
}

// ArtifactStore stores artifacts outside workflow history.
type ArtifactStore interface {
	Put(ctx context.Context, artifact agentos.ArtifactRef, payload any) (agentos.ArtifactRef, error)
	Get(ctx context.Context, artifactID string) (agentos.ArtifactRef, any, error)
	List(ctx context.Context, planID string) ([]agentos.ArtifactRef, error)
}

// Runner starts and controls backend-owned child runs.
type Runner interface {
	Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error)
	Signal(ctx context.Context, runID string, signal agentos.Signal) error
	Status(ctx context.Context, runID string) (agentos.RunStatus, error)
	Control(ctx context.Context, runID string, op agentos.ControlOperation) error
	Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error)
}
