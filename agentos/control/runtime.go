package control

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// Runtime is the stable embedded AgentOS run-control boundary.
type Runtime interface {
	Start(ctx context.Context, spec *RunSpec) (RunStatus, error)
	Signal(ctx context.Context, runID string, signal *core.Signal) error
	Status(ctx context.Context, runID string) (RunStatus, error)
	Control(ctx context.Context, runID string, control *core.ControlRequest) error
	Subscribe(ctx context.Context, scope core.StreamScope) (core.Subscription, error)
	Close() error
}

// PlanRuntime is the durable cross-backend AgentOS planning boundary.
type PlanRuntime interface {
	StartPlan(ctx context.Context, spec *RunPlanSpec) (RunPlanStatus, error)
	StatusPlan(ctx context.Context, ref PlanRef) (RunPlanStatus, error)
	DescribePlan(ctx context.Context, ref PlanRef) (RunPlanDescription, error)
	SignalPlan(ctx context.Context, ref PlanRef, signal *core.Signal) error
	ControlPlan(ctx context.Context, ref PlanRef, control *core.ControlRequest) error
	SubscribePlan(ctx context.Context, scope *PlanStreamScope) (core.Subscription, error)
	ListPlanEvents(ctx context.Context, scope *PlanEventScope) ([]PlanEvent, error)
	ListPlanDebugTraces(ctx context.Context, scope *PlanDebugTraceScope) ([]PlanDebugTrace, error)
	ListPlanArtifacts(ctx context.Context, scope *PlanArtifactScope) ([]core.ArtifactRef, error)
	GetPlanArtifact(ctx context.Context, scope *PlanArtifactScope) (core.Artifact, error)
	ListPlanAudits(ctx context.Context, scope *PlanAuditScope) ([]PlanAuditRecord, error)
}
