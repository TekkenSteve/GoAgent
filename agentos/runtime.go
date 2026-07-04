package agentos

import "context"

// Runtime is the stable embedded AgentOS boundary for external callers.
type Runtime interface {
	Start(ctx context.Context, spec *RunSpec) (RunStatus, error)
	Signal(ctx context.Context, runID string, signal *Signal) error
	Status(ctx context.Context, runID string) (RunStatus, error)
	Control(ctx context.Context, runID string, control *ControlRequest) error
	Subscribe(ctx context.Context, scope StreamScope) (Subscription, error)
	Close() error
}

// PlanRuntime is the durable cross-backend AgentOS planning boundary.
type PlanRuntime interface {
	StartPlan(ctx context.Context, spec *RunPlanSpec) (RunPlanStatus, error)
	StatusPlan(ctx context.Context, ref PlanRef) (RunPlanStatus, error)
	DescribePlan(ctx context.Context, ref PlanRef) (RunPlanDescription, error)
	SignalPlan(ctx context.Context, ref PlanRef, signal *Signal) error
	ControlPlan(ctx context.Context, ref PlanRef, control *ControlRequest) error
	SubscribePlan(ctx context.Context, scope *PlanStreamScope) (Subscription, error)
	ListPlanEvents(ctx context.Context, scope *PlanEventScope) ([]PlanEvent, error)
	ListPlanDebugTraces(ctx context.Context, scope *PlanDebugTraceScope) ([]PlanDebugTrace, error)
	ListPlanArtifacts(ctx context.Context, scope *PlanArtifactScope) ([]ArtifactRef, error)
	GetPlanArtifact(ctx context.Context, scope *PlanArtifactScope) (Artifact, error)
	ListPlanAudits(ctx context.Context, scope *PlanAuditScope) ([]PlanAuditRecord, error)
}

// ProcessRuntime is the durable business-process boundary for generic domain
// resources. Implementations should map one long-lived domain object to one
// coarse-grained process workflow instead of modeling backend internals or
// individual data records as AgentOS nodes.
type ProcessRuntime interface {
	StartProcess(ctx context.Context, spec *ProcessSpec) (ProcessStatus, error)
	StatusProcess(ctx context.Context, ref ProcessRef) (ProcessStatus, error)
	DescribeProcess(ctx context.Context, ref ProcessRef) (ProcessDescription, error)
	SignalProcess(ctx context.Context, ref ProcessRef, signal *Signal) error
	ControlProcess(ctx context.Context, ref ProcessRef, control *ControlRequest) error
	SubscribeProcess(ctx context.Context, scope *ProcessStreamScope) (Subscription, error)
	ListProcessEvents(ctx context.Context, scope *ProcessEventScope) ([]ProcessEvent, error)
}

// Subscription is a stream of run events.
type Subscription interface {
	Events() <-chan Event
	Close() error
}
