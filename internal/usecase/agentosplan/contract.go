package agentosplan

import (
	"context"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ExpressionCompiler validates and compiles deterministic AgentOS expressions.
type ExpressionCompiler interface {
	Compile(expression string) (Expression, error)
}

// Expression evaluates a deterministic AgentOS boolean expression.
type Expression interface {
	Evaluate(ctx context.Context, vars map[string]any) (bool, error)
}

// ValueExpressionCompiler validates and compiles deterministic value expressions.
type ValueExpressionCompiler interface {
	CompileValue(expression string) (ValueExpression, error)
}

// ValueExpression evaluates a deterministic AgentOS expression to a value.
type ValueExpression interface {
	EvaluateValue(ctx context.Context, vars map[string]any) (any, error)
}

// CapabilityCatalog exposes backend capabilities available to RunPlan nodes.
type CapabilityCatalog interface {
	GetCapability(ctx context.Context, backend agentos.BackendRef, name string) (agentos.Capability, bool, error)
}

// ArtifactStore stores artifacts outside workflow history.
type ArtifactStore interface {
	Put(ctx context.Context, artifact agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error)
	Get(ctx context.Context, artifactID string) (agentos.ArtifactRef, any, error)
	List(ctx context.Context, planID string) ([]agentos.ArtifactRef, error)
}

// PlanIndex stores durable plan identity and the latest aggregate status.
type PlanIndex interface {
	CreatePlan(ctx context.Context, spec agentos.RunPlanSpec, status agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error)
	GetPlan(ctx context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error)
	UpdatePlanStatus(ctx context.Context, status agentos.RunPlanStatus, idempotencyKey string) error
}

// PlanStateSnapshot is the durable replay/audit snapshot written by plan activities.
type PlanStateSnapshot struct {
	Spec           agentos.RunPlanSpec
	Status         agentos.RunPlanStatus
	IdempotencyKey string
}

// PlanStateStore persists the latest deterministic reducer snapshot.
type PlanStateStore interface {
	SavePlanState(ctx context.Context, snapshot PlanStateSnapshot) error
	LoadPlanState(ctx context.Context, planID string) (PlanStateSnapshot, bool, error)
}

// PlanEventStore is the durable event source for RunPlan timelines.
type PlanEventStore interface {
	AppendPlanEvent(ctx context.Context, event agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error)
	ListPlanEvents(ctx context.Context, scope agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error)
}

// AuditAction identifies durable control-plane actions.
type AuditAction string

const (
	AuditActionPlanStart   AuditAction = "plan.start"
	AuditActionPlanSignal  AuditAction = "plan.signal"
	AuditActionPlanControl AuditAction = "plan.control"
)

// AuditRecord is a durable control-plane audit entry.
type AuditRecord struct {
	AuditID        string
	PlanID         string
	RunID          string
	NodeID         string
	ActorID        string
	Action         AuditAction
	IdempotencyKey string
	Payload        map[string]any
	CreatedAt      time.Time
}

// AuditStore persists idempotent control-plane audit records.
type AuditStore interface {
	RecordAudit(ctx context.Context, record AuditRecord) (AuditRecord, bool, error)
}

// Runner starts and controls backend-owned child runs.
type Runner interface {
	Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error)
	Signal(ctx context.Context, runID string, signal agentos.Signal) error
	Status(ctx context.Context, runID string) (agentos.RunStatus, error)
	Control(ctx context.Context, runID string, control agentos.ControlRequest) error
	Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error)
}
