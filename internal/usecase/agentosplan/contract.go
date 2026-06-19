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

// CapabilityRegistry persists backend capability declarations.
type CapabilityRegistry interface {
	RegisterCapability(ctx context.Context, capability agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error)
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

// PlanEventPublisher publishes live PlanEvents after the durable event source
// has assigned sequence and event identity.
type PlanEventPublisher interface {
	PublishPlanEvent(ctx context.Context, event agentos.PlanEvent) error
}

// PlanEventSubscriber subscribes to the live PlanEvent tail.
type PlanEventSubscriber interface {
	SubscribePlanEvents(ctx context.Context, scope agentos.PlanStreamScope) (agentos.Subscription, error)
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
	GetAuditRecord(ctx context.Context, idempotencyKey string) (AuditRecord, bool, error)
	ListAuditRecords(ctx context.Context, scope agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error)
}

// PlanCommandStatus is the durable outbox state for a control-plane command.
type PlanCommandStatus string

const (
	PlanCommandPending   PlanCommandStatus = "pending"
	PlanCommandDelivered PlanCommandStatus = "delivered"
	PlanCommandFailed    PlanCommandStatus = "failed"
)

// PlanCommandRecord is the durable command/outbox entry written before
// delivering a control-plane signal to PlanWorkflow.
type PlanCommandRecord struct {
	CommandID      string
	PlanID         string
	ActorID        string
	Action         AuditAction
	IdempotencyKey string
	Payload        map[string]any
	Status         PlanCommandStatus
	FailureReason  string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// PlanCommandStore persists recoverable signal/control commands.
type PlanCommandStore interface {
	RecordPlanCommand(ctx context.Context, command PlanCommandRecord) (PlanCommandRecord, bool, error)
	GetPlanCommand(ctx context.Context, idempotencyKey string) (PlanCommandRecord, bool, error)
	MarkPlanCommandDelivered(ctx context.Context, idempotencyKey string) (PlanCommandRecord, error)
	MarkPlanCommandFailed(ctx context.Context, idempotencyKey string, reason string) (PlanCommandRecord, error)
}

// Runner starts and controls backend-owned child runs.
type Runner interface {
	Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error)
	Signal(ctx context.Context, runID string, signal agentos.Signal) error
	Status(ctx context.Context, runID string) (agentos.RunStatus, error)
	Control(ctx context.Context, runID string, control agentos.ControlRequest) error
	Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error)
}
