package agentosplan

import (
	"context"
	"encoding/json"
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

// ArtifactStore stores artifacts outside workflow history. Reads must carry the
// full account/project plan scope because artifact IDs are not a tenant
// boundary.
type ArtifactStore interface {
	Put(ctx context.Context, artifact agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error)
	Get(ctx context.Context, scope agentos.PlanArtifactScope) (agentos.ArtifactRef, any, error)
	List(ctx context.Context, scope agentos.PlanArtifactScope) ([]agentos.ArtifactRef, error)
}

// ArtifactSchemaCatalog resolves JSON Schemas referenced by ArtifactSpec.
type ArtifactSchemaCatalog interface {
	GetArtifactSchema(ctx context.Context, schemaRef string) (json.RawMessage, bool, error)
}

// ArtifactSchemaRegistry persists schema declarations referenced by RunPlan
// artifact contracts.
type ArtifactSchemaRegistry interface {
	RegisterArtifactSchema(ctx context.Context, schema agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, bool, error)
}

// PlanIndex stores durable plan identity and the latest aggregate status.
type PlanIndex interface {
	CreatePlan(ctx context.Context, spec agentos.RunPlanSpec, status agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error)
	GetPlanByRef(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error)
	GetPlan(ctx context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error)
}

// PlanRefScope selects durable plans for internal control-plane projectors.
type PlanRefScope struct {
	AccountID       string
	ProjectID       string
	LifecycleStates []string
	UpdatedAfter    time.Time
	Limit           int
}

// PlanRefStore lists durable plan identities without exposing implementation
// tables to projectors.
type PlanRefStore interface {
	ListPlanRefs(ctx context.Context, scope PlanRefScope) ([]agentos.PlanRef, error)
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

// PlanEventStore is the durable event source for RunPlan timelines. Reads must
// carry the full account/project plan scope; PlanID alone is not a production
// isolation boundary.
type PlanEventStore interface {
	AppendPlanEvent(ctx context.Context, event agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error)
	ListPlanEvents(ctx context.Context, scope agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error)
}

// PlanMetricCheckpoint stores the exporter-owned cursor and projection state
// for one durable RunPlan event stream.
type PlanMetricCheckpoint struct {
	ExporterID string
	PlanID     string
	AccountID  string
	ProjectID  string
	Sequence   int64
	Projection PlanMetricProjectionState
	UpdatedAt  time.Time
}

// PlanMetricCheckpointStore persists metric projection checkpoints.
type PlanMetricCheckpointStore interface {
	GetPlanMetricCheckpoint(ctx context.Context, exporterID string, ref agentos.PlanRef) (PlanMetricCheckpoint, bool, error)
	SavePlanMetricCheckpoint(ctx context.Context, checkpoint PlanMetricCheckpoint) error
}

// PlanMetricsSink receives idempotent durable metric samples. Implementations
// must de-duplicate by PlanMetricSample.Key before mutating non-idempotent
// counters or external metrics systems.
type PlanMetricsSink interface {
	RecordPlanMetric(ctx context.Context, sample PlanMetricSample) error
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
	AccountID      string
	ProjectID      string
	RunID          string
	NodeID         string
	ActorID        string
	Action         AuditAction
	IdempotencyKey string
	Payload        map[string]any
	CreatedAt      time.Time
}

// AuditRef identifies one idempotent audit record inside a tenant-scoped plan.
type AuditRef struct {
	PlanID         string
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

func AuditRefFromRecord(record AuditRecord) AuditRef {
	return AuditRef{
		PlanID:         record.PlanID,
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		IdempotencyKey: record.IdempotencyKey,
	}
}

// AuditStore persists idempotent control-plane audit records. List operations
// must carry the full account/project plan scope.
type AuditStore interface {
	RecordAudit(ctx context.Context, record AuditRecord) (AuditRecord, bool, error)
	GetAuditRecord(ctx context.Context, ref AuditRef) (AuditRecord, bool, error)
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
// delivering a control-plane operation to PlanWorkflow or starting it.
type PlanCommandRecord struct {
	CommandID      string
	PlanID         string
	AccountID      string
	ProjectID      string
	ActorID        string
	Action         AuditAction
	IdempotencyKey string
	Payload        map[string]any
	Status         PlanCommandStatus
	FailureReason  string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// PlanCommandRef identifies one durable command in a tenant-scoped plan.
type PlanCommandRef struct {
	PlanID         string
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

func PlanCommandRefFromRecord(command PlanCommandRecord) PlanCommandRef {
	return PlanCommandRef{
		PlanID:         command.PlanID,
		AccountID:      command.AccountID,
		ProjectID:      command.ProjectID,
		IdempotencyKey: command.IdempotencyKey,
	}
}

// PlanCommandScope selects recoverable command outbox entries.
type PlanCommandScope struct {
	PlanID   string
	Action   AuditAction
	Statuses []PlanCommandStatus
	Limit    int
}

// PlanCommandStore persists recoverable plan start/signal/control commands.
type PlanCommandStore interface {
	RecordPlanCommand(ctx context.Context, command PlanCommandRecord) (PlanCommandRecord, bool, error)
	GetPlanCommand(ctx context.Context, ref PlanCommandRef) (PlanCommandRecord, bool, error)
	ListRecoverablePlanCommands(ctx context.Context, scope PlanCommandScope) ([]PlanCommandRecord, error)
	MarkPlanCommandDelivered(ctx context.Context, ref PlanCommandRef) (PlanCommandRecord, error)
	MarkPlanCommandFailed(ctx context.Context, ref PlanCommandRef, reason string) (PlanCommandRecord, error)
}

// Runner starts and controls backend-owned child runs.
type Runner interface {
	Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error)
	Signal(ctx context.Context, runID string, signal agentos.Signal) error
	Status(ctx context.Context, runID string) (agentos.RunStatus, error)
	Control(ctx context.Context, runID string, control agentos.ControlRequest) error
	Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error)
}
