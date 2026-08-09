package control

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// RunPlanSpec describes a cross-backend execution plan. Each node is a
// backend-owned RunSpec; backend-internal step/graph concepts are not public
// AgentOS contract.
type RunPlanSpec struct {
	PlanID         string            `json:"plan_id"`
	ThreadID       string            `json:"thread_id,omitempty"`
	AccountID      string            `json:"account_id"`
	ProjectID      string            `json:"project_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitzero" schema:"optional"`
	Inputs         map[string]any    `json:"inputs,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Nodes          []PlanNodeSpec    `json:"nodes"`
	Edges          []PlanEdgeSpec    `json:"edges,omitempty"`
	Policy         PlanPolicy        `json:"policy,omitzero" schema:"optional"`
}

// PlanRef identifies a plan inside an account/project boundary.
type PlanRef struct {
	PlanID    string `json:"plan_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// PlanNodeSpec describes one backend-owned child run in a RunPlan.
type PlanNodeSpec struct {
	NodeID     string         `json:"node_id"`
	Capability string         `json:"capability,omitempty"`
	Run        RunSpec        `json:"run"`
	Inputs     []InputMapping `json:"inputs,omitempty"`
	Outputs    []ArtifactSpec `json:"outputs,omitempty"`
	Conditions []string       `json:"conditions,omitempty"`
	Policy     NodePolicy     `json:"policy,omitzero" schema:"optional"`
}

// PlanEdgeSpec describes data/control dependency between backend-owned runs.
type PlanEdgeSpec struct {
	EdgeID       string         `json:"edge_id"`
	From         string         `json:"from"`
	To           string         `json:"to"`
	On           EdgeTrigger    `json:"on,omitempty"`
	Condition    string         `json:"condition,omitempty"`
	InputMapping []InputMapping `json:"input_mapping,omitempty"`
}

// PlanDeltaSpec is the public wire contract for workflow-owned dynamic
// expansion proposals. External callers may author or validate this artifact,
// but only PlanWorkflow may apply it to a running topology.
type PlanDeltaSpec struct {
	Nodes []PlanNodeSpec `json:"nodes,omitempty"`
	Edges []PlanEdgeSpec `json:"edges,omitempty"`
}

// EdgeTrigger selects which upstream node terminal state activates an edge.
type EdgeTrigger string

// EdgeTrigger values selecting the upstream terminal state that activates an edge.
const (
	EdgeOnSuccess  EdgeTrigger = "success"
	EdgeOnError    EdgeTrigger = "error"
	EdgeOnComplete EdgeTrigger = "complete"
	EdgeOnAlways   EdgeTrigger = "always"
)

// InputMapping maps plan inputs or upstream artifacts into a node input.
type InputMapping struct {
	Target         string `json:"target"`
	SourceNodeID   string `json:"source_node_id,omitempty"`
	SourceArtifact string `json:"source_artifact,omitempty"`
	SourcePath     string `json:"source_path,omitempty"`
	Expression     string `json:"expression,omitempty"`
	Required       bool   `json:"required,omitempty"`
}

// ArtifactSpec describes an output artifact contract for a plan node.
type ArtifactSpec struct {
	Name      string            `json:"name"`
	Kind      core.ArtifactKind `json:"kind"`
	MediaType string            `json:"media_type,omitempty"`
	SchemaRef string            `json:"schema_ref,omitempty"`
	Required  bool              `json:"required,omitempty"`
}

// ArtifactSchema declares a JSON Schema document addressable by ArtifactSpec.SchemaRef.
type ArtifactSchema struct {
	Ref         string          `json:"ref"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema"`
}

// ArtifactSchemaCatalogSpec is the public wire format for schema declarations
// referenced by ArtifactSpec.SchemaRef.
type ArtifactSchemaCatalogSpec struct {
	ArtifactSchemas []ArtifactSchema `json:"artifact_schemas"`
}

// PlanArtifactScope selects artifacts inside a tenant-scoped RunPlan.
type PlanArtifactScope struct {
	PlanID     string `json:"plan_id"`
	AccountID  string `json:"account_id"`
	ProjectID  string `json:"project_id"`
	NodeID     string `json:"node_id,omitempty"`
	RunID      string `json:"run_id,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// Capability describes a backend-owned execution capability.
type Capability struct {
	Backend      BackendRef              `json:"backend"`
	Name         string                  `json:"name"`
	Description  string                  `json:"description,omitempty"`
	InputSchema  json.RawMessage         `json:"input_schema,omitempty"`
	OutputSchema json.RawMessage         `json:"output_schema,omitempty"`
	Signals      []core.SignalType       `json:"signals,omitempty"`
	Controls     []core.ControlOperation `json:"controls,omitempty"`
	Streaming    bool                    `json:"streaming,omitempty"`
	Artifacts    []core.ArtifactKind     `json:"artifacts,omitempty"`
	Limits       CapabilityLimits        `json:"limits,omitzero" schema:"optional"`
}

// CapabilityLimits describes control-plane limits for one backend-owned
// capability. Batch limits constrain one coarse-grained backend run; AgentOS
// does not expand batch items into PlanNodeSpec values.
type CapabilityLimits struct {
	MaxBatchItems   int32  `json:"max_batch_items,omitempty"`
	MaxParallelRuns int32  `json:"max_parallel_runs,omitempty"`
	BatchInputKey   string `json:"batch_input_key,omitempty"`
}

// CapabilityCatalogSpec is the public wire format for backend capability
// declarations used by RunPlan validation.
type CapabilityCatalogSpec struct {
	Capabilities []Capability `json:"capabilities"`
}

// PlanPolicy constrains global plan execution and bounded expansion.
type PlanPolicy struct {
	MaxNodes            int32 `json:"max_nodes,omitempty"`
	MaxDepth            int32 `json:"max_depth,omitempty"`
	MaxExpansions       int32 `json:"max_expansions,omitempty"`
	MaxIterations       int32 `json:"max_iterations,omitempty"`
	MaxHistoryEvents    int32 `json:"max_history_events,omitempty"`
	ContinueAsNewEvents int32 `json:"continue_as_new_events,omitempty"`
	MaxParallelNodes    int32 `json:"max_parallel_nodes,omitempty"`
	BudgetCents         int64 `json:"budget_cents,omitempty"`
	TimeoutSeconds      int64 `json:"timeout_seconds,omitempty"`
	// ApprovalTimeoutSeconds bounds how long a plan may stay blocked waiting
	// for human approval (approve/reject signal). When it elapses the plan is
	// auto-rejected. 0 disables the gate (plan waits indefinitely).
	ApprovalTimeoutSeconds int64 `json:"approval_timeout_seconds,omitempty"`
}

// NodePolicy constrains one plan node.
type NodePolicy struct {
	MaxAttempts    int32            `json:"max_attempts,omitempty"`
	TimeoutSeconds int64            `json:"timeout_seconds,omitempty"`
	Join           PlanJoinStrategy `json:"join,omitempty"`
}

// PlanJoinStrategy controls how converging dependencies unblock a node.
type PlanJoinStrategy string

// PlanJoinStrategy values controlling how converging dependencies unblock a node.
const (
	PlanJoinAll   PlanJoinStrategy = "all"
	PlanJoinAny   PlanJoinStrategy = "any"
	PlanJoinFirst PlanJoinStrategy = "first"
)

// RunPlanStatus is the public aggregate lifecycle view for a RunPlan.
type RunPlanStatus struct {
	PlanID         string             `json:"plan_id"`
	LifecycleState string             `json:"lifecycle_state"`
	Nodes          []PlanNodeStatus   `json:"nodes,omitempty"`
	ActiveRunIDs   []string           `json:"active_run_ids,omitempty"`
	Artifacts      []core.ArtifactRef `json:"artifacts,omitempty"`
	Reason         string             `json:"reason,omitempty"`
	BudgetUsage    PlanBudgetUsage    `json:"budget_usage,omitzero" schema:"optional"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	StartedAt      time.Time          `json:"started_at,omitzero" schema:"optional"`
	UpdatedAt      time.Time          `json:"updated_at,omitzero" schema:"optional"`
	// BlockedAt records when the plan entered the blocked (awaiting-approval)
	// lifecycle state. It is zero outside the blocked state and anchors the
	// ApprovalTimeoutSeconds gate.
	BlockedAt time.Time `json:"blocked_at,omitzero" schema:"optional"`
	// Approval is the live approval-gate projection. It is nil while no gate
	// is active: a plan that is not blocked, or a blocked plan persisted before
	// the auditable-approval feature (the decision is then derived lazily).
	Approval *PlanApprovalStatus `json:"approval,omitempty"`
}

// PlanApprovalGate snapshots what a plan approval gate covers when the plan
// enters the blocked lifecycle. It is derived deterministically from the plan
// topology and policy so a later decision can be validated against the exact
// scope that was gated. The semantics mirror GovernedAction approval, extended
// with the plan-level policy version and expiry.
type PlanApprovalGate struct {
	// Summary describes what the gate covers (gated node count and plan id).
	Summary string `json:"summary,omitempty"`
	// NodeIDs are the precise plan-node references the gate covers: the nodes
	// that resume or start once the gate is approved.
	NodeIDs []string `json:"node_ids,omitempty"`
	// RiskReason is the rationale for requiring approval (why the plan paused).
	RiskReason string `json:"risk_reason,omitempty"`
	// PolicyVersion is a deterministic fingerprint of the plan policy and node
	// set at gate creation. It changes when the topology is replanned
	// (PlanDelta), which invalidates an existing decision.
	PolicyVersion string `json:"policy_version,omitempty"`
	// RequestedAt is when the gate was created (plan entered blocked).
	RequestedAt time.Time `json:"requested_at,omitzero" schema:"optional"`
	// ExpiresAt is when the gate auto-rejects. Zero means no expiry
	// (ApprovalTimeoutSeconds == 0).
	ExpiresAt time.Time `json:"expires_at,omitzero" schema:"optional"`
}

// PlanApprovalDecision records an approver's decision for one approval gate.
// It follows the GovernedAction decision shape (approved/actor/reason/decided
// at) and additionally records which policy version the decision validated.
type PlanApprovalDecision struct {
	Approved      bool      `json:"approved"`
	ActorID       string    `json:"actor_id,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	PolicyVersion string    `json:"policy_version,omitempty"`
	DecidedAt     time.Time `json:"decided_at,omitzero" schema:"optional"`
}

// PlanApprovalStatus is the live projection of a plan's approval gate.
// LifecycleState is empty only when Approval is nil (no gate active).
type PlanApprovalStatus struct {
	LifecycleState string                `json:"lifecycle_state,omitempty"`
	Gate           PlanApprovalGate      `json:"gate,omitzero" schema:"optional"`
	Decision       *PlanApprovalDecision `json:"decision,omitempty"`
}

// Plan approval gate lifecycle constants. "stale" marks a gate whose policy
// version no longer matches the active topology (the plan was replanned), so a
// fresh gate is required before the plan may resume.
const (
	PlanApprovalPending  = "pending"
	PlanApprovalApproved = "approved"
	PlanApprovalRejected = "rejected"
	PlanApprovalStale    = "stale"
)

// RunPlanDescription is the public, read-oriented view of a RunPlan. Topology
// is built from the latest durable plan snapshot, including workflow-owned
// dynamic expansion that has already been validated and persisted.
type RunPlanDescription struct {
	PlanID    string            `json:"plan_id"`
	ThreadID  string            `json:"thread_id,omitempty"`
	AccountID string            `json:"account_id,omitempty"`
	ProjectID string            `json:"project_id,omitempty"`
	Status    RunPlanStatus     `json:"status"`
	Topology  PlanTopology      `json:"topology"`
	Policy    PlanPolicy        `json:"policy,omitzero" schema:"optional"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	UpdatedAt time.Time         `json:"updated_at,omitzero" schema:"optional"`
}

// PlanTopology is a public graph view where each node is one backend-owned
// child run. Backend-internal steps or framework graph nodes are intentionally
// not represented here.
type PlanTopology struct {
	Nodes []PlanTopologyNode `json:"nodes"`
	Edges []PlanTopologyEdge `json:"edges,omitempty"`
	Order []string           `json:"order,omitempty"`
}

// PlanTopologyNode describes one backend-owned child run in the public graph.
type PlanTopologyNode struct {
	NodeID     string         `json:"node_id"`
	RunID      string         `json:"run_id,omitempty"`
	Backend    BackendRef     `json:"backend"`
	Capability string         `json:"capability,omitempty"`
	Conditions []string       `json:"conditions,omitempty"`
	Inputs     []InputMapping `json:"inputs,omitempty"`
	Outputs    []ArtifactSpec `json:"outputs,omitempty"`
	Policy     NodePolicy     `json:"policy,omitzero" schema:"optional"`
	Status     PlanNodeStatus `json:"status"`
}

// PlanTopologyEdge describes a dependency between backend-owned child runs.
type PlanTopologyEdge struct {
	EdgeID       string         `json:"edge_id,omitempty"`
	From         string         `json:"from"`
	To           string         `json:"to"`
	On           EdgeTrigger    `json:"on,omitempty"`
	Condition    string         `json:"condition,omitempty"`
	InputMapping []InputMapping `json:"input_mapping,omitempty"`
}

// PlanNodeStatus is the public lifecycle view for one plan node.
type PlanNodeStatus struct {
	NodeID         string             `json:"node_id"`
	RunID          string             `json:"run_id,omitempty"`
	Backend        BackendRef         `json:"backend"`
	LifecycleState string             `json:"lifecycle_state"`
	Attempts       int32              `json:"attempts,omitempty"`
	BudgetUsage    PlanBudgetUsage    `json:"budget_usage,omitzero" schema:"optional"`
	Reason         string             `json:"reason,omitempty"`
	Artifacts      []core.ArtifactRef `json:"artifacts,omitempty"`
	StartedAt      time.Time          `json:"started_at,omitzero" schema:"optional"`
	CompletedAt    time.Time          `json:"completed_at,omitzero" schema:"optional"`
	UpdatedAt      time.Time          `json:"updated_at,omitzero" schema:"optional"`
}

// PlanBudgetUsage reports plan-level resource consumption.
type PlanBudgetUsage struct {
	SpentCents int64 `json:"spent_cents,omitempty"`
}

// Plan lifecycle constants.
const (
	PlanLifecyclePending   = "pending"
	PlanLifecycleRunning   = "running"
	PlanLifecycleBlocked   = "blocked"
	PlanLifecycleSucceeded = "succeeded"
	PlanLifecycleFailed    = "failed"
	PlanLifecycleCanceled  = "canceled"
)

// Plan node lifecycle constants.
const (
	PlanNodePending   = "pending"
	PlanNodeReady     = "ready"
	PlanNodeRunning   = "running"
	PlanNodeBlocked   = "blocked"
	PlanNodeSkipped   = "skipped"
	PlanNodeSucceeded = "succeeded"
	PlanNodeFailed    = "failed"
	PlanNodeCanceled  = "canceled"
)

// PlanStreamScope selects events for a plan, node, or child run.
type PlanStreamScope struct {
	PlanID        string `json:"plan_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	NodeID        string `json:"node_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
}

// PlanEventScope selects durable plan events for timeline/debug queries.
type PlanEventScope struct {
	PlanID        string `json:"plan_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	NodeID        string `json:"node_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// PlanDebugTraceScope selects durable debug traces projected from PlanEvents.
type PlanDebugTraceScope struct {
	PlanID        string `json:"plan_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	NodeID        string `json:"node_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// PlanEvent is the public event envelope for plan-level events.
type PlanEvent struct {
	core.Event
	PlanID          string `json:"plan_id"`
	AccountID       string `json:"account_id"`
	ProjectID       string `json:"project_id"`
	NodeID          string `json:"node_id,omitempty"`
	ExternalEventID string `json:"external_event_id,omitempty"`
}

// ExternalPlanEvent is an event reported by a backend that executes a plan
// node outside the AgentOS process. Event.EventID is the backend's stable
// delivery identity and is used as the durable idempotency key.
type ExternalPlanEvent struct {
	Event  core.Event `json:"event"`
	Plan   PlanRef    `json:"plan"`
	NodeID string     `json:"node_id"`
}

// PlanDebugTrace is a typed debug projection over durable plan events. It keeps
// UI/debug clients away from raw event payload parsing.
type PlanDebugTrace struct {
	EventID         string                    `json:"event_id"`
	EventType       core.EventType            `json:"event_type"`
	PlanID          string                    `json:"plan_id"`
	NodeID          string                    `json:"node_id,omitempty"`
	RunID           string                    `json:"run_id,omitempty"`
	ThreadID        string                    `json:"thread_id,omitempty"`
	Sequence        int64                     `json:"sequence,omitempty"`
	Timestamp       time.Time                 `json:"timestamp"`
	Transition      *PlanStateTransition      `json:"transition,omitempty"`
	Capability      *PlanCapabilityTrace      `json:"capability,omitempty"`
	InputResolution *PlanInputResolutionTrace `json:"input_resolution,omitempty"`
	Conditions      []PlanConditionTrace      `json:"conditions,omitempty"`
}

// PlanStateTransition records a reducer lifecycle transition.
type PlanStateTransition struct {
	PreviousLifecycleState string `json:"previous_lifecycle_state,omitempty"`
	NextLifecycleState     string `json:"next_lifecycle_state,omitempty"`
}

// PlanCapabilityTrace records backend capability selected for one node.
type PlanCapabilityTrace struct {
	Backend         BackendRef              `json:"backend"`
	Capability      string                  `json:"capability"`
	Signals         []core.SignalType       `json:"signals,omitempty"`
	Controls        []core.ControlOperation `json:"controls,omitempty"`
	HasInputSchema  bool                    `json:"has_input_schema,omitempty"`
	HasOutputSchema bool                    `json:"has_output_schema,omitempty"`
}

// PlanInputResolutionTrace records redacted input mapping details.
type PlanInputResolutionTrace struct {
	InputDigest  string                  `json:"input_digest,omitempty"`
	InputKeys    []string                `json:"input_keys,omitempty"`
	MappingCount int                     `json:"mapping_count,omitempty"`
	Mappings     []PlanInputMappingTrace `json:"mappings,omitempty"`
}

// PlanInputMappingTrace records one input mapping rule without payload values.
type PlanInputMappingTrace struct {
	Target         string `json:"target"`
	SourceNodeID   string `json:"source_node_id,omitempty"`
	SourceArtifact string `json:"source_artifact,omitempty"`
	SourcePath     string `json:"source_path,omitempty"`
	Expression     string `json:"expression,omitempty"`
	Required       bool   `json:"required,omitempty"`
}

// PlanConditionTrace records one deterministic condition evaluation result.
type PlanConditionTrace struct {
	Scope       string      `json:"scope"`
	NodeID      string      `json:"node_id,omitempty"`
	EdgeID      string      `json:"edge_id,omitempty"`
	From        string      `json:"from,omitempty"`
	To          string      `json:"to,omitempty"`
	Expression  string      `json:"expression"`
	Result      bool        `json:"result"`
	On          EdgeTrigger `json:"on,omitempty"`
	ParentState string      `json:"parent_state,omitempty"`
}

// PlanAuditAction identifies durable control-plane actions.
type PlanAuditAction string

// PlanAuditAction values identifying durable control-plane actions.
const (
	PlanAuditActionStart   PlanAuditAction = "plan.start"
	PlanAuditActionSignal  PlanAuditAction = "plan.signal"
	PlanAuditActionControl PlanAuditAction = "plan.control"
)

// PlanAuditScope selects durable audit records for a plan.
type PlanAuditScope struct {
	PlanID    string          `json:"plan_id"`
	AccountID string          `json:"account_id"`
	ProjectID string          `json:"project_id"`
	NodeID    string          `json:"node_id,omitempty"`
	RunID     string          `json:"run_id,omitempty"`
	Action    PlanAuditAction `json:"action,omitempty"`
	Limit     int             `json:"limit,omitempty"`
}

// PlanAuditRecord is a durable audit entry for plan control-plane actions.
type PlanAuditRecord struct {
	AuditID        string          `json:"audit_id"`
	PlanID         string          `json:"plan_id"`
	AccountID      string          `json:"account_id,omitempty"`
	ProjectID      string          `json:"project_id,omitempty"`
	RunID          string          `json:"run_id,omitempty"`
	NodeID         string          `json:"node_id,omitempty"`
	ActorID        string          `json:"actor_id,omitempty"`
	Action         PlanAuditAction `json:"action"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        map[string]any  `json:"payload,omitempty"`
	CreatedAt      time.Time       `json:"created_at,omitzero" schema:"optional"`
}

// RunPlanSpecJSONSchema returns a JSON Schema inferred from RunPlanSpec.
func RunPlanSpecJSONSchema() ([]byte, error) {
	return core.JSONSchemaFor[RunPlanSpec]()
}

// PlanDeltaSpecJSONSchema returns a JSON Schema inferred from PlanDeltaSpec.
func PlanDeltaSpecJSONSchema() ([]byte, error) {
	return core.JSONSchemaFor[PlanDeltaSpec]()
}

// CapabilityCatalogSpecJSONSchema returns a JSON Schema inferred from
// CapabilityCatalogSpec.
func CapabilityCatalogSpecJSONSchema() ([]byte, error) {
	return core.JSONSchemaFor[CapabilityCatalogSpec]()
}

// ArtifactSchemaCatalogSpecJSONSchema returns a JSON Schema inferred from
// ArtifactSchemaCatalogSpec.
func ArtifactSchemaCatalogSpecJSONSchema() ([]byte, error) {
	return core.JSONSchemaFor[ArtifactSchemaCatalogSpec]()
}

// PlanSchemaKind identifies a public RunPlan authoring schema.
type PlanSchemaKind string

// PlanSchemaKind values identifying public RunPlan authoring schemas.
const (
	PlanSchemaKindRunPlan               PlanSchemaKind = "run-plan"
	PlanSchemaKindPlanDelta             PlanSchemaKind = "plan-delta"
	PlanSchemaKindCapabilityCatalog     PlanSchemaKind = "capability-catalog"
	PlanSchemaKindArtifactSchemaCatalog PlanSchemaKind = "artifact-schema-catalog"
)

// PlanSchemaKinds returns the complete set of public RunPlan authoring schema
// identifiers.
func PlanSchemaKinds() []PlanSchemaKind {
	return []PlanSchemaKind{
		PlanSchemaKindRunPlan,
		PlanSchemaKindPlanDelta,
		PlanSchemaKindCapabilityCatalog,
		PlanSchemaKindArtifactSchemaCatalog,
	}
}

// PlanJSONSchema returns the public JSON Schema for one RunPlan authoring
// contract.
func PlanJSONSchema(kind PlanSchemaKind) ([]byte, error) {
	switch kind {
	case PlanSchemaKindRunPlan:
		return RunPlanSpecJSONSchema()
	case PlanSchemaKindPlanDelta:
		return PlanDeltaSpecJSONSchema()
	case PlanSchemaKindCapabilityCatalog:
		return CapabilityCatalogSpecJSONSchema()
	case PlanSchemaKindArtifactSchemaCatalog:
		return ArtifactSchemaCatalogSpecJSONSchema()
	default:
		return nil, fmt.Errorf("%w: unsupported plan schema kind %q", core.ErrInvalidRunPlan, kind)
	}
}
