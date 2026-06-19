package agentos

import (
	"encoding/json"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

// RunPlanSpec describes a cross-backend execution plan. Each node is a
// backend-owned RunSpec; backend-internal step/graph concepts are not public
// AgentOS contract.
type RunPlanSpec struct {
	PlanID         string            `json:"plan_id"`
	ThreadID       string            `json:"thread_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	ProjectID      string            `json:"project_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitempty"`
	Inputs         map[string]any    `json:"inputs,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Nodes          []PlanNodeSpec    `json:"nodes"`
	Edges          []PlanEdgeSpec    `json:"edges,omitempty"`
	Policy         PlanPolicy        `json:"policy,omitempty"`
}

// PlanRef identifies a plan inside an account/project boundary.
type PlanRef struct {
	PlanID    string `json:"plan_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id,omitempty"`
}

// PlanNodeSpec describes one backend-owned child run in a RunPlan.
type PlanNodeSpec struct {
	NodeID     string         `json:"node_id"`
	Capability string         `json:"capability,omitempty"`
	Run        RunSpec        `json:"run"`
	Inputs     []InputMapping `json:"inputs,omitempty"`
	Outputs    []ArtifactSpec `json:"outputs,omitempty"`
	Conditions []string       `json:"conditions,omitempty"`
	Policy     NodePolicy     `json:"policy,omitempty"`
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
	Name      string       `json:"name"`
	Kind      ArtifactKind `json:"kind"`
	MediaType string       `json:"media_type,omitempty"`
	SchemaRef string       `json:"schema_ref,omitempty"`
	Required  bool         `json:"required,omitempty"`
}

// ArtifactKind identifies public artifact payload categories.
type ArtifactKind string

const (
	ArtifactKindObject    ArtifactKind = "object"
	ArtifactKindText      ArtifactKind = "text"
	ArtifactKindFile      ArtifactKind = "file"
	ArtifactKindPatch     ArtifactKind = "patch"
	ArtifactKindReport    ArtifactKind = "report"
	ArtifactKindReference ArtifactKind = "reference"
	ArtifactKindPlanDelta ArtifactKind = "plan_delta"
)

// ArtifactRef points to an artifact outside Temporal workflow history.
type ArtifactRef struct {
	ArtifactID string            `json:"artifact_id"`
	PlanID     string            `json:"plan_id,omitempty"`
	NodeID     string            `json:"node_id,omitempty"`
	RunID      string            `json:"run_id,omitempty"`
	Name       string            `json:"name"`
	Kind       ArtifactKind      `json:"kind"`
	MediaType  string            `json:"media_type,omitempty"`
	URI        string            `json:"uri,omitempty"`
	SizeBytes  int64             `json:"size_bytes,omitempty"`
	Digest     string            `json:"digest,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at,omitempty"`
}

// Artifact is the public artifact document returned by ArtifactStore-backed
// control-plane APIs. Payloads are never embedded in Temporal workflow history.
type Artifact struct {
	Ref     ArtifactRef `json:"ref"`
	Payload any         `json:"payload,omitempty"`
}

// PlanArtifactScope selects artifacts inside a tenant-scoped RunPlan.
type PlanArtifactScope struct {
	PlanID     string `json:"plan_id"`
	AccountID  string `json:"account_id"`
	ProjectID  string `json:"project_id,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	RunID      string `json:"run_id,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// Capability describes a backend-owned execution capability.
type Capability struct {
	Backend      BackendRef         `json:"backend"`
	Name         string             `json:"name"`
	Description  string             `json:"description,omitempty"`
	InputSchema  json.RawMessage    `json:"input_schema,omitempty"`
	OutputSchema json.RawMessage    `json:"output_schema,omitempty"`
	Signals      []SignalType       `json:"signals,omitempty"`
	Controls     []ControlOperation `json:"controls,omitempty"`
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
}

// NodePolicy constrains one plan node.
type NodePolicy struct {
	MaxAttempts    int32            `json:"max_attempts,omitempty"`
	TimeoutSeconds int64            `json:"timeout_seconds,omitempty"`
	Join           PlanJoinStrategy `json:"join,omitempty"`
}

// PlanJoinStrategy controls how converging dependencies unblock a node.
type PlanJoinStrategy string

const (
	PlanJoinAll   PlanJoinStrategy = "all"
	PlanJoinAny   PlanJoinStrategy = "any"
	PlanJoinFirst PlanJoinStrategy = "first"
)

// RunPlanStatus is the public aggregate lifecycle view for a RunPlan.
type RunPlanStatus struct {
	PlanID         string            `json:"plan_id"`
	LifecycleState string            `json:"lifecycle_state"`
	Nodes          []PlanNodeStatus  `json:"nodes,omitempty"`
	ActiveRunIDs   []string          `json:"active_run_ids,omitempty"`
	Artifacts      []ArtifactRef     `json:"artifacts,omitempty"`
	Reason         string            `json:"reason,omitempty"`
	BudgetUsage    PlanBudgetUsage   `json:"budget_usage,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	UpdatedAt      time.Time         `json:"updated_at,omitempty"`
}

// PlanNodeStatus is the public lifecycle view for one plan node.
type PlanNodeStatus struct {
	NodeID         string          `json:"node_id"`
	RunID          string          `json:"run_id,omitempty"`
	Backend        BackendRef      `json:"backend"`
	LifecycleState string          `json:"lifecycle_state"`
	Attempts       int32           `json:"attempts,omitempty"`
	BudgetUsage    PlanBudgetUsage `json:"budget_usage,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	Artifacts      []ArtifactRef   `json:"artifacts,omitempty"`
	StartedAt      time.Time       `json:"started_at,omitempty"`
	CompletedAt    time.Time       `json:"completed_at,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at,omitempty"`
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
	ProjectID     string `json:"project_id,omitempty"`
	NodeID        string `json:"node_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
}

// PlanEventScope selects durable plan events for timeline/debug queries.
type PlanEventScope struct {
	PlanID        string `json:"plan_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id,omitempty"`
	NodeID        string `json:"node_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// PlanEvent is the public event envelope for plan-level events.
type PlanEvent struct {
	Event
	PlanID string `json:"plan_id"`
	NodeID string `json:"node_id,omitempty"`
}

// PlanAuditAction identifies durable control-plane actions.
type PlanAuditAction string

const (
	PlanAuditActionStart   PlanAuditAction = "plan.start"
	PlanAuditActionSignal  PlanAuditAction = "plan.signal"
	PlanAuditActionControl PlanAuditAction = "plan.control"
)

// PlanAuditScope selects durable audit records for a plan.
type PlanAuditScope struct {
	PlanID    string          `json:"plan_id"`
	AccountID string          `json:"account_id"`
	ProjectID string          `json:"project_id,omitempty"`
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
	CreatedAt      time.Time       `json:"created_at,omitempty"`
}

// RunPlanSpecJSONSchema returns a JSON Schema inferred from RunPlanSpec.
func RunPlanSpecJSONSchema() ([]byte, error) {
	schema, err := jsonschema.For[RunPlanSpec](nil)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(schema, "", "  ")
}

// PlanDeltaSpecJSONSchema returns a JSON Schema inferred from PlanDeltaSpec.
func PlanDeltaSpecJSONSchema() ([]byte, error) {
	schema, err := jsonschema.For[PlanDeltaSpec](nil)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(schema, "", "  ")
}
