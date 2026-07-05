package request

import (
	"errors"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosprocess "github.com/TekkenSteve/GoAgent/agentos/process"
)

var errAgentOSTenantScopeRequired = errors.New("account_id and project_id are required")

// AgentOSStart starts a generic AgentOS run.
type AgentOSStart struct {
	RunID          string             `json:"run_id" validate:"required"`
	ThreadID       string             `json:"thread_id,omitempty"`
	AccountID      string             `json:"account_id" validate:"required"`
	ProjectID      string             `json:"project_id,omitempty"`
	AgentID        string             `json:"agent_id,omitempty"`
	ModelRef       string             `json:"model_ref,omitempty"`
	SystemPrompt   string             `json:"system_prompt,omitempty"`
	UserMessage    string             `json:"user_message,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time          `json:"requested_at,omitzero"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Backend        agentos.BackendRef `json:"backend" validate:"required"`
	Input          map[string]any     `json:"input,omitempty"`
}

// AgentOSSignal sends business input to a generic AgentOS run.
type AgentOSSignal struct {
	Type           agentoscore.SignalType `json:"type" validate:"required"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
	ActorID        string                 `json:"actor_id,omitempty"`
	Payload        map[string]any         `json:"payload,omitempty"`
	SentAt         time.Time              `json:"sent_at,omitzero"`
}

// AgentOSControl sends a lifecycle control operation to a generic AgentOS run.
type AgentOSControl struct {
	Operation      agentoscore.ControlOperation `json:"operation" validate:"required"`
	IdempotencyKey string                       `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                    `json:"requested_at,omitzero"`
	ActorID        string                       `json:"actor_id,omitempty"`
	Metadata       map[string]string            `json:"metadata,omitempty"`
}

// AgentOSPlanSignal sends business input to a RunPlan inside tenant scope.
type AgentOSPlanSignal struct {
	Type           agentoscore.SignalType `json:"type" validate:"required"`
	AccountID      string                 `json:"account_id" validate:"required"`
	ProjectID      string                 `json:"project_id" validate:"required"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
	ActorID        string                 `json:"actor_id,omitempty"`
	Payload        map[string]any         `json:"payload,omitempty"`
	SentAt         time.Time              `json:"sent_at,omitzero"`
}

func (r *AgentOSPlanSignal) GetAccountID() string {
	return r.AccountID
}

func (r *AgentOSPlanSignal) GetProjectID() string {
	return r.ProjectID
}

// AgentOSPlanControl sends a lifecycle control operation to a RunPlan inside tenant scope.
type AgentOSPlanControl struct {
	Operation      agentoscore.ControlOperation `json:"operation" validate:"required"`
	AccountID      string                       `json:"account_id" validate:"required"`
	ProjectID      string                       `json:"project_id" validate:"required"`
	IdempotencyKey string                       `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                    `json:"requested_at,omitzero"`
	ActorID        string                       `json:"actor_id,omitempty"`
	Metadata       map[string]string            `json:"metadata,omitempty"`
}

func (r *AgentOSPlanControl) GetAccountID() string {
	return r.AccountID
}

func (r *AgentOSPlanControl) GetProjectID() string {
	return r.ProjectID
}

// AgentOSPlanStreamScope selects plan events for REST streaming.
type AgentOSPlanStreamScope struct {
	AccountID     string `query:"account_id" validate:"required"`
	ProjectID     string `query:"project_id" validate:"required"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
}

func (r *AgentOSPlanStreamScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSPlanEventScope selects durable plan events for REST history queries.
type AgentOSPlanEventScope struct {
	AccountID     string `query:"account_id" validate:"required"`
	ProjectID     string `query:"project_id" validate:"required"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
	Limit         int    `query:"limit"`
}

func (r *AgentOSPlanEventScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSPlanDebugTraceScope selects durable plan debug traces for REST queries.
type AgentOSPlanDebugTraceScope struct {
	AccountID     string `query:"account_id" validate:"required"`
	ProjectID     string `query:"project_id" validate:"required"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
	Limit         int    `query:"limit"`
}

func (r *AgentOSPlanDebugTraceScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSPlanAuditScope selects plan audit records for REST queries.
type AgentOSPlanAuditScope struct {
	AccountID string                  `query:"account_id" validate:"required"`
	ProjectID string                  `query:"project_id" validate:"required"`
	NodeID    string                  `query:"node_id"`
	RunID     string                  `query:"run_id"`
	Action    agentos.PlanAuditAction `query:"action"`
	Limit     int                     `query:"limit"`
}

func (r *AgentOSPlanAuditScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSPlanArtifactScope selects plan artifacts for REST queries.
type AgentOSPlanArtifactScope struct {
	AccountID string `query:"account_id" validate:"required"`
	ProjectID string `query:"project_id" validate:"required"`
	NodeID    string `query:"node_id"`
	RunID     string `query:"run_id"`
	Limit     int    `query:"limit"`
}

func (r *AgentOSPlanArtifactScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSPlanScope selects one plan inside a tenant boundary.
type AgentOSPlanScope struct {
	AccountID string `query:"account_id" validate:"required"`
	ProjectID string `query:"project_id" validate:"required"`
}

func (r *AgentOSPlanScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSPlanConsoleScope selects plan data for the operator console.
type AgentOSPlanConsoleScope struct {
	AccountID     string `query:"account_id" validate:"required"`
	ProjectID     string `query:"project_id" validate:"required"`
	EventLimit    int    `query:"event_limit"`
	AuditLimit    int    `query:"audit_limit"`
	ArtifactLimit int    `query:"artifact_limit"`
}

func (r *AgentOSPlanConsoleScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSEvent is the public REST envelope for external backend event ingest.
type AgentOSEvent struct {
	EventID   string                `json:"event_id" validate:"required"`
	RunID     string                `json:"run_id,omitempty"`
	ThreadID  string                `json:"thread_id,omitempty"`
	Sequence  int64                 `json:"sequence,omitempty"`
	EventType agentoscore.EventType `json:"event_type" validate:"required"`
	Source    string                `json:"source" validate:"required"`
	Timestamp time.Time             `json:"timestamp"`
	TraceID   string                `json:"trace_id,omitempty"`
	Tags      map[string]string     `json:"tags,omitempty"`
	Payload   map[string]any        `json:"payload,omitempty"`
}

// AgentOSProcessScope selects durable process projections.
type AgentOSProcessScope struct {
	AccountID      string                      `query:"account_id" validate:"required"`
	ProjectID      string                      `query:"project_id" validate:"required"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	Kind           agentosprocess.Kind         `query:"kind"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

func (r *AgentOSProcessScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSProcessRef selects one durable process inside tenant scope.
type AgentOSProcessRef struct {
	AccountID string `query:"account_id" validate:"required"`
	ProjectID string `query:"project_id" validate:"required"`
}

func (r *AgentOSProcessRef) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSLedgerScope selects ledger entries for a tenant, process, resource, or kind.
type AgentOSLedgerScope struct {
	AccountID     string                         `query:"account_id" validate:"required"`
	ProjectID     string                         `query:"project_id" validate:"required"`
	ProcessID     string                         `query:"process_id"`
	ResourceKind  agentosprocess.ResourceKind    `query:"resource_kind"`
	ResourceID    string                         `query:"resource_id"`
	Kind          agentosprocess.LedgerEntryKind `query:"kind"`
	AfterSequence int64                          `query:"after_sequence"`
	Limit         int                            `query:"limit"`
}

func (r *AgentOSLedgerScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSActionScope selects governed actions for a tenant, process, resource, kind, or lifecycle state.
type AgentOSActionScope struct {
	AccountID      string                      `query:"account_id" validate:"required"`
	ProjectID      string                      `query:"project_id" validate:"required"`
	ProcessID      string                      `query:"process_id"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	Kind           agentosprocess.ActionKind   `query:"kind"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

func (r *AgentOSActionScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSActionRef selects one governed action inside tenant scope.
type AgentOSActionRef struct {
	AccountID string `query:"account_id" validate:"required"`
	ProjectID string `query:"project_id" validate:"required"`
}

func (r *AgentOSActionRef) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSWorksetScope selects worksets for a tenant, process, resource, kind, or lifecycle state.
type AgentOSWorksetScope struct {
	AccountID      string                      `query:"account_id" validate:"required"`
	ProjectID      string                      `query:"project_id" validate:"required"`
	ProcessID      string                      `query:"process_id"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	Kind           agentosprocess.WorksetKind  `query:"kind"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

func (r *AgentOSWorksetScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSWorksetRef selects one workset inside tenant scope.
type AgentOSWorksetRef struct {
	AccountID string `query:"account_id" validate:"required"`
	ProjectID string `query:"project_id" validate:"required"`
}

func (r *AgentOSWorksetRef) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

// AgentOSResourceScope selects one resource projection or a resource projection list.
type AgentOSResourceScope struct {
	AccountID      string                      `query:"account_id" validate:"required"`
	ProjectID      string                      `query:"project_id" validate:"required"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

func (r *AgentOSResourceScope) Validate() error {
	return validateAgentOSTenantScope(r.AccountID, r.ProjectID)
}

func validateAgentOSTenantScope(accountID, projectID string) error {
	if accountID == "" || projectID == "" {
		return errAgentOSTenantScopeRequired
	}

	return nil
}
