// Package request defines the v1 API request payloads.
package request

import (
	"errors"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosprocess "github.com/TekkenSteve/GoAgent/agentos/process"
)

// errAgentOSProjectRequired reports a request that names no project. The account
// half is never a request field: it comes from the credential.
var errAgentOSProjectRequired = errors.New("project_id is required")

// AgentOSStart starts a generic AgentOS run.
//
// The tenant's account is not a request field: it comes from the authenticated
// principal. Only the project is named by the caller, and the pair is
// authorized before the run starts.
type AgentOSStart struct {
	RunID          string             `json:"run_id" validate:"required"`
	ThreadID       string             `json:"thread_id,omitempty"`
	ProjectID      string             `json:"project_id" validate:"required"`
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
	Payload        map[string]any         `json:"payload,omitempty"`
	SentAt         time.Time              `json:"sent_at,omitzero"`
}

// AgentOSControl sends a lifecycle control operation to a generic AgentOS run.
type AgentOSControl struct {
	Operation      agentoscore.ControlOperation `json:"operation" validate:"required"`
	IdempotencyKey string                       `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                    `json:"requested_at,omitzero"`
	Metadata       map[string]string            `json:"metadata,omitempty"`
}

// AgentOSPlanSignal sends business input to a RunPlan inside tenant scope.
type AgentOSPlanSignal struct {
	Type           agentoscore.SignalType `json:"type" validate:"required"`
	ProjectID      string                 `json:"project_id" validate:"required"`
	ActorID        string                 `json:"actor_id,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
	Payload        map[string]any         `json:"payload,omitempty"`
	SentAt         time.Time              `json:"sent_at,omitzero"`
}

// GetProjectID returns the tenant project ID of the plan signal.
func (r *AgentOSPlanSignal) GetProjectID() string {
	return r.ProjectID
}

// AgentOSPlanControl sends a lifecycle control operation to a RunPlan inside tenant scope.
type AgentOSPlanControl struct {
	Operation      agentoscore.ControlOperation `json:"operation" validate:"required"`
	ProjectID      string                       `json:"project_id" validate:"required"`
	ActorID        string                       `json:"actor_id,omitempty"`
	IdempotencyKey string                       `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                    `json:"requested_at,omitzero"`
	Metadata       map[string]string            `json:"metadata,omitempty"`
}

// GetProjectID returns the tenant project ID of the plan control operation.
func (r *AgentOSPlanControl) GetProjectID() string {
	return r.ProjectID
}

// AgentOSPlanStreamScope selects plan events for REST streaming.
type AgentOSPlanStreamScope struct {
	ProjectID     string `query:"project_id" validate:"required"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanStreamScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the stream scope carries the required tenant account and project IDs.
func (r *AgentOSPlanStreamScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSPlanEventScope selects durable plan events for REST history queries.
type AgentOSPlanEventScope struct {
	ProjectID     string `query:"project_id" validate:"required"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
	Limit         int    `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanEventScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the event scope carries the required tenant account and project IDs.
func (r *AgentOSPlanEventScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSPlanDebugTraceScope selects durable plan debug traces for REST queries.
type AgentOSPlanDebugTraceScope struct {
	ProjectID     string `query:"project_id" validate:"required"`
	NodeID        string `query:"node_id"`
	RunID         string `query:"run_id"`
	AfterSequence int64  `query:"after_sequence"`
	Limit         int    `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanDebugTraceScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the debug trace scope carries the required tenant account and project IDs.
func (r *AgentOSPlanDebugTraceScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSPlanAuditScope selects plan audit records for REST queries.
type AgentOSPlanAuditScope struct {
	ProjectID string                  `query:"project_id" validate:"required"`
	NodeID    string                  `query:"node_id"`
	RunID     string                  `query:"run_id"`
	Action    agentos.PlanAuditAction `query:"action"`
	Limit     int                     `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanAuditScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the audit scope carries the required tenant account and project IDs.
func (r *AgentOSPlanAuditScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSPlanArtifactScope selects plan artifacts for REST queries.
type AgentOSPlanArtifactScope struct {
	ProjectID string `query:"project_id" validate:"required"`
	NodeID    string `query:"node_id"`
	RunID     string `query:"run_id"`
	Limit     int    `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanArtifactScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the artifact scope carries the required tenant account and project IDs.
func (r *AgentOSPlanArtifactScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSPlanScope selects one plan inside a tenant boundary.
type AgentOSPlanScope struct {
	ProjectID string `query:"project_id" validate:"required"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the plan scope carries the required tenant account and project IDs.
func (r *AgentOSPlanScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSPlanConsoleScope selects plan data for the operator console.
type AgentOSPlanConsoleScope struct {
	ProjectID     string `query:"project_id" validate:"required"`
	EventLimit    int    `query:"event_limit"`
	AuditLimit    int    `query:"audit_limit"`
	ArtifactLimit int    `query:"artifact_limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSPlanConsoleScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the console scope carries the required tenant account and project IDs.
func (r *AgentOSPlanConsoleScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSRunEventScope selects a run's durable event history for REST queries.
type AgentOSRunEventScope struct {
	AfterSequence int64 `query:"after_sequence"`
	Limit         int   `query:"limit"`
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
	ProjectID      string                      `query:"project_id" validate:"required"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	Kind           agentosprocess.Kind         `query:"kind"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSProcessScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the process scope carries the required tenant account and project IDs.
func (r *AgentOSProcessScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSProcessRef selects one durable process inside tenant scope.
type AgentOSProcessRef struct {
	ProjectID string `query:"project_id" validate:"required"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSProcessRef) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the process ref carries the required tenant account and project IDs.
func (r *AgentOSProcessRef) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSLedgerScope selects ledger entries for a tenant, process, resource, or kind.
type AgentOSLedgerScope struct {
	ProjectID     string                         `query:"project_id" validate:"required"`
	ProcessID     string                         `query:"process_id"`
	ResourceKind  agentosprocess.ResourceKind    `query:"resource_kind"`
	ResourceID    string                         `query:"resource_id"`
	Kind          agentosprocess.LedgerEntryKind `query:"kind"`
	AfterSequence int64                          `query:"after_sequence"`
	Limit         int                            `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSLedgerScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the ledger scope carries the required tenant account and project IDs.
func (r *AgentOSLedgerScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSActionScope selects governed actions for a tenant, process, resource, kind, or lifecycle state.
type AgentOSActionScope struct {
	ProjectID      string                      `query:"project_id" validate:"required"`
	ProcessID      string                      `query:"process_id"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	Kind           agentosprocess.ActionKind   `query:"kind"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSActionScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the action scope carries the required tenant account and project IDs.
func (r *AgentOSActionScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSActionRef selects one governed action inside tenant scope.
type AgentOSActionRef struct {
	ProjectID string `query:"project_id" validate:"required"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSActionRef) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the action ref carries the required tenant account and project IDs.
func (r *AgentOSActionRef) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSWorksetScope selects worksets for a tenant, process, resource, kind, or lifecycle state.
type AgentOSWorksetScope struct {
	ProjectID      string                      `query:"project_id" validate:"required"`
	ProcessID      string                      `query:"process_id"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	Kind           agentosprocess.WorksetKind  `query:"kind"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSWorksetScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the workset scope carries the required tenant account and project IDs.
func (r *AgentOSWorksetScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSWorksetRef selects one workset inside tenant scope.
type AgentOSWorksetRef struct {
	ProjectID string `query:"project_id" validate:"required"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSWorksetRef) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the workset ref carries the required tenant account and project IDs.
func (r *AgentOSWorksetRef) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// AgentOSResourceScope selects one resource projection or a resource projection list.
type AgentOSResourceScope struct {
	ProjectID      string                      `query:"project_id" validate:"required"`
	ResourceKind   agentosprocess.ResourceKind `query:"resource_kind"`
	ResourceID     string                      `query:"resource_id"`
	LifecycleState string                      `query:"lifecycle_state"`
	Limit          int                         `query:"limit"`
}

// GetProjectID returns the project the request scopes itself to.
func (r *AgentOSResourceScope) GetProjectID() string {
	return r.ProjectID
}

// Validate checks that the resource scope carries the required tenant account and project IDs.
func (r *AgentOSResourceScope) Validate() error {
	return validateAgentOSProjectScope(r.ProjectID)
}

// validateAgentOSProjectScope checks the resource dimension a plan request
// names. The account half is not a request field: it comes from the credential
// (see internal/controller/restapi/v1/tenant.go).
func validateAgentOSProjectScope(projectID string) error {
	if projectID == "" {
		return errAgentOSProjectRequired
	}

	return nil
}
