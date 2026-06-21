package entity

import (
	"encoding/json"
	"time"
)

// AgentOSPlanRecord is the durable aggregate record for a RunPlan.
type AgentOSPlanRecord struct {
	PlanID         string
	ThreadID       string
	AccountID      string
	ProjectID      string
	IdempotencyKey string
	LifecycleState string
	Reason         string
	SpecJSON       json.RawMessage
	StatusJSON     json.RawMessage
	RequestedAt    time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// AgentOSPlanNodeRecord is the latest durable node state inside a RunPlan.
type AgentOSPlanNodeRecord struct {
	PlanID         string
	NodeID         string
	RunID          string
	BackendKind    string
	BackendName    string
	Capability     string
	LifecycleState string
	Attempts       int32
	Reason         string
	StatusJSON     json.RawMessage
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// AgentOSPlanEventRecord is one durable PlanEvent timeline entry.
type AgentOSPlanEventRecord struct {
	EventID        string
	PlanID         string
	NodeID         string
	RunID          string
	EventType      string
	Sequence       int64
	IdempotencyKey string
	PayloadJSON    json.RawMessage
	EventJSON      json.RawMessage
	Timestamp      time.Time
	CreatedAt      time.Time
}

// RunBackendIndexRecord stores durable run ownership for routing.
type RunBackendIndexRecord struct {
	RunID          string
	PlanID         string
	NodeID         string
	ThreadID       string
	AccountID      string
	ProjectID      string
	BackendKind    string
	BackendName    string
	IdempotencyKey string
	LifecycleState string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ArtifactRecord stores artifact metadata while payloads live in blob storage.
type ArtifactRecord struct {
	ArtifactID     string
	PlanID         string
	NodeID         string
	RunID          string
	Name           string
	Kind           string
	MediaType      string
	URI            string
	SizeBytes      int64
	Digest         string
	IdempotencyKey string
	Metadata       json.RawMessage
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// AuditLogRecord stores durable human/system control actions.
type AuditLogRecord struct {
	AuditID        string
	PlanID         string
	RunID          string
	NodeID         string
	ActorID        string
	Action         string
	IdempotencyKey string
	PayloadJSON    json.RawMessage
	CreatedAt      time.Time
}
