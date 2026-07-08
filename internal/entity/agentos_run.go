package entity

import "time"

// AgentOSRunRecord is the durable control-plane route and status record for an AgentOS run.
type AgentOSRunRecord struct {
	RunID              string
	ThreadID           string
	AccountID          string
	ProjectID          string
	BackendKind        string
	BackendName        string
	ExternalWorkflowID string
	ExternalRunID      string
	LifecycleState     string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
