package eventing

import (
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// IngestEvent is the external backend event envelope accepted by AgentOS.
type IngestEvent struct {
	EventID   string                `json:"event_id"`
	RunID     string                `json:"run_id"`
	ThreadID  string                `json:"thread_id,omitempty"`
	Sequence  int64                 `json:"sequence,omitempty"`
	EventType agentoscore.EventType `json:"event_type"`
	Source    string                `json:"source"`
	Timestamp time.Time             `json:"timestamp"`
	TraceID   string                `json:"trace_id,omitempty"`
	Tags      map[string]string     `json:"tags,omitempty"`
	Payload   map[string]any        `json:"payload,omitempty"`
}

// IngestResult reports the authoritative AgentOS stream sequence.
type IngestResult struct {
	RunID     string `json:"run_id"`
	EventID   string `json:"event_id"`
	Sequence  int64  `json:"sequence"`
	Duplicate bool   `json:"duplicate"`
}
