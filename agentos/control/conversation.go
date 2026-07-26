package control

import (
	"context"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

const ConversationSchemaVersion = "agentos.conversation.v1"

const (
	ConversationEventRunStarted         core.EventType = "RUN_STARTED"
	ConversationEventTextMessageStart   core.EventType = "TEXT_MESSAGE_START"
	ConversationEventTextMessageContent core.EventType = "TEXT_MESSAGE_CONTENT"
	ConversationEventTextMessageEnd     core.EventType = "TEXT_MESSAGE_END"
	ConversationEventRunFinished        core.EventType = "RUN_FINISHED"
	ConversationEventRunError           core.EventType = "RUN_ERROR"
)

const (
	ConversationRunPending     = "pending"
	ConversationRunRunning     = "running"
	ConversationRunCompleted   = "completed"
	ConversationRunInterrupted = "interrupted"
	ConversationRunCancelled   = "cancelled" //nolint:misspell // agentos.conversation.v1 uses this wire value.
	ConversationRunError       = "error"
)

const (
	ConversationOutcomeNormal    = "normal"
	ConversationOutcomeInterrupt = "interrupt"
	ConversationOutcomeCancelled = "cancelled" //nolint:misspell // agentos.conversation.v1 uses this wire value.
)

type ConversationRuntime interface {
	StartRun(context.Context, *StartConversationRunSpec) (ConversationRun, error)
	IngestEvent(context.Context, *ExternalConversationEvent) (ConversationEvent, error)
	GetThreadSnapshot(context.Context, ThreadScope) (ThreadSnapshot, error)
	SubscribeThread(context.Context, ThreadStreamScope) (core.Subscription, error)
	Close() error
}

type Attachment struct {
	FileID   string         `json:"file_id"`
	Filename string         `json:"filename"`
	Size     int64          `json:"size,omitempty"`
	MIMEType string         `json:"mime_type,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type ConversationMessage struct {
	MessageID   string         `json:"message_id"`
	ThreadID    string         `json:"thread_id"`
	RunID       string         `json:"run_id"`
	ProcessID   string         `json:"process_id,omitempty"`
	Role        string         `json:"role"`
	Content     string         `json:"content,omitempty"`
	Status      string         `json:"status"`
	Attachments []Attachment   `json:"attachments,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	CompletedAt time.Time      `json:"completed_at,omitzero" schema:"optional"`
}

type ConversationInterrupt struct {
	InterruptID string         `json:"interrupt_id"`
	Type        string         `json:"type"`
	Prompt      string         `json:"prompt"`
	InputSchema map[string]any `json:"input_schema,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type ConversationResume struct {
	InterruptID string `json:"interrupt_id"`
	Response    any    `json:"response"`
}

type ConversationRun struct {
	RunID       string                 `json:"run_id"`
	ThreadID    string                 `json:"thread_id"`
	ProcessID   string                 `json:"process_id,omitempty"`
	AccountID   string                 `json:"account_id"`
	ProjectID   string                 `json:"project_id"`
	Status      string                 `json:"status"`
	Outcome     string                 `json:"outcome,omitempty"`
	Interrupt   *ConversationInterrupt `json:"interrupt,omitempty"`
	ErrorCode   string                 `json:"error_code,omitempty"`
	Error       string                 `json:"error,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	StartedAt   time.Time              `json:"started_at,omitzero" schema:"optional"`
	CompletedAt time.Time              `json:"completed_at,omitzero" schema:"optional"`
}

type StartConversationRunSpec struct {
	RunID           string              `json:"run_id"`
	ThreadID        string              `json:"thread_id"`
	ProcessID       string              `json:"process_id,omitempty"`
	AccountID       string              `json:"account_id"`
	ProjectID       string              `json:"project_id"`
	MessageID       string              `json:"message_id"`
	UserMessage     string              `json:"user_message"`
	Attachments     []Attachment        `json:"attachments,omitempty"`
	MessageMetadata map[string]any      `json:"message_metadata,omitempty"`
	RunMetadata     map[string]any      `json:"run_metadata,omitempty"`
	Resume          *ConversationResume `json:"resume,omitempty"`
	IdempotencyKey  string              `json:"idempotency_key"`
	RequestedAt     time.Time           `json:"requested_at,omitzero" schema:"optional"`
}

type ExternalConversationEvent struct {
	ThreadID       string         `json:"thread_id"`
	RunID          string         `json:"run_id"`
	ProcessID      string         `json:"process_id,omitempty"`
	AccountID      string         `json:"account_id"`
	ProjectID      string         `json:"project_id"`
	SourceEventID  string         `json:"source_event_id"`
	SourceSequence int64          `json:"source_sequence"`
	EventType      core.EventType `json:"event_type"`
	OccurredAt     time.Time      `json:"occurred_at"`
	Payload        map[string]any `json:"payload,omitempty"`
}

type ConversationEvent struct {
	SchemaVersion  string         `json:"schema_version"`
	EventID        string         `json:"event_id"`
	ThreadID       string         `json:"thread_id"`
	RunID          string         `json:"run_id"`
	ProcessID      string         `json:"process_id,omitempty"`
	Sequence       int64          `json:"sequence"`
	SourceEventID  string         `json:"source_event_id,omitempty"`
	SourceSequence int64          `json:"source_sequence,omitempty"`
	EventType      core.EventType `json:"event_type"`
	OccurredAt     time.Time      `json:"occurred_at"`
	Payload        map[string]any `json:"payload,omitempty"`
}

type ThreadScope struct {
	ThreadID   string `json:"thread_id"`
	AccountID  string `json:"account_id"`
	ProjectID  string `json:"project_id"`
	EventLimit int    `json:"event_limit,omitempty"`
}

type ThreadStreamScope struct {
	ThreadID      string `json:"thread_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
}

type ThreadSnapshot struct {
	SchemaVersion string                `json:"schema_version"`
	ThreadID      string                `json:"thread_id"`
	AccountID     string                `json:"account_id"`
	ProjectID     string                `json:"project_id"`
	Messages      []ConversationMessage `json:"messages"`
	Runs          []ConversationRun     `json:"runs"`
	Events        []ConversationEvent   `json:"events,omitempty"`
	Cursor        int64                 `json:"cursor"`
	UpdatedAt     time.Time             `json:"updated_at"`
}
