package state

import "context"

// MessageRecord is a warm-state message payload.
type MessageRecord struct {
	RunID   string
	Role    string
	Content string
}

// ToolResultRecord is a warm-state normalized tool output payload.
type ToolResultRecord struct {
	RunID      string
	ToolCallID string
	ToolName   string
	ResultJSON string
}

// ArchiveRecord is a cold-state archival payload descriptor.
type ArchiveRecord struct {
	RunID       string
	PayloadType string
	Content     []byte
}

// WarmStateStore persists operational state outside workflow history.
type WarmStateStore interface {
	PersistMessage(ctx context.Context, record MessageRecord) (string, error)
	PersistToolResult(ctx context.Context, record ToolResultRecord) (string, error)
	GetMessage(ctx context.Context, ref string) (MessageRecord, bool, error)
	GetToolResult(ctx context.Context, ref string) (ToolResultRecord, bool, error)
}

// ColdStateStore persists archival state outside workflow history.
type ColdStateStore interface {
	PersistArchive(ctx context.Context, record ArchiveRecord) (string, error)
	GetArchive(ctx context.Context, ref string) (ArchiveRecord, bool, error)
}

// Repository binds warm and cold adapters as boundary contracts for orchestration.
type Repository struct {
	Warm WarmStateStore
	Cold ColdStateStore
}
