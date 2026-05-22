// Package state provides the foundational state management abstraction for the agent domain.
//
// It defines minimal interfaces (WarmStateStore, ColdStateStore) that capture the
// hot/warm/cold state layering pattern used by Temporal workflows. Broader repository
// interfaces that extend these are defined in the usecase package.
//
// This package depends only on entity/ — it belongs to the inner (business logic) layer.
package state

import (
	"context"

	"github.com/TekkenSteve/GoAgent/entity"
)

// WarmStateStore persists and queries operational state outside workflow history.
// This is the minimal interface; repo.WarmStateRepo extends it with list operations.
type WarmStateStore interface {
	PersistMessage(ctx context.Context, record entity.MessageRecord) (string, error)
	PersistToolResult(ctx context.Context, record entity.ToolResultRecord) (string, error)
	GetMessage(ctx context.Context, ref string) (entity.MessageRecord, bool, error)
	GetToolResult(ctx context.Context, ref string) (entity.ToolResultRecord, bool, error)
}

// ColdStateStore persists archival state outside workflow history.
type ColdStateStore interface {
	PersistArchive(ctx context.Context, record entity.ArchiveRecord) (string, error)
	GetArchive(ctx context.Context, ref string) (entity.ArchiveRecord, bool, error)
}

// Repository binds warm and cold adapters as boundary contracts for orchestration.
type Repository struct {
	Warm WarmStateStore
	Cold ColdStateStore
}
