// Package memory provides in-memory adapter implementations of usecase state ports.
// These are intended for testing and local development without infrastructure dependencies.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/entity"
)

// InMemoryRepository is a baseline persistence adapter for warm/cold state.
// It implements both repo.WarmStateRepo and repo.ColdStateRepo.
type InMemoryRepository struct {
	mu sync.RWMutex

	seq int64

	messages    map[string]entity.MessageRecord
	toolResults map[string]entity.ToolResultRecord
	archives    map[string]entity.ArchiveRecord
}

// NewInMemoryRepository creates a process-local persistence adapter.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		messages:    make(map[string]entity.MessageRecord),
		toolResults: make(map[string]entity.ToolResultRecord),
		archives:    make(map[string]entity.ArchiveRecord),
	}
}

func (r *InMemoryRepository) nextRef(prefix string) string {
	r.seq++

	return fmt.Sprintf("%s-%d", prefix, r.seq)
}

// PersistMessage stores message content and returns a warm reference id.
func (r *InMemoryRepository) PersistMessage(_ context.Context, record entity.MessageRecord) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ref := r.nextRef("msg")
	r.messages[ref] = record

	return ref, nil
}

// PersistToolResult stores normalized tool output and returns a warm reference id.
func (r *InMemoryRepository) PersistToolResult(_ context.Context, record entity.ToolResultRecord) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ref := r.nextRef("tool")
	r.toolResults[ref] = record

	return ref, nil
}

// GetMessage returns a stored message record by warm reference id.
func (r *InMemoryRepository) GetMessage(_ context.Context, ref string) (entity.MessageRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.messages[ref]

	return record, ok, nil
}

// GetToolResult returns a stored tool result by warm reference id.
func (r *InMemoryRepository) GetToolResult(_ context.Context, ref string) (entity.ToolResultRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.toolResults[ref]

	return record, ok, nil
}

// ListMessagesByRun returns all message records for a given run, ordered by insertion sequence.
func (r *InMemoryRepository) ListMessagesByRun(_ context.Context, runID string, _ uint64, _ uint64) ([]entity.MessageRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []entity.MessageRecord
	for _, m := range r.messages {
		if m.RunID == runID {
			result = append(result, m)
		}
	}

	return result, nil
}

// ListToolResultsByRun returns all tool result records for a given run.
func (r *InMemoryRepository) ListToolResultsByRun(_ context.Context, runID string, _ uint64, _ uint64) ([]entity.ToolResultRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []entity.ToolResultRecord
	for _, tr := range r.toolResults {
		if tr.RunID == runID {
			result = append(result, tr)
		}
	}

	return result, nil
}

// PersistArchive stores archival payload and returns a cold reference id.
func (r *InMemoryRepository) PersistArchive(_ context.Context, record entity.ArchiveRecord) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ref := r.nextRef("archive")
	r.archives[ref] = record

	return ref, nil
}

// GetArchive returns an archival record by cold reference id.
func (r *InMemoryRepository) GetArchive(_ context.Context, ref string) (entity.ArchiveRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.archives[ref]

	return record, ok, nil
}
