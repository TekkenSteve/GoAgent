package state

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryRepository is a baseline persistence adapter for warm/cold state.
type InMemoryRepository struct {
	mu sync.RWMutex

	seq int64

	messages    map[string]MessageRecord
	toolResults map[string]ToolResultRecord
	archives    map[string]ArchiveRecord
}

// NewInMemoryRepository creates a process-local persistence adapter.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		messages:    make(map[string]MessageRecord),
		toolResults: make(map[string]ToolResultRecord),
		archives:    make(map[string]ArchiveRecord),
	}
}

func (r *InMemoryRepository) nextRef(prefix string) string {
	r.seq++
	return fmt.Sprintf("%s-%d", prefix, r.seq)
}

// PersistMessage stores message content and returns a warm reference id.
func (r *InMemoryRepository) PersistMessage(_ context.Context, record MessageRecord) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ref := r.nextRef("msg")
	r.messages[ref] = record
	return ref, nil
}

// PersistToolResult stores normalized tool output and returns a warm reference id.
func (r *InMemoryRepository) PersistToolResult(_ context.Context, record ToolResultRecord) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ref := r.nextRef("tool")
	r.toolResults[ref] = record
	return ref, nil
}

// GetMessage returns a stored message record by warm reference id.
func (r *InMemoryRepository) GetMessage(_ context.Context, ref string) (MessageRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.messages[ref]
	return record, ok, nil
}

// GetToolResult returns a stored tool result by warm reference id.
func (r *InMemoryRepository) GetToolResult(_ context.Context, ref string) (ToolResultRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.toolResults[ref]
	return record, ok, nil
}

// PersistArchive stores archival payload and returns a cold reference id.
func (r *InMemoryRepository) PersistArchive(_ context.Context, record ArchiveRecord) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ref := r.nextRef("archive")
	r.archives[ref] = record
	return ref, nil
}

// GetArchive returns an archival record by cold reference id.
func (r *InMemoryRepository) GetArchive(_ context.Context, ref string) (ArchiveRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.archives[ref]
	return record, ok, nil
}
