package cached

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
)

var (
	ErrAgentNotFound        = errors.New("agent not found")
	ErrUnexpectedCacheEntry = errors.New("cache: unexpected entry type")
)

type agentCacheEntry struct {
	record entity.AgentRecord
	err    error
}

// AgentRepo is a caching decorator around repo.AgentRepo.
// It provides cache-first reads and delegates writes to the underlying repo.
type AgentRepo struct {
	inner repo.AgentRepo
	cache sync.Map
}

// NewAgentRepo creates a caching wrapper around the given AgentRepo.
func NewAgentRepo(inner repo.AgentRepo) *AgentRepo {
	return &AgentRepo{inner: inner}
}

// Get returns the agent record for the given ID, using cache when available.
func (r *AgentRepo) Get(ctx context.Context, agentID string) (entity.AgentRecord, bool, error) {
	if v, ok := r.cache.Load(agentID); ok {
		entry, ok := v.(agentCacheEntry)
		if !ok {
			return entity.AgentRecord{}, false, ErrUnexpectedCacheEntry
		}

		if entry.err != nil {
			return entity.AgentRecord{}, false, entry.err
		}

		return entry.record, true, nil
	}

	record, exists, err := r.inner.Get(ctx, agentID)
	if err != nil {
		r.cache.Store(agentID, agentCacheEntry{err: err})

		return entity.AgentRecord{}, false, fmt.Errorf("AgentRepo - Get: %w", err)
	}

	if !exists {
		r.cache.Store(agentID, agentCacheEntry{err: fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)})

		return entity.AgentRecord{}, false, nil
	}

	r.cache.Store(agentID, agentCacheEntry{record: record})

	return record, true, nil
}

// Create delegates to the inner repo and invalidates the cache for the new agent.
func (r *AgentRepo) Create(ctx context.Context, req *entity.CreateAgentRequest) (entity.AgentRecord, error) {
	record, err := r.inner.Create(ctx, req)
	if err == nil {
		r.cache.Delete(record.AgentID)
	}

	return record, err
}

// Update delegates to the inner repo and invalidates the cache.
func (r *AgentRepo) Update(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
	record, err := r.inner.Update(ctx, agentID, req)
	if err == nil {
		r.cache.Delete(agentID)
	}

	return record, err
}

// Delete delegates to the inner repo and invalidates the cache.
func (r *AgentRepo) Delete(ctx context.Context, agentID string) error {
	err := r.inner.Delete(ctx, agentID)
	if err == nil {
		r.cache.Delete(agentID)
	}

	return err
}

// ListByAccount delegates to the inner repo (no caching for list queries).
func (r *AgentRepo) ListByAccount(ctx context.Context, accountID string) ([]entity.AgentRecord, error) {
	return r.inner.ListByAccount(ctx, accountID)
}

// CreateVersion delegates to the inner repo.
func (r *AgentRepo) CreateVersion(ctx context.Context, record *entity.AgentVersionRecord) error {
	return r.inner.CreateVersion(ctx, record)
}

// GetVersion delegates to the inner repo.
func (r *AgentRepo) GetVersion(ctx context.Context, versionID string) (entity.AgentVersionRecord, bool, error) {
	return r.inner.GetVersion(ctx, versionID)
}

// ListVersions delegates to the inner repo.
func (r *AgentRepo) ListVersions(ctx context.Context, agentID string) ([]entity.AgentVersionRecord, error) {
	return r.inner.ListVersions(ctx, agentID)
}

// Invalidate removes the cached entry for the given agent ID.
func (r *AgentRepo) Invalidate(agentID string) {
	r.cache.Delete(agentID)
}

// InvalidateAll clears the entire cache.
func (r *AgentRepo) InvalidateAll() {
	r.cache.Clear()
}
