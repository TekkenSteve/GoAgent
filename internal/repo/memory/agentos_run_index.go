package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

// AgentOSRunIndex is a process-local AgentOS run route index.
type AgentOSRunIndex struct {
	mu      sync.RWMutex
	records map[string]entity.RunBackendIndexRecord
	keys    map[string]string
}

// NewAgentOSRunIndex creates an empty in-memory AgentOS run route index.
func NewAgentOSRunIndex() *AgentOSRunIndex {
	return &AgentOSRunIndex{
		records: make(map[string]entity.RunBackendIndexRecord),
		keys:    make(map[string]string),
	}
}

// Bind stores the backend reference for a run.
func (i *AgentOSRunIndex) Bind(_ context.Context, spec agentos.RunSpec, status agentos.RunStatus) error {
	return i.bind(agentosruntime.RunBackendIndexRecordFromRunSpec(spec, status), true)
}

// BindPlanNode stores the backend reference for a plan-owned child run.
func (i *AgentOSRunIndex) BindPlanNode(_ context.Context, planID string, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error {
	return i.bind(agentosruntime.RunBackendIndexRecordFromPlanNode(planID, nodeID, spec, status), true)
}

func (i *AgentOSRunIndex) bind(record entity.RunBackendIndexRecord, requireIdempotencyKey bool) error {
	record = agentosruntime.NormalizeRunBackendIndexRecord(record)
	if err := agentosruntime.ValidateRunBackendIndexRecord(record, requireIdempotencyKey); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if record.IdempotencyKey != "" {
		if existingRunID, exists := i.keys[record.IdempotencyKey]; exists {
			existing := i.records[existingRunID]
			if err := agentosruntime.ValidateRunBackendIndexIdempotency(existing, record); err != nil {
				return err
			}
			i.records[existingRunID] = mergeRunBackendIndexRecord(existing, record)

			return nil
		}
	}
	if existing, exists := i.records[record.RunID]; exists {
		if err := agentosruntime.ValidateRunBackendIndexIdempotency(existing, record); err != nil {
			return err
		}
		i.records[record.RunID] = mergeRunBackendIndexRecord(existing, record)

		return nil
	}
	i.records[record.RunID] = record
	if record.IdempotencyKey != "" {
		i.keys[record.IdempotencyKey] = record.RunID
	}

	return nil
}

func mergeRunBackendIndexRecord(existing entity.RunBackendIndexRecord, requested entity.RunBackendIndexRecord) entity.RunBackendIndexRecord {
	if requested.LifecycleState == agentosruntime.RunBackendLifecycleClaiming && existing.LifecycleState != agentosruntime.RunBackendLifecycleClaiming {
		return existing
	}
	existing.LifecycleState = requested.LifecycleState

	return existing
}

func (i *AgentOSRunIndex) GetRunBackend(_ context.Context, runID string) (agentos.RunBackendOwnership, bool, error) {
	if runID == "" {
		return agentos.RunBackendOwnership{}, false, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	record, ok := i.records[runID]
	if !ok {
		return agentos.RunBackendOwnership{}, false, nil
	}

	return agentosruntime.RunBackendOwnershipFromRecord(record), true, nil
}

// Resolve returns the backend reference that owns a run.
func (i *AgentOSRunIndex) Resolve(_ context.Context, runID string) (agentos.BackendRef, error) {
	if runID == "" {
		return agentos.BackendRef{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	record, ok := i.records[runID]
	if !ok {
		return agentos.BackendRef{}, fmt.Errorf("%w: %s", agentos.ErrRunRouteNotFound, runID)
	}

	return agentos.BackendRef{
		Kind: agentos.BackendKind(record.BackendKind),
		Name: record.BackendName,
	}, nil
}
