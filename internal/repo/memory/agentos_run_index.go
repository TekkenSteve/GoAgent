package memory

import (
	"context"
	"fmt"
	"sync"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
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
func (i *AgentOSRunIndex) Bind(_ context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) error {
	record := agentosruntime.RunBackendIndexRecordFromRunSpec(spec, status)

	return i.bind(&record, true)
}

// BindPlanNode stores the backend reference for a plan-owned child run.
func (i *AgentOSRunIndex) BindPlanNode(_ context.Context, planID, nodeID string, spec *agentos.RunSpec, status *agentos.RunStatus) error {
	record := agentosruntime.RunBackendIndexRecordFromPlanNode(planID, nodeID, spec, status)

	return i.bind(&record, true)
}

func (i *AgentOSRunIndex) bind(record *entity.RunBackendIndexRecord, requireIdempotencyKey bool) error {
	normalized := agentosruntime.NormalizeRunBackendIndexRecord(record)
	if err := agentosruntime.ValidateRunBackendIndexRecord(&normalized, requireIdempotencyKey); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if normalized.IdempotencyKey != "" {
		if existingRunID, exists := i.keys[normalized.IdempotencyKey]; exists {
			existing := i.records[existingRunID]
			if err := agentosruntime.ValidateRunBackendIndexIdempotency(&existing, &normalized); err != nil {
				return err
			}

			i.records[existingRunID] = mergeRunBackendIndexRecord(&existing, &normalized)

			return nil
		}
	}

	if existing, exists := i.records[normalized.RunID]; exists {
		if err := agentosruntime.ValidateRunBackendIndexIdempotency(&existing, &normalized); err != nil {
			return err
		}

		i.records[normalized.RunID] = mergeRunBackendIndexRecord(&existing, &normalized)

		return nil
	}

	i.records[normalized.RunID] = normalized
	if normalized.IdempotencyKey != "" {
		i.keys[normalized.IdempotencyKey] = normalized.RunID
	}

	return nil
}

func mergeRunBackendIndexRecord(existing, requested *entity.RunBackendIndexRecord) entity.RunBackendIndexRecord {
	if requested.LifecycleState == agentosruntime.RunBackendLifecycleClaiming && existing.LifecycleState != agentosruntime.RunBackendLifecycleClaiming {
		return *existing
	}

	merged := *existing
	merged.LifecycleState = requested.LifecycleState

	return merged
}

func (i *AgentOSRunIndex) GetRunBackend(_ context.Context, runID string) (agentos.RunBackendOwnership, bool, error) {
	if runID == "" {
		return agentos.RunBackendOwnership{}, false, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	record, ok := i.records[runID]
	if !ok {
		return agentos.RunBackendOwnership{}, false, nil
	}

	return agentosruntime.RunBackendOwnershipFromRecord(&record), true, nil
}

// Resolve returns the backend reference that owns a run.
func (i *AgentOSRunIndex) Resolve(_ context.Context, runID string) (agentos.BackendRef, error) {
	if runID == "" {
		return agentos.BackendRef{}, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	record, ok := i.records[runID]
	if !ok {
		return agentos.BackendRef{}, fmt.Errorf("%w: %s", agentoscore.ErrRunRouteNotFound, runID)
	}

	return agentos.BackendRef{
		Kind: agentos.BackendKind(record.BackendKind),
		Name: record.BackendName,
	}, nil
}
