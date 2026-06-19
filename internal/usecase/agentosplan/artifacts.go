package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// MemoryArtifactStore stores artifacts in-process. Production runtimes should
// inject a durable store.
type MemoryArtifactStore struct {
	mu        sync.RWMutex
	artifacts map[string]storedArtifact
	byPlan    map[string][]string
	byKey     map[string]string
	now       func() time.Time
}

type storedArtifact struct {
	ref     agentos.ArtifactRef
	payload any
}

// NewMemoryArtifactStore creates an in-memory artifact store.
func NewMemoryArtifactStore() *MemoryArtifactStore {
	return &MemoryArtifactStore{
		artifacts: make(map[string]storedArtifact),
		byPlan:    make(map[string][]string),
		byKey:     make(map[string]string),
		now:       func() time.Time { return time.Now().UTC() },
	}
}

// Put stores one artifact payload.
func (s *MemoryArtifactStore) Put(_ context.Context, artifact agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	if idempotencyKey == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact idempotency key is required", agentos.ErrInvalidArtifact)
	}
	if artifact.Name == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact name is required", agentos.ErrInvalidArtifact)
	}
	if artifact.Kind == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact kind is required", agentos.ErrInvalidArtifact)
	}
	if artifact.ArtifactID == "" {
		artifact.ArtifactID = ArtifactIDFromIdempotencyKey(idempotencyKey)
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = s.now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if idempotencyKey != "" {
		if artifactID, exists := s.byKey[idempotencyKey]; exists {
			existing := s.artifacts[artifactID]
			if err := ValidateArtifactPublishIdempotency(existing.ref, artifact); err != nil {
				return agentos.ArtifactRef{}, err
			}
			if err := validateArtifactPayloadIdempotency(existing.payload, payload); err != nil {
				return agentos.ArtifactRef{}, err
			}

			return existing.ref, nil
		}
	}
	s.artifacts[artifact.ArtifactID] = storedArtifact{ref: artifact, payload: payload}
	if idempotencyKey != "" {
		s.byKey[idempotencyKey] = artifact.ArtifactID
	}
	if artifact.PlanID != "" {
		s.byPlan[artifact.PlanID] = append(s.byPlan[artifact.PlanID], artifact.ArtifactID)
	}

	return artifact, nil
}

// Get retrieves one artifact.
func (s *MemoryArtifactStore) Get(_ context.Context, artifactID string) (agentos.ArtifactRef, any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	artifact, ok := s.artifacts[artifactID]
	if !ok {
		return agentos.ArtifactRef{}, nil, fmt.Errorf("%w: %s", agentos.ErrArtifactNotFound, artifactID)
	}

	return artifact.ref, artifact.payload, nil
}

// List returns artifacts associated with a plan.
func (s *MemoryArtifactStore) List(_ context.Context, planID string) ([]agentos.ArtifactRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.byPlan[planID]
	refs := make([]agentos.ArtifactRef, 0, len(ids))
	for _, id := range ids {
		if artifact, ok := s.artifacts[id]; ok {
			refs = append(refs, artifact.ref)
		}
	}

	return refs, nil
}

func validateArtifactPayloadIdempotency(existing any, requested any) error {
	if reflect.DeepEqual(existing, requested) {
		return nil
	}
	existingJSON, existingErr := json.Marshal(existing)
	requestedJSON, requestedErr := json.Marshal(requested)
	if existingErr == nil && requestedErr == nil && string(existingJSON) == string(requestedJSON) {
		return nil
	}

	return fmt.Errorf("%w: artifact idempotency key was reused with a different payload", agentos.ErrInvalidArtifact)
}
