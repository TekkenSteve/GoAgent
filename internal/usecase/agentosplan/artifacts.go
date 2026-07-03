package agentosplan

import (
	"context"
	"fmt"
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
	byKey     map[artifactIdempotencyKey]string
	now       func() time.Time
}

type artifactIdempotencyKey struct {
	PlanID         string
	IdempotencyKey string
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
		byKey:     make(map[artifactIdempotencyKey]string),
		now:       func() time.Time { return time.Now().UTC() },
	}
}

// Put stores one artifact payload.
func (s *MemoryArtifactStore) Put(_ context.Context, artifact *agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	ref, err := s.prepareArtifactRef(artifact, payload, idempotencyKey)
	if err != nil {
		return agentos.ArtifactRef{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok, err := s.replayedArtifact(&ref, idempotencyKey); ok || err != nil {
		return existing, err
	}

	return s.storeNewArtifact(&ref, payload, idempotencyKey)
}

func (s *MemoryArtifactStore) prepareArtifactRef(artifact *agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	if err := validateArtifactPutInput(artifact, idempotencyKey); err != nil {
		return agentos.ArtifactRef{}, err
	}

	ref, err := artifactRefWithPayloadMetadata(artifact, payload)
	if err != nil {
		return agentos.ArtifactRef{}, err
	}

	if ref.ArtifactID == "" {
		ref.ArtifactID = ArtifactIDFromRef(ref.PlanID, idempotencyKey)
	}

	if ref.CreatedAt.IsZero() {
		ref.CreatedAt = s.now()
	}

	return ref, nil
}

func validateArtifactPutInput(artifact *agentos.ArtifactRef, idempotencyKey string) error {
	if idempotencyKey == "" {
		return fmt.Errorf("%w: artifact idempotency key is required", agentos.ErrInvalidArtifact)
	}

	if artifact.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidArtifact)
	}

	if artifact.Name == "" {
		return fmt.Errorf("%w: artifact name is required", agentos.ErrInvalidArtifact)
	}

	if artifact.Kind == "" {
		return fmt.Errorf("%w: artifact kind is required", agentos.ErrInvalidArtifact)
	}

	return nil
}

func artifactRefWithPayloadMetadata(artifact *agentos.ArtifactRef, payload any) (agentos.ArtifactRef, error) {
	ref := *artifact
	if payload == nil {
		return ref, nil
	}

	encodedPayload, mediaType, err := EncodeArtifactPayload(payload, ref.MediaType)
	if err != nil {
		return agentos.ArtifactRef{}, err
	}

	ref.MediaType = mediaType
	ref.SizeBytes = int64(len(encodedPayload))
	ref.Digest = DigestArtifactPayload(encodedPayload)

	return ref, nil
}

func (s *MemoryArtifactStore) replayedArtifact(artifact *agentos.ArtifactRef, idempotencyKey string) (agentos.ArtifactRef, bool, error) {
	key := artifactIdempotencyKey{PlanID: artifact.PlanID, IdempotencyKey: idempotencyKey}

	artifactID, exists := s.byKey[key]
	if !exists {
		return agentos.ArtifactRef{}, false, nil
	}

	existing := s.artifacts[artifactID]
	if err := ValidateArtifactPublishIdempotency(&existing.ref, artifact); err != nil {
		return agentos.ArtifactRef{}, true, err
	}

	return existing.ref, true, nil
}

func (s *MemoryArtifactStore) storeNewArtifact(artifact *agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	if payload == nil {
		if err := ValidateNewRefOnlyArtifactPublish(artifact); err != nil {
			return agentos.ArtifactRef{}, err
		}
	}

	if _, exists := s.artifacts[artifact.ArtifactID]; exists {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact id %q already exists with a different idempotency key", agentos.ErrInvalidArtifact, artifact.ArtifactID)
	}

	s.artifacts[artifact.ArtifactID] = storedArtifact{ref: *artifact, payload: payload}
	s.byKey[artifactIdempotencyKey{PlanID: artifact.PlanID, IdempotencyKey: idempotencyKey}] = artifact.ArtifactID
	s.byPlan[artifact.PlanID] = append(s.byPlan[artifact.PlanID], artifact.ArtifactID)

	return *artifact, nil
}

// Get retrieves one artifact inside a plan scope.
func (s *MemoryArtifactStore) Get(_ context.Context, scope *agentos.PlanArtifactScope) (agentos.ArtifactRef, any, error) {
	if err := ValidatePlanArtifactScope(scope); err != nil {
		return agentos.ArtifactRef{}, nil, err
	}

	if scope.ArtifactID == "" {
		return agentos.ArtifactRef{}, nil, fmt.Errorf("%w: artifact id is required", agentos.ErrInvalidArtifact)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	artifact, ok := s.artifacts[scope.ArtifactID]
	if !ok || !artifactRefMatchesScope(&artifact.ref, scope) {
		return agentos.ArtifactRef{}, nil, fmt.Errorf("%w: %s", agentos.ErrArtifactNotFound, scope.ArtifactID)
	}

	return artifact.ref, artifact.payload, nil
}

// List returns artifacts associated with a plan scope.
func (s *MemoryArtifactStore) List(_ context.Context, scope *agentos.PlanArtifactScope) ([]agentos.ArtifactRef, error) {
	if err := ValidatePlanArtifactScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.byPlan[scope.PlanID]

	refs := make([]agentos.ArtifactRef, 0, len(ids))
	for _, id := range ids {
		if artifact, ok := s.artifacts[id]; ok && artifactRefMatchesScope(&artifact.ref, scope) {
			refs = append(refs, artifact.ref)
			if scope.Limit > 0 && len(refs) >= scope.Limit {
				break
			}
		}
	}

	return refs, nil
}

func artifactRefMatchesScope(ref *agentos.ArtifactRef, scope *agentos.PlanArtifactScope) bool {
	if ref.PlanID != scope.PlanID {
		return false
	}

	if scope.ArtifactID != "" && ref.ArtifactID != scope.ArtifactID {
		return false
	}

	if scope.NodeID != "" && ref.NodeID != scope.NodeID {
		return false
	}

	if scope.RunID != "" && ref.RunID != scope.RunID {
		return false
	}

	return true
}

// ValidateNewRefOnlyArtifactPublish rejects first-time ref-only artifact
// publishes that attempt to claim blob metadata without writing payload through
// the ArtifactStore. Idempotent replays of an existing artifact are validated
// against the stored ref before this check is needed.
func ValidateNewRefOnlyArtifactPublish(ref *agentos.ArtifactRef) error {
	if ref.URI != "" || ref.SizeBytes != 0 || ref.Digest != "" {
		return fmt.Errorf("%w: artifact payload metadata requires a stored payload", agentos.ErrInvalidArtifact)
	}

	return nil
}
