package agentosplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidatePlanStartIdempotency verifies that a repeated plan start request is
// the same durable request that originally claimed the idempotency key.
func ValidatePlanStartIdempotency(existing agentos.RunPlanSpec, requested agentos.RunPlanSpec) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: plan idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}
	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: plan %q was started with a different idempotency key", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing plan start request: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan start request: %s", agentos.ErrInvalidRunPlan, err)
	}
	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: plan %q idempotency key was reused with a different request", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	return nil
}

// ValidatePlanStateIdentity verifies that a workflow-owned state snapshot keeps
// the immutable plan identity and request envelope unchanged. Topology may
// evolve through validated PlanDelta application, so Nodes and Edges are not
// part of this comparison.
func ValidatePlanStateIdentity(existing agentos.RunPlanSpec, requested agentos.RunPlanSpec) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: plan state belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}
	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: plan %q state has a different idempotency key", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	existingJSON, err := json.Marshal(planStateIdentity(existing))
	if err != nil {
		return fmt.Errorf("%w: marshal existing plan state identity: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedJSON, err := json.Marshal(planStateIdentity(requested))
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan state identity: %s", agentos.ErrInvalidRunPlan, err)
	}
	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: plan %q state changed immutable fields", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	return nil
}

type planStateIdentityFields struct {
	PlanID         string             `json:"plan_id"`
	ThreadID       string             `json:"thread_id,omitempty"`
	AccountID      string             `json:"account_id,omitempty"`
	ProjectID      string             `json:"project_id,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time          `json:"requested_at,omitempty"`
	Inputs         map[string]any     `json:"inputs,omitempty"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Policy         agentos.PlanPolicy `json:"policy,omitempty"`
}

func planStateIdentity(spec agentos.RunPlanSpec) planStateIdentityFields {
	return planStateIdentityFields{
		PlanID:         spec.PlanID,
		ThreadID:       spec.ThreadID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
		RequestedAt:    spec.RequestedAt,
		Inputs:         spec.Inputs,
		Metadata:       spec.Metadata,
		Policy:         spec.Policy,
	}
}

// ValidateAuditIdempotency verifies that an audit idempotency key is replayed
// for the same control-plane action.
func ValidateAuditIdempotency(existing AuditRecord, requested AuditRecord) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: audit idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}
	if existing.AccountID != requested.AccountID {
		return fmt.Errorf("%w: audit idempotency key belongs to account %q", agentos.ErrInvalidRunPlan, existing.AccountID)
	}
	if existing.ProjectID != requested.ProjectID {
		return fmt.Errorf("%w: audit idempotency key belongs to project %q", agentos.ErrInvalidRunPlan, existing.ProjectID)
	}
	if existing.RunID != requested.RunID {
		return fmt.Errorf("%w: audit idempotency key belongs to run %q", agentos.ErrInvalidRunPlan, existing.RunID)
	}
	if existing.NodeID != requested.NodeID {
		return fmt.Errorf("%w: audit idempotency key belongs to node %q", agentos.ErrInvalidRunPlan, existing.NodeID)
	}
	if existing.ActorID != requested.ActorID {
		return fmt.Errorf("%w: audit idempotency key belongs to actor %q", agentos.ErrInvalidRunPlan, existing.ActorID)
	}
	if existing.Action != requested.Action {
		return fmt.Errorf("%w: audit idempotency key belongs to action %q", agentos.ErrInvalidRunPlan, existing.Action)
	}
	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: audit idempotency key mismatch", agentos.ErrInvalidRunPlan)
	}

	existingPayload, err := json.Marshal(normalizeAuditPayload(existing.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal existing audit payload: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedPayload, err := json.Marshal(normalizeAuditPayload(requested.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal requested audit payload: %s", agentos.ErrInvalidRunPlan, err)
	}
	if !bytes.Equal(existingPayload, requestedPayload) {
		return fmt.Errorf("%w: audit idempotency key was reused with a different payload", agentos.ErrInvalidRunPlan)
	}

	return nil
}

// ValidatePlanCommandIdempotency verifies that a command idempotency key is
// replayed for the same durable control-plane command.
func ValidatePlanCommandIdempotency(existing PlanCommandRecord, requested PlanCommandRecord) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: command idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}
	if existing.AccountID != requested.AccountID {
		return fmt.Errorf("%w: command idempotency key belongs to account %q", agentos.ErrInvalidRunPlan, existing.AccountID)
	}
	if existing.ProjectID != requested.ProjectID {
		return fmt.Errorf("%w: command idempotency key belongs to project %q", agentos.ErrInvalidRunPlan, existing.ProjectID)
	}
	if existing.ActorID != requested.ActorID {
		return fmt.Errorf("%w: command idempotency key belongs to actor %q", agentos.ErrInvalidRunPlan, existing.ActorID)
	}
	if existing.Action != requested.Action {
		return fmt.Errorf("%w: command idempotency key belongs to action %q", agentos.ErrInvalidRunPlan, existing.Action)
	}
	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: command idempotency key mismatch", agentos.ErrInvalidRunPlan)
	}

	existingPayload, err := json.Marshal(normalizeAuditPayload(existing.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal existing command payload: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedPayload, err := json.Marshal(normalizeAuditPayload(requested.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal requested command payload: %s", agentos.ErrInvalidRunPlan, err)
	}
	if !bytes.Equal(existingPayload, requestedPayload) {
		return fmt.Errorf("%w: command idempotency key was reused with a different payload", agentos.ErrInvalidRunPlan)
	}

	return nil
}

func normalizeAuditPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}

	return payload
}

// ArtifactIDFromIdempotencyKey creates the stable artifact identity used when
// callers do not provide one explicitly.
func ArtifactIDFromIdempotencyKey(idempotencyKey string) string {
	sum := sha256.Sum256([]byte(idempotencyKey))

	return hex.EncodeToString(sum[:])
}

// ValidatePlanEventIdempotency verifies that a repeated event append request
// is the same durable event request that originally claimed the key. Store-
// assigned identity fields are only compared when the replay explicitly sets
// them.
func ValidatePlanEventIdempotency(existing agentos.PlanEvent, requested agentos.PlanEvent) error {
	if requested.EventID != "" && existing.EventID != requested.EventID {
		return fmt.Errorf("%w: plan event idempotency key belongs to event %q", agentos.ErrInvalidPlanEvent, existing.EventID)
	}
	if requested.Sequence != 0 && existing.Sequence != requested.Sequence {
		return fmt.Errorf("%w: plan event idempotency key belongs to sequence %d", agentos.ErrInvalidPlanEvent, existing.Sequence)
	}
	if !requested.Timestamp.IsZero() && !sameTime(existing.Timestamp, requested.Timestamp) {
		return fmt.Errorf("%w: plan event idempotency key belongs to timestamp %s", agentos.ErrInvalidPlanEvent, existing.Timestamp.Format(time.RFC3339Nano))
	}

	existingJSON, err := json.Marshal(planEventIdempotencyIdentity(existing))
	if err != nil {
		return fmt.Errorf("%w: marshal existing plan event: %s", agentos.ErrInvalidPlanEvent, err)
	}
	requestedJSON, err := json.Marshal(planEventIdempotencyIdentity(requested))
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan event: %s", agentos.ErrInvalidPlanEvent, err)
	}
	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: plan event idempotency key was reused with a different event", agentos.ErrInvalidPlanEvent)
	}

	return nil
}

type planEventIdempotencyFields struct {
	PlanID    string            `json:"plan_id"`
	NodeID    string            `json:"node_id,omitempty"`
	RunID     string            `json:"run_id,omitempty"`
	ThreadID  string            `json:"thread_id,omitempty"`
	EventType agentos.EventType `json:"event_type"`
	Source    string            `json:"source,omitempty"`
	Payload   map[string]any    `json:"payload"`
}

func planEventIdempotencyIdentity(event agentos.PlanEvent) planEventIdempotencyFields {
	return planEventIdempotencyFields{
		PlanID:    event.PlanID,
		NodeID:    event.NodeID,
		RunID:     event.RunID,
		ThreadID:  event.ThreadID,
		EventType: event.EventType,
		Source:    event.Source,
		Payload:   normalizeEventPayload(event.Payload),
	}
}

func normalizeEventPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}

	return payload
}

// ValidateArtifactPublishIdempotency verifies that a repeated artifact publish
// request is the same durable artifact request that originally claimed the key.
func ValidateArtifactPublishIdempotency(existing agentos.ArtifactRef, requested agentos.ArtifactRef) error {
	if existing.ArtifactID != requested.ArtifactID {
		return fmt.Errorf("%w: artifact idempotency key belongs to artifact %q", agentos.ErrInvalidArtifact, existing.ArtifactID)
	}
	if requested.URI != "" && existing.URI != requested.URI {
		return fmt.Errorf("%w: artifact idempotency key belongs to uri %q", agentos.ErrInvalidArtifact, existing.URI)
	}

	existingJSON, err := json.Marshal(artifactIdempotencyIdentity(existing))
	if err != nil {
		return fmt.Errorf("%w: marshal existing artifact: %s", agentos.ErrInvalidArtifact, err)
	}
	requestedJSON, err := json.Marshal(artifactIdempotencyIdentity(requested))
	if err != nil {
		return fmt.Errorf("%w: marshal requested artifact: %s", agentos.ErrInvalidArtifact, err)
	}
	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: artifact idempotency key was reused with a different artifact", agentos.ErrInvalidArtifact)
	}

	return nil
}

type artifactIdempotencyFields struct {
	ArtifactID string               `json:"artifact_id"`
	PlanID     string               `json:"plan_id,omitempty"`
	NodeID     string               `json:"node_id,omitempty"`
	RunID      string               `json:"run_id,omitempty"`
	Name       string               `json:"name"`
	Kind       agentos.ArtifactKind `json:"kind"`
	MediaType  string               `json:"media_type,omitempty"`
	SizeBytes  int64                `json:"size_bytes,omitempty"`
	Digest     string               `json:"digest,omitempty"`
	Metadata   map[string]string    `json:"metadata"`
}

func artifactIdempotencyIdentity(ref agentos.ArtifactRef) artifactIdempotencyFields {
	return artifactIdempotencyFields{
		ArtifactID: ref.ArtifactID,
		PlanID:     ref.PlanID,
		NodeID:     ref.NodeID,
		RunID:      ref.RunID,
		Name:       ref.Name,
		Kind:       ref.Kind,
		MediaType:  ref.MediaType,
		SizeBytes:  ref.SizeBytes,
		Digest:     ref.Digest,
		Metadata:   normalizeArtifactMetadata(ref.Metadata),
	}
}

func normalizeArtifactMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}

	return metadata
}

func sameTime(left time.Time, right time.Time) bool {
	if left.IsZero() || right.IsZero() {
		return left.IsZero() && right.IsZero()
	}

	return left.Equal(right)
}
