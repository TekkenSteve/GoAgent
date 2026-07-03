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
func ValidatePlanStartIdempotency(existing, requested *agentos.RunPlanSpec) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: plan idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: plan %q was started with a different idempotency key", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing plan start request: %w", agentos.ErrInvalidRunPlan, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan start request: %w", agentos.ErrInvalidRunPlan, err)
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
func ValidatePlanStateIdentity(existing, requested *agentos.RunPlanSpec) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: plan state belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: plan %q state has a different idempotency key", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	existingJSON, err := json.Marshal(planStateIdentity(existing))
	if err != nil {
		return fmt.Errorf("%w: marshal existing plan state identity: %w", agentos.ErrInvalidRunPlan, err)
	}

	requestedJSON, err := json.Marshal(planStateIdentity(requested))
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan state identity: %w", agentos.ErrInvalidRunPlan, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: plan %q state changed immutable fields", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	if err := validateTopologyExpansion(existing, requested); err != nil {
		return err
	}

	return nil
}

// PlanTransitionSnapshotIdentity is the immutable content-addressed identity
// for the reducer snapshot claimed by one transition idempotency key.
type PlanTransitionSnapshotIdentity struct {
	Digest string
	JSON   []byte
}

// NewPlanTransitionSnapshotIdentity returns the stable snapshot identity that
// must be persisted with a state-transition event. Replays compare this digest
// instead of the latest plan state because the latest state can advance after
// the transition being retried.
func NewPlanTransitionSnapshotIdentity(snapshot *PlanStateSnapshot, idempotencyKey string) (PlanTransitionSnapshotIdentity, error) {
	if idempotencyKey == "" {
		return PlanTransitionSnapshotIdentity{}, fmt.Errorf("%w: transition idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	identityJSON, err := json.Marshal(planTransitionIdempotencyIdentity(snapshot, idempotencyKey))
	if err != nil {
		return PlanTransitionSnapshotIdentity{}, fmt.Errorf("%w: marshal plan transition identity: %w", agentos.ErrInvalidRunPlan, err)
	}

	sum := sha256.Sum256(identityJSON)

	return PlanTransitionSnapshotIdentity{
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		JSON:   identityJSON,
	}, nil
}

// ValidatePlanTransitionIdempotency verifies that a replayed reducer
// transition carries the same durable snapshot that originally claimed the
// transition idempotency key.
func ValidatePlanTransitionIdempotency(existingDigest string, requested PlanTransitionSnapshotIdentity) error {
	if requested.Digest == "" {
		return fmt.Errorf("%w: requested transition snapshot digest is required", agentos.ErrInvalidRunPlan)
	}

	if existingDigest == "" {
		return fmt.Errorf("%w: transition idempotency key was not claimed by a reducer snapshot", agentos.ErrInvalidRunPlan)
	}

	if existingDigest != requested.Digest {
		return fmt.Errorf("%w: transition idempotency key was reused with a different reducer snapshot", agentos.ErrInvalidRunPlan)
	}

	return nil
}

type planTransitionIdempotencyFields struct {
	Spec           agentos.RunPlanSpec   `json:"spec"`
	Status         agentos.RunPlanStatus `json:"status"`
	IdempotencyKey string                `json:"idempotency_key"`
}

func planTransitionIdempotencyIdentity(snapshot *PlanStateSnapshot, idempotencyKey string) planTransitionIdempotencyFields {
	return planTransitionIdempotencyFields{
		Spec:           snapshot.Spec,
		Status:         snapshot.Status,
		IdempotencyKey: idempotencyKey,
	}
}

type planStateIdentityFields struct {
	PlanID         string             `json:"plan_id"`
	ThreadID       string             `json:"thread_id,omitempty"`
	AccountID      string             `json:"account_id,omitempty"`
	ProjectID      string             `json:"project_id,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time          `json:"requested_at"`
	Inputs         map[string]any     `json:"inputs,omitempty"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Policy         agentos.PlanPolicy `json:"policy"`
}

func planStateIdentity(spec *agentos.RunPlanSpec) planStateIdentityFields {
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

func validateTopologyExpansion(existing, requested *agentos.RunPlanSpec) error {
	requestedNodes := make(map[string]agentos.PlanNodeSpec, len(requested.Nodes))
	for i := range requested.Nodes {
		node := requested.Nodes[i]

		requestedNodes[node.NodeID] = node
	}

	for i := range existing.Nodes {
		node := existing.Nodes[i]
		requestedNode, exists := requestedNodes[node.NodeID]

		if !exists {
			return fmt.Errorf("%w: plan %q removed existing node %q", agentos.ErrInvalidRunPlan, existing.PlanID, node.NodeID)
		}

		if !jsonEqual(node, requestedNode) {
			return fmt.Errorf("%w: plan %q changed existing node %q", agentos.ErrInvalidRunPlan, existing.PlanID, node.NodeID)
		}
	}

	requestedEdges := make(map[string]struct{}, len(requested.Edges))
	for i := range requested.Edges {
		edge := requested.Edges[i]
		requestedEdges[jsonKey(edge)] = struct{}{}
	}

	for i := range existing.Edges {
		edge := existing.Edges[i]
		if _, exists := requestedEdges[jsonKey(edge)]; !exists {
			return fmt.Errorf("%w: plan %q removed or changed existing edge %q", agentos.ErrInvalidRunPlan, existing.PlanID, edge.EdgeID)
		}
	}

	return nil
}

func jsonEqual(left, right any) bool {
	return jsonKey(left) == jsonKey(right)
}

func jsonKey(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}

	return string(data)
}

// ValidateAuditIdempotency verifies that an audit idempotency key is replayed
// for the same control-plane action.
func ValidateAuditIdempotency(existing, requested *AuditRecord) error {
	if err := validateAuditReplayScope(existing, requested); err != nil {
		return err
	}

	existingPayload, err := json.Marshal(normalizeAuditPayload(existing.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal existing audit payload: %w", agentos.ErrInvalidRunPlan, err)
	}

	requestedPayload, err := json.Marshal(normalizeAuditPayload(requested.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal requested audit payload: %w", agentos.ErrInvalidRunPlan, err)
	}

	if !bytes.Equal(existingPayload, requestedPayload) {
		return fmt.Errorf("%w: audit idempotency key was reused with a different payload", agentos.ErrInvalidRunPlan)
	}

	return nil
}

func validateAuditReplayScope(existing, requested *AuditRecord) error {
	fields := []struct {
		existing  string
		requested string
		message   string
	}{
		{existing: existing.PlanID, requested: requested.PlanID, message: fmt.Sprintf("audit idempotency key belongs to plan %q", existing.PlanID)},
		{existing: existing.AccountID, requested: requested.AccountID, message: fmt.Sprintf("audit idempotency key belongs to account %q", existing.AccountID)},
		{existing: existing.ProjectID, requested: requested.ProjectID, message: fmt.Sprintf("audit idempotency key belongs to project %q", existing.ProjectID)},
		{existing: existing.RunID, requested: requested.RunID, message: fmt.Sprintf("audit idempotency key belongs to run %q", existing.RunID)},
		{existing: existing.NodeID, requested: requested.NodeID, message: fmt.Sprintf("audit idempotency key belongs to node %q", existing.NodeID)},
		{existing: existing.ActorID, requested: requested.ActorID, message: fmt.Sprintf("audit idempotency key belongs to actor %q", existing.ActorID)},
		{existing: string(existing.Action), requested: string(requested.Action), message: fmt.Sprintf("audit idempotency key belongs to action %q", existing.Action)},
	}

	for _, field := range fields {
		if field.existing != field.requested {
			return fmt.Errorf("%w: %s", agentos.ErrInvalidRunPlan, field.message)
		}
	}

	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: audit idempotency key mismatch", agentos.ErrInvalidRunPlan)
	}

	return nil
}

// ValidatePlanCommandIdempotency verifies that a command idempotency key is
// replayed for the same durable control-plane command.
func ValidatePlanCommandIdempotency(existing, requested *PlanCommandRecord) error {
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
		return fmt.Errorf("%w: marshal existing command payload: %w", agentos.ErrInvalidRunPlan, err)
	}

	requestedPayload, err := json.Marshal(normalizeAuditPayload(requested.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal requested command payload: %w", agentos.ErrInvalidRunPlan, err)
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

// ArtifactIDFromRef creates the stable artifact identity used when callers do
// not provide one explicitly. Artifact IDs are globally addressed, so the plan
// identity participates in the generated ID.
func ArtifactIDFromRef(planID, idempotencyKey string) string {
	data, err := json.Marshal(struct {
		PlanID         string `json:"plan_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}{
		PlanID:         planID,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

// PlanCommandIDFromRef creates the stable command identity used when callers do
// not provide one explicitly. Idempotency is tenant and plan scoped.
func PlanCommandIDFromRef(ref PlanCommandRef) string {
	data, err := json.Marshal(struct {
		PlanID         string `json:"plan_id"`
		AccountID      string `json:"account_id"`
		ProjectID      string `json:"project_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: ref.IdempotencyKey,
	})
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(data)

	return "command:" + hex.EncodeToString(sum[:])
}

// AuditIDFromRef creates the stable audit identity used when callers do not
// provide one explicitly. Idempotency is tenant and plan scoped.
func AuditIDFromRef(ref AuditRef) string {
	data, err := json.Marshal(struct {
		PlanID         string `json:"plan_id"`
		AccountID      string `json:"account_id"`
		ProjectID      string `json:"project_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: ref.IdempotencyKey,
	})
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(data)

	return "audit:" + hex.EncodeToString(sum[:])
}

// ValidatePlanEventIdempotency verifies that a repeated event append request
// is the same durable event request that originally claimed the key. Store-
// assigned identity fields are only compared when the replay explicitly sets
// them.
func ValidatePlanEventIdempotency(existing, requested *agentos.PlanEvent) error {
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
		return fmt.Errorf("%w: marshal existing plan event: %w", agentos.ErrInvalidPlanEvent, err)
	}

	requestedJSON, err := json.Marshal(planEventIdempotencyIdentity(requested))
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan event: %w", agentos.ErrInvalidPlanEvent, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: plan event idempotency key was reused with a different event", agentos.ErrInvalidPlanEvent)
	}

	return nil
}

type planEventIdempotencyFields struct {
	PlanID    string            `json:"plan_id"`
	AccountID string            `json:"account_id"`
	ProjectID string            `json:"project_id"`
	NodeID    string            `json:"node_id,omitempty"`
	RunID     string            `json:"run_id,omitempty"`
	ThreadID  string            `json:"thread_id,omitempty"`
	EventType agentos.EventType `json:"event_type"`
	Source    string            `json:"source,omitempty"`
	Payload   map[string]any    `json:"payload"`
}

func planEventIdempotencyIdentity(event *agentos.PlanEvent) planEventIdempotencyFields {
	return planEventIdempotencyFields{
		PlanID:    event.PlanID,
		AccountID: event.AccountID,
		ProjectID: event.ProjectID,
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

// NormalizePlanEventAppendRequest removes durable-store-owned identity fields
// before an append request is persisted or compared for idempotency.
func NormalizePlanEventAppendRequest(event *agentos.PlanEvent) agentos.PlanEvent {
	normalized := *event
	normalized.EventID = ""
	normalized.Sequence = 0
	normalized.Timestamp = NormalizeDurableTimestamp(normalized.Timestamp)

	return normalized
}

// NormalizeDurableTimestamp returns the canonical timestamp precision used by
// durable stores. Postgres timestamptz stores microseconds, and JSON snapshots
// constrained against timestamp columns must match that precision exactly.
func NormalizeDurableTimestamp(timestamp time.Time) time.Time {
	if timestamp.IsZero() {
		return timestamp
	}

	return timestamp.Round(time.Microsecond)
}

// ValidateArtifactPublishIdempotency verifies that a repeated artifact publish
// request is the same durable artifact request that originally claimed the key.
func ValidateArtifactPublishIdempotency(existing, requested *agentos.ArtifactRef) error {
	if existing.ArtifactID != requested.ArtifactID {
		return fmt.Errorf("%w: artifact idempotency key belongs to artifact %q", agentos.ErrInvalidArtifact, existing.ArtifactID)
	}

	if requested.URI != "" && existing.URI != requested.URI {
		return fmt.Errorf("%w: artifact idempotency key belongs to uri %q", agentos.ErrInvalidArtifact, existing.URI)
	}

	existingJSON, err := json.Marshal(artifactIdempotencyIdentity(existing))
	if err != nil {
		return fmt.Errorf("%w: marshal existing artifact: %w", agentos.ErrInvalidArtifact, err)
	}

	requestedJSON, err := json.Marshal(artifactIdempotencyIdentity(requested))
	if err != nil {
		return fmt.Errorf("%w: marshal requested artifact: %w", agentos.ErrInvalidArtifact, err)
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

func artifactIdempotencyIdentity(ref *agentos.ArtifactRef) artifactIdempotencyFields {
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

func sameTime(left, right time.Time) bool {
	if left.IsZero() || right.IsZero() {
		return left.IsZero() && right.IsZero()
	}

	return left.Equal(right)
}
