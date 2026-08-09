package process

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// LedgerEntryKind identifies a durable audit fact without binding AgentOS to a
// business domain.
type LedgerEntryKind string

// LedgerEntryKind values identifying durable audit facts.
const (
	LedgerEntryDecision  LedgerEntryKind = "decision"
	LedgerEntryEvidence  LedgerEntryKind = "evidence"
	LedgerEntryArtifact  LedgerEntryKind = "artifact"
	LedgerEntryAction    LedgerEntryKind = "action"
	LedgerEntryPrompt    LedgerEntryKind = "prompt"
	LedgerEntryResponse  LedgerEntryKind = "response"
	LedgerEntryToolIO    LedgerEntryKind = "tool_io"
	LedgerEntryRationale LedgerEntryKind = "rationale"
)

// ActorRef identifies the actor that produced a ledger entry.
type ActorRef struct {
	Kind    string `json:"kind,omitempty"`
	ActorID string `json:"actor_id,omitempty"`
}

// LedgerDataRef points to evidence, prompts, tool I/O, or other large payloads
// outside Temporal workflow history.
type LedgerDataRef struct {
	Kind       string            `json:"kind"`
	URI        string            `json:"uri,omitempty"`
	ArtifactID string            `json:"artifact_id,omitempty"`
	MediaType  string            `json:"media_type,omitempty"`
	Digest     string            `json:"digest,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// LedgerEntrySpec is the append-only public write contract for process and
// resource timelines. Large payloads must be referenced through DataRefs or
// ArtifactRefs rather than embedded into Temporal history.
type LedgerEntrySpec struct {
	EntryID        string             `json:"entry_id"`
	IdempotencyKey string             `json:"idempotency_key"`
	AccountID      string             `json:"account_id"`
	ProjectID      string             `json:"project_id"`
	ProcessID      string             `json:"process_id,omitempty"`
	Resource       ResourceRef        `json:"resource,omitzero" schema:"optional"`
	Kind           LedgerEntryKind    `json:"kind"`
	Actor          ActorRef           `json:"actor,omitzero" schema:"optional"`
	OccurredAt     time.Time          `json:"occurred_at,omitzero" schema:"optional"`
	Summary        string             `json:"summary,omitempty"`
	Rationale      string             `json:"rationale,omitempty"`
	DataRefs       []LedgerDataRef    `json:"data_refs,omitempty"`
	ArtifactRefs   []core.ArtifactRef `json:"artifact_refs,omitempty"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
}

// LedgerEntry is a durable append-only audit record with store-owned ordering.
type LedgerEntry struct {
	LedgerEntrySpec
	Sequence  int64     `json:"sequence"`
	CreatedAt time.Time `json:"created_at,omitzero" schema:"optional"`
}

// LedgerScope selects ledger entries for a tenant, process, resource, or kind.
type LedgerScope struct {
	AccountID     string          `json:"account_id"`
	ProjectID     string          `json:"project_id"`
	ProcessID     string          `json:"process_id,omitempty"`
	Resource      ResourceRef     `json:"resource,omitzero" schema:"optional"`
	Kind          LedgerEntryKind `json:"kind,omitempty"`
	AfterSequence int64           `json:"after_sequence,omitempty"`
	Limit         int             `json:"limit,omitempty"`
}

// ValidateLedgerEntrySpec validates one append-only ledger record.
func ValidateLedgerEntrySpec(spec *LedgerEntrySpec) error {
	if spec == nil {
		return fmt.Errorf("%w: ledger entry is required", core.ErrInvalidLedgerEntry)
	}

	if err := validateLedgerEntryIdentity(spec); err != nil {
		return err
	}

	if err := validateLedgerEntryScope(spec); err != nil {
		return err
	}

	return validateLedgerDataRefs(spec.DataRefs)
}

// ValidateLedgerScope validates a ledger query scope.
func ValidateLedgerScope(scope *LedgerScope) error {
	if scope == nil {
		return fmt.Errorf("%w: ledger scope is required", core.ErrInvalidLedgerScope)
	}

	switch {
	case scope.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidLedgerScope)
	case scope.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidLedgerScope)
	case scope.Limit < 0:
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidLedgerScope)
	}

	return validateLedgerResource(scope.AccountID, scope.ProjectID, scope.Resource, core.ErrInvalidLedgerScope)
}

func validateLedgerEntryIdentity(spec *LedgerEntrySpec) error {
	switch {
	case spec.EntryID == "":
		return fmt.Errorf("%w: entry id is required", core.ErrInvalidLedgerEntry)
	case spec.IdempotencyKey == "":
		return fmt.Errorf("%w: idempotency key is required", core.ErrInvalidLedgerEntry)
	case spec.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidLedgerEntry)
	case spec.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidLedgerEntry)
	case spec.Kind == "":
		return fmt.Errorf("%w: kind is required", core.ErrInvalidLedgerEntry)
	}

	return nil
}

func validateLedgerEntryScope(spec *LedgerEntrySpec) error {
	if err := validateLedgerResource(spec.AccountID, spec.ProjectID, spec.Resource, core.ErrInvalidLedgerEntry); err != nil {
		return err
	}

	if spec.ProcessID == "" && spec.Resource.Kind == "" {
		return fmt.Errorf("%w: process id or resource ref is required", core.ErrInvalidLedgerEntry)
	}

	return nil
}

func validateLedgerResource(accountID, projectID string, resource ResourceRef, scopeErr error) error {
	if resource.Kind == "" && resource.ResourceID == "" && resource.AccountID == "" && resource.ProjectID == "" {
		return nil
	}

	if err := ValidateResourceRef(resource); err != nil {
		return fmt.Errorf("%w: %w", scopeErr, err)
	}

	if resource.AccountID != accountID || resource.ProjectID != projectID {
		return fmt.Errorf("%w: resource must be in ledger tenant scope", scopeErr)
	}

	return nil
}

func validateLedgerDataRefs(refs []LedgerDataRef) error {
	for i := range refs {
		if err := validateLedgerDataRef(&refs[i]); err != nil {
			return err
		}
	}

	return nil
}

func validateLedgerDataRef(ref *LedgerDataRef) error {
	switch {
	case ref.Kind == "":
		return fmt.Errorf("%w: data ref kind is required", core.ErrInvalidLedgerEntry)
	case ref.URI == "" && ref.ArtifactID == "":
		return fmt.Errorf("%w: data ref uri or artifact id is required", core.ErrInvalidLedgerEntry)
	default:
		return nil
	}
}
