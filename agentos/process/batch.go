package process

import (
	"fmt"
	"math"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// WorksetKind identifies an application-defined batch work category without
// making AgentOS own the domain model.
type WorksetKind string

// WorksetRef identifies a batch workset inside an account/project boundary.
type WorksetRef struct {
	WorksetID string `json:"workset_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// WorksetItemsRef points to the batch data plane outside Temporal history.
type WorksetItemsRef struct {
	Kind       string            `json:"kind"`
	URI        string            `json:"uri,omitempty"`
	ArtifactID string            `json:"artifact_id,omitempty"`
	MediaType  string            `json:"media_type,omitempty"`
	Digest     string            `json:"digest,omitempty"`
	Count      int64             `json:"count,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// WorksetChunkSpec describes one coarse chunk of a larger workset. A chunk is
// owned by an activity or backend run, not by one workflow per data record.
type WorksetChunkSpec struct {
	ChunkID     string          `json:"chunk_id"`
	ItemsRef    WorksetItemsRef `json:"items_ref"`
	ItemCount   int64           `json:"item_count,omitempty"`
	Concurrency int32           `json:"concurrency,omitempty"`
}

// WorksetPolicy constrains chunking and execution pressure.
type WorksetPolicy struct {
	MaxItems       int64 `json:"max_items,omitempty"`
	MaxChunkSize   int64 `json:"max_chunk_size,omitempty"`
	MaxChunks      int32 `json:"max_chunks,omitempty"`
	MaxConcurrency int32 `json:"max_concurrency,omitempty"`
}

// WorksetSpec requests one batch workset. Inputs are referenced, not embedded,
// so AgentOS can coordinate durable progress without becoming the data plane.
type WorksetSpec struct {
	WorksetID      string             `json:"workset_id"`
	IdempotencyKey string             `json:"idempotency_key"`
	AccountID      string             `json:"account_id"`
	ProjectID      string             `json:"project_id"`
	ProcessID      string             `json:"process_id,omitempty"`
	Resource       ResourceRef        `json:"resource,omitzero" schema:"optional"`
	Kind           WorksetKind        `json:"kind"`
	RequestedBy    ActorRef           `json:"requested_by,omitzero" schema:"optional"`
	RequestedAt    time.Time          `json:"requested_at,omitzero" schema:"optional"`
	ItemsRef       WorksetItemsRef    `json:"items_ref"`
	Chunks         []WorksetChunkSpec `json:"chunks,omitempty"`
	Policy         WorksetPolicy      `json:"policy,omitzero" schema:"optional"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
}

// WorksetProgress is the aggregate durable progress projection.
type WorksetProgress struct {
	TotalItems      int64 `json:"total_items,omitempty"`
	CompletedItems  int64 `json:"completed_items,omitempty"`
	FailedItems     int64 `json:"failed_items,omitempty"`
	TotalChunks     int32 `json:"total_chunks,omitempty"`
	CompletedChunks int32 `json:"completed_chunks,omitempty"`
	FailedChunks    int32 `json:"failed_chunks,omitempty"`
}

// WorksetStatus is the public lifecycle projection for one batch workset.
type WorksetStatus struct {
	WorksetID      string            `json:"workset_id"`
	AccountID      string            `json:"account_id"`
	ProjectID      string            `json:"project_id"`
	ProcessID      string            `json:"process_id,omitempty"`
	Resource       ResourceRef       `json:"resource,omitzero" schema:"optional"`
	Kind           WorksetKind       `json:"kind,omitempty"`
	LifecycleState string            `json:"lifecycle_state"`
	Progress       WorksetProgress   `json:"progress,omitzero" schema:"optional"`
	Reason         string            `json:"reason,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	UpdatedAt      time.Time         `json:"updated_at,omitzero" schema:"optional"`
}

// WorksetChunkResult records durable progress for one processed chunk.
type WorksetChunkResult struct {
	ChunkID        string          `json:"chunk_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	CompletedItems int64           `json:"completed_items,omitempty"`
	FailedItems    int64           `json:"failed_items,omitempty"`
	Succeeded      bool            `json:"succeeded"`
	Reason         string          `json:"reason,omitempty"`
	OutputRefs     []LedgerDataRef `json:"output_refs,omitempty"`
	RecordedAt     time.Time       `json:"recorded_at,omitzero" schema:"optional"`
}

// WorksetScope selects worksets for a tenant, process, resource, kind, or
// lifecycle state.
type WorksetScope struct {
	AccountID      string      `json:"account_id"`
	ProjectID      string      `json:"project_id"`
	ProcessID      string      `json:"process_id,omitempty"`
	Resource       ResourceRef `json:"resource,omitzero" schema:"optional"`
	Kind           WorksetKind `json:"kind,omitempty"`
	LifecycleState string      `json:"lifecycle_state,omitempty"`
	Limit          int         `json:"limit,omitempty"`
}

// Workset lifecycle state constants.
const (
	WorksetPending   = "pending"
	WorksetRunning   = "running"
	WorksetSucceeded = "succeeded"
	WorksetFailed    = "failed"
	WorksetCanceled  = "canceled"
)

// ValidateWorksetSpec validates a batch workset request.
func ValidateWorksetSpec(spec *WorksetSpec) error {
	if spec == nil {
		return fmt.Errorf("%w: workset spec is required", core.ErrInvalidWorkset)
	}

	if err := validateWorksetIdentity(spec); err != nil {
		return err
	}

	if err := validateWorksetScope(spec.AccountID, spec.ProjectID, spec.ProcessID, spec.Resource, core.ErrInvalidWorkset); err != nil {
		return err
	}

	if err := validateWorksetItemsRef(&spec.ItemsRef); err != nil {
		return err
	}

	return validateWorksetChunks(spec.Chunks, spec.Policy)
}

// ValidateWorksetRef validates a tenant-scoped workset reference.
func ValidateWorksetRef(ref WorksetRef) error {
	switch {
	case ref.WorksetID == "":
		return fmt.Errorf("%w: workset id is required", core.ErrInvalidWorksetScope)
	case ref.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidWorksetScope)
	case ref.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidWorksetScope)
	default:
		return nil
	}
}

// ValidateWorksetScope validates a workset query scope.
func ValidateWorksetScope(scope *WorksetScope) error {
	if scope == nil {
		return fmt.Errorf("%w: workset scope is required", core.ErrInvalidWorksetScope)
	}

	switch {
	case scope.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidWorksetScope)
	case scope.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidWorksetScope)
	case scope.Limit < 0:
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidWorksetScope)
	default:
		return validateWorksetResource(scope.AccountID, scope.ProjectID, scope.Resource, core.ErrInvalidWorksetScope)
	}
}

func validateWorksetIdentity(spec *WorksetSpec) error {
	switch {
	case spec.WorksetID == "":
		return fmt.Errorf("%w: workset id is required", core.ErrInvalidWorkset)
	case spec.IdempotencyKey == "":
		return fmt.Errorf("%w: idempotency key is required", core.ErrInvalidWorkset)
	case spec.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidWorkset)
	case spec.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidWorkset)
	case spec.Kind == "":
		return fmt.Errorf("%w: kind is required", core.ErrInvalidWorkset)
	default:
		return nil
	}
}

func validateWorksetScope(accountID, projectID, processID string, resource ResourceRef, scopeErr error) error {
	if processID == "" && resource.Kind == "" {
		return fmt.Errorf("%w: process id or resource ref is required", scopeErr)
	}

	return validateWorksetResource(accountID, projectID, resource, scopeErr)
}

func validateWorksetResource(accountID, projectID string, resource ResourceRef, scopeErr error) error {
	if resource.Kind == "" && resource.ResourceID == "" && resource.AccountID == "" && resource.ProjectID == "" {
		return nil
	}

	if err := ValidateResourceRef(resource); err != nil {
		return fmt.Errorf("%w: %w", scopeErr, err)
	}

	if resource.AccountID != accountID || resource.ProjectID != projectID {
		return fmt.Errorf("%w: resource must be in workset tenant scope", scopeErr)
	}

	return nil
}

func validateWorksetItemsRef(ref *WorksetItemsRef) error {
	switch {
	case ref.Kind == "":
		return fmt.Errorf("%w: items ref kind is required", core.ErrInvalidWorkset)
	case ref.URI == "" && ref.ArtifactID == "":
		return fmt.Errorf("%w: items ref uri or artifact id is required", core.ErrInvalidWorkset)
	case ref.Count < 0:
		return fmt.Errorf("%w: items ref count must be non-negative", core.ErrInvalidWorkset)
	default:
		return nil
	}
}

func validateWorksetChunks(chunks []WorksetChunkSpec, policy WorksetPolicy) error {
	if len(chunks) > math.MaxInt32 {
		return fmt.Errorf("%w: chunk count exceeds int32 limit", core.ErrInvalidWorkset)
	}

	if policy.MaxChunks > 0 && len(chunks) > int(policy.MaxChunks) {
		return fmt.Errorf("%w: chunk count exceeds policy", core.ErrInvalidWorkset)
	}

	for i := range chunks {
		if err := validateWorksetChunk(&chunks[i], policy); err != nil {
			return err
		}
	}

	return nil
}

func validateWorksetChunk(chunk *WorksetChunkSpec, policy WorksetPolicy) error {
	switch {
	case chunk.ChunkID == "":
		return fmt.Errorf("%w: chunk id is required", core.ErrInvalidWorkset)
	case chunk.ItemCount < 0:
		return fmt.Errorf("%w: chunk item count must be non-negative", core.ErrInvalidWorkset)
	case policy.MaxChunkSize > 0 && chunk.ItemCount > policy.MaxChunkSize:
		return fmt.Errorf("%w: chunk item count exceeds policy", core.ErrInvalidWorkset)
	case policy.MaxConcurrency > 0 && chunk.Concurrency > policy.MaxConcurrency:
		return fmt.Errorf("%w: chunk concurrency exceeds policy", core.ErrInvalidWorkset)
	default:
		return validateWorksetItemsRef(&chunk.ItemsRef)
	}
}
