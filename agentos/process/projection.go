package process

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// ResourceProjectionScope selects the durable read model for one resource.
type ResourceProjectionScope struct {
	Resource ResourceRef `json:"resource"`
	Limit    int         `json:"limit,omitempty"`
}

// ResourceProjectionListScope selects resource projection summaries for a
// tenant, resource kind, and optional lifecycle filter.
type ResourceProjectionListScope struct {
	AccountID      string       `json:"account_id"`
	ProjectID      string       `json:"project_id"`
	ResourceKind   ResourceKind `json:"resource_kind,omitempty"`
	LifecycleState string       `json:"lifecycle_state,omitempty"`
	Limit          int          `json:"limit,omitempty"`
}

// ResourceProjection is the generic read model for a business resource. It is
// assembled from durable process, ledger, governed action, and workset
// projections while keeping the business domain model outside AgentOS core.
type ResourceProjection struct {
	Resource  ResourceRef            `json:"resource"`
	Processes []Status               `json:"processes,omitempty"`
	Ledger    []LedgerEntry          `json:"ledger,omitempty"`
	Actions   []GovernedActionStatus `json:"actions,omitempty"`
	Worksets  []WorksetStatus        `json:"worksets,omitempty"`
	Artifacts []core.ArtifactRef     `json:"artifacts,omitempty"`
	UpdatedAt time.Time              `json:"updated_at,omitzero" schema:"optional"`
	Metadata  map[string]string      `json:"metadata,omitempty"`
}

// ResourceProjectionSummary is a compact row for resource list views.
type ResourceProjectionSummary struct {
	Resource             ResourceRef `json:"resource"`
	LatestProcessID      string      `json:"latest_process_id,omitempty"`
	LatestLifecycleState string      `json:"latest_lifecycle_state,omitempty"`
	ProcessCount         int         `json:"process_count,omitempty"`
	ActionCount          int         `json:"action_count,omitempty"`
	WorksetCount         int         `json:"workset_count,omitempty"`
	LedgerCount          int         `json:"ledger_count,omitempty"`
	UpdatedAt            time.Time   `json:"updated_at,omitzero" schema:"optional"`
}

// ValidateResourceProjectionScope validates a single-resource projection query.
func ValidateResourceProjectionScope(scope *ResourceProjectionScope) error {
	if scope == nil {
		return fmt.Errorf("%w: resource projection scope is required", core.ErrInvalidResourceRef)
	}

	if scope.Limit < 0 {
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidResourceRef)
	}

	return ValidateResourceRef(scope.Resource)
}

// ValidateResourceProjectionListScope validates a resource projection list query.
func ValidateResourceProjectionListScope(scope *ResourceProjectionListScope) error {
	if scope == nil {
		return fmt.Errorf("%w: resource projection list scope is required", core.ErrInvalidResourceRef)
	}

	switch {
	case scope.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidResourceRef)
	case scope.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidResourceRef)
	case scope.Limit < 0:
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidResourceRef)
	default:
		return nil
	}
}
