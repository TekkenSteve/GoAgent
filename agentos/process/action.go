package process

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// ActionKind identifies an application-defined governed action type without
// making AgentOS own the business domain.
type ActionKind string

// ActionRef identifies a governed action inside an account/project boundary.
type ActionRef struct {
	ActionID  string `json:"action_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// ActionRiskLevel describes the operator-facing risk class for a governed
// action.
type ActionRiskLevel string

const (
	ActionRiskLow      ActionRiskLevel = "low"
	ActionRiskMedium   ActionRiskLevel = "medium"
	ActionRiskHigh     ActionRiskLevel = "high"
	ActionRiskCritical ActionRiskLevel = "critical"
)

// ActionRiskAssessment records the current risk evaluation for a governed
// action. Evidence must be referenced rather than embedded.
type ActionRiskAssessment struct {
	Level        ActionRiskLevel `json:"level,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	EvidenceRefs []LedgerDataRef `json:"evidence_refs,omitempty"`
	AssessedAt   time.Time       `json:"assessed_at,omitzero" schema:"optional"`
}

// GovernedActionSpec requests one coarse-grained action that may require
// dry-run, risk review, approval, execution, cancellation, and compensation.
type GovernedActionSpec struct {
	ActionID         string               `json:"action_id"`
	IdempotencyKey   string               `json:"idempotency_key"`
	AccountID        string               `json:"account_id"`
	ProjectID        string               `json:"project_id"`
	ProcessID        string               `json:"process_id,omitempty"`
	Resource         ResourceRef          `json:"resource,omitzero" schema:"optional"`
	Kind             ActionKind           `json:"kind"`
	Intent           string               `json:"intent,omitempty"`
	RequestedBy      ActorRef             `json:"requested_by,omitzero" schema:"optional"`
	RequestedAt      time.Time            `json:"requested_at,omitzero" schema:"optional"`
	DryRunRequired   bool                 `json:"dry_run_required,omitempty"`
	ApprovalRequired bool                 `json:"approval_required,omitempty"`
	Risk             ActionRiskAssessment `json:"risk,omitzero" schema:"optional"`
	InputRefs        []LedgerDataRef      `json:"input_refs,omitempty"`
	CompensationRef  *LedgerDataRef       `json:"compensation_ref,omitempty"`
	Metadata         map[string]string    `json:"metadata,omitempty"`
}

// GovernedActionStatus is the public lifecycle projection for one governed
// action.
type GovernedActionStatus struct {
	ActionID       string               `json:"action_id"`
	AccountID      string               `json:"account_id"`
	ProjectID      string               `json:"project_id"`
	ProcessID      string               `json:"process_id,omitempty"`
	Resource       ResourceRef          `json:"resource,omitzero" schema:"optional"`
	Kind           ActionKind           `json:"kind,omitempty"`
	LifecycleState string               `json:"lifecycle_state"`
	DryRunState    string               `json:"dry_run_state,omitempty"`
	ApprovalState  string               `json:"approval_state,omitempty"`
	ExecutionState string               `json:"execution_state,omitempty"`
	Risk           ActionRiskAssessment `json:"risk,omitzero" schema:"optional"`
	Reason         string               `json:"reason,omitempty"`
	UpdatedAt      time.Time            `json:"updated_at,omitzero" schema:"optional"`
}

// ActionDryRunResult records the result of a non-mutating preview.
type ActionDryRunResult struct {
	IdempotencyKey string               `json:"idempotency_key"`
	Succeeded      bool                 `json:"succeeded"`
	Summary        string               `json:"summary,omitempty"`
	Risk           ActionRiskAssessment `json:"risk,omitzero" schema:"optional"`
	OutputRefs     []LedgerDataRef      `json:"output_refs,omitempty"`
	RecordedAt     time.Time            `json:"recorded_at,omitzero" schema:"optional"`
}

// ActionApprovalDecision records a human or policy approval gate decision.
type ActionApprovalDecision struct {
	IdempotencyKey string    `json:"idempotency_key"`
	Approved       bool      `json:"approved"`
	Actor          ActorRef  `json:"actor,omitzero" schema:"optional"`
	Reason         string    `json:"reason,omitempty"`
	DecidedAt      time.Time `json:"decided_at,omitzero" schema:"optional"`
}

// ActionExecutionResult records the execution outcome. Payloads stay outside
// Temporal history and are referenced through data/artifact refs.
type ActionExecutionResult struct {
	IdempotencyKey string             `json:"idempotency_key"`
	Succeeded      bool               `json:"succeeded"`
	Summary        string             `json:"summary,omitempty"`
	OutputRefs     []LedgerDataRef    `json:"output_refs,omitempty"`
	ArtifactRefs   []core.ArtifactRef `json:"artifact_refs,omitempty"`
	RecordedAt     time.Time          `json:"recorded_at,omitzero" schema:"optional"`
}

// ActionCancelRequest records an operator or policy cancellation.
type ActionCancelRequest struct {
	IdempotencyKey string    `json:"idempotency_key"`
	Actor          ActorRef  `json:"actor,omitzero" schema:"optional"`
	Reason         string    `json:"reason,omitempty"`
	RequestedAt    time.Time `json:"requested_at,omitzero" schema:"optional"`
}

// ActionScope selects governed actions for a tenant, process, resource, kind,
// or lifecycle state.
type ActionScope struct {
	AccountID      string      `json:"account_id"`
	ProjectID      string      `json:"project_id"`
	ProcessID      string      `json:"process_id,omitempty"`
	Resource       ResourceRef `json:"resource,omitzero" schema:"optional"`
	Kind           ActionKind  `json:"kind,omitempty"`
	LifecycleState string      `json:"lifecycle_state,omitempty"`
	Limit          int         `json:"limit,omitempty"`
}

const (
	ActionRequested       = "requested"
	ActionWaitingDryRun   = "waiting_dry_run"
	ActionWaitingApproval = "waiting_approval"
	ActionReady           = "ready"
	ActionExecuting       = "executing"
	ActionExecuted        = "executed"
	ActionFailed          = "failed"
	ActionCanceled        = "canceled"

	ActionDryRunPending   = "pending"
	ActionDryRunSucceeded = "succeeded"
	ActionDryRunFailed    = "failed"

	ActionApprovalPending  = "pending"
	ActionApprovalApproved = "approved"
	ActionApprovalRejected = "rejected"

	ActionExecutionPending   = "pending"
	ActionExecutionSucceeded = "succeeded"
	ActionExecutionFailed    = "failed"
)

// ValidateGovernedActionSpec validates a governed action request.
func ValidateGovernedActionSpec(spec *GovernedActionSpec) error {
	if spec == nil {
		return fmt.Errorf("%w: governed action spec is required", core.ErrInvalidGovernedAction)
	}

	if err := validateGovernedActionIdentity(spec); err != nil {
		return err
	}

	if err := validateGovernedActionScope(spec.AccountID, spec.ProjectID, spec.ProcessID, spec.Resource, core.ErrInvalidGovernedAction); err != nil {
		return err
	}

	return validateActionRefs(spec.InputRefs, spec.CompensationRef, spec.Risk.EvidenceRefs)
}

// ValidateActionRef validates a tenant-scoped action reference.
func ValidateActionRef(ref ActionRef) error {
	switch {
	case ref.ActionID == "":
		return fmt.Errorf("%w: action id is required", core.ErrInvalidGovernedActionScope)
	case ref.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidGovernedActionScope)
	case ref.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidGovernedActionScope)
	default:
		return nil
	}
}

// ValidateActionScope validates a governed action query scope.
func ValidateActionScope(scope *ActionScope) error {
	if scope == nil {
		return fmt.Errorf("%w: action scope is required", core.ErrInvalidGovernedActionScope)
	}

	switch {
	case scope.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidGovernedActionScope)
	case scope.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidGovernedActionScope)
	case scope.Limit < 0:
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidGovernedActionScope)
	default:
		return validateGovernedActionResource(scope.AccountID, scope.ProjectID, scope.Resource, core.ErrInvalidGovernedActionScope)
	}
}

func validateGovernedActionIdentity(spec *GovernedActionSpec) error {
	switch {
	case spec.ActionID == "":
		return fmt.Errorf("%w: action id is required", core.ErrInvalidGovernedAction)
	case spec.IdempotencyKey == "":
		return fmt.Errorf("%w: idempotency key is required", core.ErrInvalidGovernedAction)
	case spec.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidGovernedAction)
	case spec.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidGovernedAction)
	case spec.Kind == "":
		return fmt.Errorf("%w: kind is required", core.ErrInvalidGovernedAction)
	default:
		return nil
	}
}

func validateGovernedActionScope(accountID, projectID, processID string, resource ResourceRef, scopeErr error) error {
	if processID == "" && resource.Kind == "" {
		return fmt.Errorf("%w: process id or resource ref is required", scopeErr)
	}

	return validateGovernedActionResource(accountID, projectID, resource, scopeErr)
}

func validateGovernedActionResource(accountID, projectID string, resource ResourceRef, scopeErr error) error {
	if resource.Kind == "" && resource.ResourceID == "" && resource.AccountID == "" && resource.ProjectID == "" {
		return nil
	}

	if err := ValidateResourceRef(resource); err != nil {
		return fmt.Errorf("%w: %w", scopeErr, err)
	}

	if resource.AccountID != accountID || resource.ProjectID != projectID {
		return fmt.Errorf("%w: resource must be in action tenant scope", scopeErr)
	}

	return nil
}

func validateActionRefs(refs []LedgerDataRef, optionalRef *LedgerDataRef, moreRefs []LedgerDataRef) error {
	if err := validateActionDataRefs(refs); err != nil {
		return err
	}

	if optionalRef != nil {
		if err := validateActionDataRef(optionalRef); err != nil {
			return err
		}
	}

	return validateActionDataRefs(moreRefs)
}

func validateActionDataRefs(refs []LedgerDataRef) error {
	for i := range refs {
		if err := validateActionDataRef(&refs[i]); err != nil {
			return err
		}
	}

	return nil
}

func validateActionDataRef(ref *LedgerDataRef) error {
	switch {
	case ref.Kind == "":
		return fmt.Errorf("%w: data ref kind is required", core.ErrInvalidGovernedAction)
	case ref.URI == "" && ref.ArtifactID == "":
		return fmt.Errorf("%w: data ref uri or artifact id is required", core.ErrInvalidGovernedAction)
	default:
		return nil
	}
}
