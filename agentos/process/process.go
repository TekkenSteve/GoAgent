package process

import (
	"fmt"
	"maps"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// ResourceKind identifies a business resource type without making AgentOS own
// the domain model.
type ResourceKind string

// Kind identifies a durable process template owned by a platform
// distribution or application.
type Kind string

// ResourceRef is a stable tenant-scoped pointer to an application-owned domain
// object.
type ResourceRef struct {
	Kind       ResourceKind `json:"kind"`
	ResourceID string       `json:"resource_id"`
	AccountID  string       `json:"account_id"`
	ProjectID  string       `json:"project_id"`
}

// Ref identifies a durable process inside an account/project boundary.
type Ref struct {
	ProcessID string `json:"process_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// Spec describes one coarse-grained durable process for a domain
// resource. Backend runs, plans, tools, and framework internals remain behind
// their own runtime boundaries.
type Spec struct {
	ProcessID      string            `json:"process_id"`
	Kind           Kind              `json:"kind"`
	AccountID      string            `json:"account_id"`
	ProjectID      string            `json:"project_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	Resource       ResourceRef       `json:"resource"`
	RequestedAt    time.Time         `json:"requested_at,omitzero" schema:"optional"`
	Inputs         map[string]any    `json:"inputs,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Policy         Policy            `json:"policy,omitzero" schema:"optional"`
	Timers         []TimerSpec       `json:"timers,omitempty"`
}

// Policy constrains one durable process workflow.
type Policy struct {
	TimeoutSeconds      int64 `json:"timeout_seconds,omitempty"`
	MaxHistoryEvents    int32 `json:"max_history_events,omitempty"`
	ContinueAsNewEvents int32 `json:"continue_as_new_events,omitempty"`
}

// TimerSpec declares a durable timer owned by the process workflow.
type TimerSpec struct {
	TimerID      string          `json:"timer_id"`
	FireAt       time.Time       `json:"fire_at,omitzero" schema:"optional"`
	AfterSeconds int64           `json:"after_seconds,omitempty"`
	Signal       core.SignalType `json:"signal,omitempty"`
	Payload      map[string]any  `json:"payload,omitempty"`
}

// Status is the public lifecycle projection for a durable process.
type Status struct {
	ProcessID      string            `json:"process_id"`
	Kind           Kind              `json:"kind,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	ProjectID      string            `json:"project_id,omitempty"`
	Resource       ResourceRef       `json:"resource,omitzero" schema:"optional"`
	LifecycleState string            `json:"lifecycle_state"`
	Reason         string            `json:"reason,omitempty"`
	Progress       *core.RunProgress `json:"progress,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	StartedAt      time.Time         `json:"started_at,omitzero" schema:"optional"`
	UpdatedAt      time.Time         `json:"updated_at,omitzero" schema:"optional"`
}

// Description is the read-oriented process view for REST, MCP, UI, and
// operators.
type Description struct {
	ProcessID string            `json:"process_id"`
	Kind      Kind              `json:"kind"`
	AccountID string            `json:"account_id"`
	ProjectID string            `json:"project_id"`
	Resource  ResourceRef       `json:"resource"`
	Status    Status            `json:"status"`
	Policy    Policy            `json:"policy,omitzero" schema:"optional"`
	Timers    []TimerSpec       `json:"timers,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	UpdatedAt time.Time         `json:"updated_at,omitzero" schema:"optional"`
}

// StreamScope selects events for a durable process.
type StreamScope struct {
	ProcessID     string `json:"process_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
}

// EventScope selects durable process events for timeline queries.
type EventScope struct {
	ProcessID     string `json:"process_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// Scope selects durable process projections for a tenant, resource, process
// kind, or lifecycle state.
type Scope struct {
	AccountID      string       `json:"account_id"`
	ProjectID      string       `json:"project_id"`
	Resource       ResourceRef  `json:"resource,omitzero" schema:"optional"`
	ResourceKind   ResourceKind `json:"resource_kind,omitempty"`
	Kind           Kind         `json:"kind,omitempty"`
	LifecycleState string       `json:"lifecycle_state,omitempty"`
	Limit          int          `json:"limit,omitempty"`
}

// Event is the public event envelope for process-level events.
type Event struct {
	core.Event
	ProcessID string      `json:"process_id"`
	AccountID string      `json:"account_id"`
	ProjectID string      `json:"project_id"`
	Resource  ResourceRef `json:"resource,omitzero" schema:"optional"`
}

const processEventExtraFields = 5

// ToEvent projects the process-scoped event into the generic stream envelope
// used by Subscription.
func (event *Event) ToEvent() core.Event {
	if event == nil {
		return core.Event{}
	}

	generic := event.Event
	payload := make(map[string]any, len(generic.Payload)+processEventExtraFields)
	maps.Copy(payload, generic.Payload)

	payload["process_id"] = event.ProcessID
	payload["account_id"] = event.AccountID
	payload["project_id"] = event.ProjectID
	payload["resource_kind"] = string(event.Resource.Kind)
	payload["resource_id"] = event.Resource.ResourceID

	generic.ProcessID = event.ProcessID
	generic.Payload = payload

	return generic
}

// Process lifecycle constants.
const (
	ProcessPending   = "pending"
	ProcessRunning   = "running"
	ProcessWaiting   = "waiting"
	ProcessBlocked   = "blocked"
	ProcessSucceeded = "succeeded"
	ProcessFailed    = "failed"
	ProcessCanceled  = "canceled"
)

// ValidateResourceRef validates a tenant-scoped resource reference.
func ValidateResourceRef(ref ResourceRef) error {
	switch {
	case ref.Kind == "":
		return fmt.Errorf("%w: resource kind is required", core.ErrInvalidResourceRef)
	case ref.ResourceID == "":
		return fmt.Errorf("%w: resource id is required", core.ErrInvalidResourceRef)
	case ref.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidResourceRef)
	case ref.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidResourceRef)
	default:
		return nil
	}
}

// ValidateProcessRef validates a tenant-scoped process reference.
func ValidateProcessRef(ref Ref) error {
	switch {
	case ref.ProcessID == "":
		return fmt.Errorf("%w: process id is required", core.ErrInvalidProcessScope)
	case ref.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidProcessScope)
	case ref.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidProcessScope)
	default:
		return nil
	}
}

// ValidateProcessSpec validates the generic durable-process contract.
func ValidateProcessSpec(spec *Spec) error {
	if spec == nil {
		return fmt.Errorf("%w: process spec is required", core.ErrInvalidProcess)
	}

	if err := validateProcessStartIdentity(spec); err != nil {
		return err
	}

	if err := validateProcessPolicy(spec.Policy); err != nil {
		return err
	}

	for _, timer := range spec.Timers {
		if err := validateProcessTimer(timer); err != nil {
			return err
		}
	}

	return nil
}

// ValidateProcessStreamScope validates a process event stream scope.
func ValidateProcessStreamScope(scope *StreamScope) error {
	if scope == nil {
		return fmt.Errorf("%w: process stream scope is required", core.ErrInvalidProcessScope)
	}

	return ValidateProcessRef(Ref{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
}

// ValidateProcessEventScope validates a durable process event query scope.
func ValidateProcessEventScope(scope *EventScope) error {
	if scope == nil {
		return fmt.Errorf("%w: process event scope is required", core.ErrInvalidProcessScope)
	}

	if scope.Limit < 0 {
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidProcessScope)
	}

	return ValidateProcessRef(Ref{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
}

// ValidateScope validates a durable process projection query scope.
func ValidateScope(scope *Scope) error {
	if scope == nil {
		return fmt.Errorf("%w: process scope is required", core.ErrInvalidProcessScope)
	}

	if scope.AccountID == "" {
		return fmt.Errorf("%w: account id is required", core.ErrInvalidProcessScope)
	}

	if scope.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", core.ErrInvalidProcessScope)
	}

	if scope.Limit < 0 {
		return fmt.Errorf("%w: limit must be non-negative", core.ErrInvalidProcessScope)
	}

	return validateOptionalScopeResource(scope.AccountID, scope.ProjectID, scope.Resource)
}

func validateOptionalScopeResource(accountID, projectID string, resource ResourceRef) error {
	if resource.Kind == "" && resource.ResourceID == "" && resource.AccountID == "" && resource.ProjectID == "" {
		return nil
	}

	if err := ValidateResourceRef(resource); err != nil {
		return fmt.Errorf("%w: %w", core.ErrInvalidProcessScope, err)
	}

	if resource.AccountID != accountID || resource.ProjectID != projectID {
		return fmt.Errorf("%w: resource must be in process tenant scope", core.ErrInvalidProcessScope)
	}

	return nil
}

// SpecJSONSchema returns a JSON Schema inferred from Spec.
func SpecJSONSchema() ([]byte, error) {
	return core.JSONSchemaFor[Spec]()
}

func validateProcessStartIdentity(spec *Spec) error {
	switch {
	case spec.ProcessID == "":
		return fmt.Errorf("%w: process id is required", core.ErrInvalidProcess)
	case spec.Kind == "":
		return fmt.Errorf("%w: process kind is required", core.ErrInvalidProcess)
	case spec.AccountID == "":
		return fmt.Errorf("%w: account id is required", core.ErrInvalidProcess)
	case spec.ProjectID == "":
		return fmt.Errorf("%w: project id is required", core.ErrInvalidProcess)
	case spec.IdempotencyKey == "":
		return fmt.Errorf("%w: idempotency key is required", core.ErrInvalidProcess)
	}

	if err := ValidateResourceRef(spec.Resource); err != nil {
		return err
	}

	if spec.Resource.AccountID != spec.AccountID {
		return fmt.Errorf("%w: resource account %q does not match process account %q", core.ErrInvalidProcess, spec.Resource.AccountID, spec.AccountID)
	}

	if spec.Resource.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: resource project %q does not match process project %q", core.ErrInvalidProcess, spec.Resource.ProjectID, spec.ProjectID)
	}

	return nil
}

func validateProcessPolicy(policy Policy) error {
	switch {
	case policy.TimeoutSeconds < 0:
		return fmt.Errorf("%w: timeout seconds must be non-negative", core.ErrInvalidProcess)
	case policy.MaxHistoryEvents < 0:
		return fmt.Errorf("%w: max history events must be non-negative", core.ErrInvalidProcess)
	case policy.ContinueAsNewEvents < 0:
		return fmt.Errorf("%w: continue-as-new events must be non-negative", core.ErrInvalidProcess)
	case policy.MaxHistoryEvents > 0 && policy.ContinueAsNewEvents > policy.MaxHistoryEvents:
		return fmt.Errorf("%w: continue-as-new events %d exceeds max history events %d", core.ErrInvalidProcess, policy.ContinueAsNewEvents, policy.MaxHistoryEvents)
	default:
		return nil
	}
}

func validateProcessTimer(timer TimerSpec) error {
	switch {
	case timer.TimerID == "":
		return fmt.Errorf("%w: timer id is required", core.ErrInvalidProcess)
	case !timer.FireAt.IsZero() && timer.AfterSeconds > 0:
		return fmt.Errorf("%w: timer %q must use fire_at or after_seconds, not both", core.ErrInvalidProcess, timer.TimerID)
	case timer.FireAt.IsZero() && timer.AfterSeconds <= 0:
		return fmt.Errorf("%w: timer %q schedule is required", core.ErrInvalidProcess, timer.TimerID)
	case timer.AfterSeconds < 0:
		return fmt.Errorf("%w: timer %q after_seconds must be non-negative", core.ErrInvalidProcess, timer.TimerID)
	default:
		return nil
	}
}
