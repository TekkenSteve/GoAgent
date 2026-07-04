package agentos

import (
	"fmt"
	"maps"
	"time"
)

// ResourceKind identifies a business resource type without making AgentOS own
// the domain model.
type ResourceKind string

// ProcessKind identifies a durable process template owned by a platform
// distribution or application.
type ProcessKind string

// ResourceRef is a stable tenant-scoped pointer to an application-owned domain
// object.
type ResourceRef struct {
	Kind       ResourceKind `json:"kind"`
	ResourceID string       `json:"resource_id"`
	AccountID  string       `json:"account_id"`
	ProjectID  string       `json:"project_id"`
}

// ProcessRef identifies a durable process inside an account/project boundary.
type ProcessRef struct {
	ProcessID string `json:"process_id"`
	AccountID string `json:"account_id"`
	ProjectID string `json:"project_id"`
}

// ProcessSpec describes one coarse-grained durable process for a domain
// resource. Backend runs, plans, tools, and framework internals remain behind
// their own runtime boundaries.
type ProcessSpec struct {
	ProcessID      string             `json:"process_id"`
	Kind           ProcessKind        `json:"kind"`
	AccountID      string             `json:"account_id"`
	ProjectID      string             `json:"project_id"`
	IdempotencyKey string             `json:"idempotency_key"`
	Resource       ResourceRef        `json:"resource"`
	RequestedAt    time.Time          `json:"requested_at,omitzero" schema:"optional"`
	Inputs         map[string]any     `json:"inputs,omitempty"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Policy         ProcessPolicy      `json:"policy,omitzero" schema:"optional"`
	Timers         []ProcessTimerSpec `json:"timers,omitempty"`
}

// ProcessPolicy constrains one durable process workflow.
type ProcessPolicy struct {
	TimeoutSeconds      int64 `json:"timeout_seconds,omitempty"`
	MaxHistoryEvents    int32 `json:"max_history_events,omitempty"`
	ContinueAsNewEvents int32 `json:"continue_as_new_events,omitempty"`
}

// ProcessTimerSpec declares a durable timer owned by the process workflow.
type ProcessTimerSpec struct {
	TimerID      string         `json:"timer_id"`
	FireAt       time.Time      `json:"fire_at,omitzero" schema:"optional"`
	AfterSeconds int64          `json:"after_seconds,omitempty"`
	Signal       SignalType     `json:"signal,omitempty"`
	Payload      map[string]any `json:"payload,omitempty"`
}

// ProcessStatus is the public lifecycle projection for a durable process.
type ProcessStatus struct {
	ProcessID      string            `json:"process_id"`
	Kind           ProcessKind       `json:"kind,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	ProjectID      string            `json:"project_id,omitempty"`
	Resource       ResourceRef       `json:"resource,omitzero" schema:"optional"`
	LifecycleState string            `json:"lifecycle_state"`
	Reason         string            `json:"reason,omitempty"`
	Progress       *RunProgress      `json:"progress,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	StartedAt      time.Time         `json:"started_at,omitzero" schema:"optional"`
	UpdatedAt      time.Time         `json:"updated_at,omitzero" schema:"optional"`
}

// ProcessDescription is the read-oriented process view for REST, MCP, UI, and
// operators.
type ProcessDescription struct {
	ProcessID string             `json:"process_id"`
	Kind      ProcessKind        `json:"kind"`
	AccountID string             `json:"account_id"`
	ProjectID string             `json:"project_id"`
	Resource  ResourceRef        `json:"resource"`
	Status    ProcessStatus      `json:"status"`
	Policy    ProcessPolicy      `json:"policy,omitzero" schema:"optional"`
	Timers    []ProcessTimerSpec `json:"timers,omitempty"`
	Metadata  map[string]string  `json:"metadata,omitempty"`
	UpdatedAt time.Time          `json:"updated_at,omitzero" schema:"optional"`
}

// ProcessStreamScope selects events for a durable process.
type ProcessStreamScope struct {
	ProcessID     string `json:"process_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
}

// ProcessEventScope selects durable process events for timeline queries.
type ProcessEventScope struct {
	ProcessID     string `json:"process_id"`
	AccountID     string `json:"account_id"`
	ProjectID     string `json:"project_id"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// ProcessEvent is the public event envelope for process-level events.
type ProcessEvent struct {
	Event
	ProcessID string      `json:"process_id"`
	AccountID string      `json:"account_id"`
	ProjectID string      `json:"project_id"`
	Resource  ResourceRef `json:"resource,omitzero" schema:"optional"`
}

const processEventExtraFields = 5

// ToEvent projects the process-scoped event into the generic stream envelope
// used by Subscription.
func (event *ProcessEvent) ToEvent() Event {
	if event == nil {
		return Event{}
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
		return fmt.Errorf("%w: resource kind is required", ErrInvalidResourceRef)
	case ref.ResourceID == "":
		return fmt.Errorf("%w: resource id is required", ErrInvalidResourceRef)
	case ref.AccountID == "":
		return fmt.Errorf("%w: account id is required", ErrInvalidResourceRef)
	case ref.ProjectID == "":
		return fmt.Errorf("%w: project id is required", ErrInvalidResourceRef)
	default:
		return nil
	}
}

// ValidateProcessRef validates a tenant-scoped process reference.
func ValidateProcessRef(ref ProcessRef) error {
	switch {
	case ref.ProcessID == "":
		return fmt.Errorf("%w: process id is required", ErrInvalidProcessScope)
	case ref.AccountID == "":
		return fmt.Errorf("%w: account id is required", ErrInvalidProcessScope)
	case ref.ProjectID == "":
		return fmt.Errorf("%w: project id is required", ErrInvalidProcessScope)
	default:
		return nil
	}
}

// ValidateProcessSpec validates the generic durable-process contract.
func ValidateProcessSpec(spec *ProcessSpec) error {
	if spec == nil {
		return fmt.Errorf("%w: process spec is required", ErrInvalidProcess)
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
func ValidateProcessStreamScope(scope *ProcessStreamScope) error {
	if scope == nil {
		return fmt.Errorf("%w: process stream scope is required", ErrInvalidProcessScope)
	}

	return ValidateProcessRef(ProcessRef{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
}

// ValidateProcessEventScope validates a durable process event query scope.
func ValidateProcessEventScope(scope *ProcessEventScope) error {
	if scope == nil {
		return fmt.Errorf("%w: process event scope is required", ErrInvalidProcessScope)
	}

	if scope.Limit < 0 {
		return fmt.Errorf("%w: limit must be non-negative", ErrInvalidProcessScope)
	}

	return ValidateProcessRef(ProcessRef{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
}

// ProcessSpecJSONSchema returns a JSON Schema inferred from ProcessSpec.
func ProcessSpecJSONSchema() ([]byte, error) {
	return jsonSchemaFor[ProcessSpec]()
}

func validateProcessStartIdentity(spec *ProcessSpec) error {
	switch {
	case spec.ProcessID == "":
		return fmt.Errorf("%w: process id is required", ErrInvalidProcess)
	case spec.Kind == "":
		return fmt.Errorf("%w: process kind is required", ErrInvalidProcess)
	case spec.AccountID == "":
		return fmt.Errorf("%w: account id is required", ErrInvalidProcess)
	case spec.ProjectID == "":
		return fmt.Errorf("%w: project id is required", ErrInvalidProcess)
	case spec.IdempotencyKey == "":
		return fmt.Errorf("%w: idempotency key is required", ErrInvalidProcess)
	}

	if err := ValidateResourceRef(spec.Resource); err != nil {
		return err
	}

	if spec.Resource.AccountID != spec.AccountID {
		return fmt.Errorf("%w: resource account %q does not match process account %q", ErrInvalidProcess, spec.Resource.AccountID, spec.AccountID)
	}

	if spec.Resource.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: resource project %q does not match process project %q", ErrInvalidProcess, spec.Resource.ProjectID, spec.ProjectID)
	}

	return nil
}

func validateProcessPolicy(policy ProcessPolicy) error {
	switch {
	case policy.TimeoutSeconds < 0:
		return fmt.Errorf("%w: timeout seconds must be non-negative", ErrInvalidProcess)
	case policy.MaxHistoryEvents < 0:
		return fmt.Errorf("%w: max history events must be non-negative", ErrInvalidProcess)
	case policy.ContinueAsNewEvents < 0:
		return fmt.Errorf("%w: continue-as-new events must be non-negative", ErrInvalidProcess)
	case policy.MaxHistoryEvents > 0 && policy.ContinueAsNewEvents > policy.MaxHistoryEvents:
		return fmt.Errorf("%w: continue-as-new events %d exceeds max history events %d", ErrInvalidProcess, policy.ContinueAsNewEvents, policy.MaxHistoryEvents)
	default:
		return nil
	}
}

func validateProcessTimer(timer ProcessTimerSpec) error {
	switch {
	case timer.TimerID == "":
		return fmt.Errorf("%w: timer id is required", ErrInvalidProcess)
	case !timer.FireAt.IsZero() && timer.AfterSeconds > 0:
		return fmt.Errorf("%w: timer %q must use fire_at or after_seconds, not both", ErrInvalidProcess, timer.TimerID)
	case timer.FireAt.IsZero() && timer.AfterSeconds <= 0:
		return fmt.Errorf("%w: timer %q schedule is required", ErrInvalidProcess, timer.TimerID)
	case timer.AfterSeconds < 0:
		return fmt.Errorf("%w: timer %q after_seconds must be non-negative", ErrInvalidProcess, timer.TimerID)
	default:
		return nil
	}
}
