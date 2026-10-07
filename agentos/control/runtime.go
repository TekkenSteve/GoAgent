package control

import (
	"context"
	"fmt"
	"strings"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// Runtime is the stable embedded AgentOS run-control boundary.
type Runtime interface {
	Start(ctx context.Context, spec *RunSpec) (RunStatus, error)
	Signal(ctx context.Context, ref RunRef, signal *core.Signal) error
	Status(ctx context.Context, ref RunRef) (RunStatus, error)
	Control(ctx context.Context, ref RunRef, control *core.ControlRequest) error
	Subscribe(ctx context.Context, scope core.StreamScope) (core.Subscription, error)
	Close() error
}

// RunRef addresses one run on behalf of a caller: the run, and the tenant the
// caller may reach it through.
//
// Operations that name a run by id cannot take a scope from a request — the
// request does not carry one — so they carry the caller's account instead and
// the runtime enforces it against the run's recorded ownership. A mismatch is
// reported as ErrRunRouteNotFound, not as a permission error: whether another
// account's run exists is not a caller's business.
//
// AccountID is the authenticated account. It is empty only for in-process
// components that already resolved ownership themselves, never for anything
// reachable from a request — core.Principal.RunRef is how a request-derived
// reference is built.
type RunRef struct {
	RunID     string `json:"run_id"`
	AccountID string `json:"account_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

// NewRunRef pairs an authenticated principal's account with a run id. It is
// the sanctioned way to build a reference for a request-addressed run: the
// account half comes from the credential, so a caller cannot reach another
// account's run by knowing its id.
func NewRunRef(principal core.Principal, runID string) (RunRef, error) {
	if err := principal.Validate(); err != nil {
		return RunRef{}, err
	}

	ref := RunRef{RunID: strings.TrimSpace(runID), AccountID: principal.AccountID}
	if err := ref.Validate(); err != nil {
		return RunRef{}, err
	}

	return ref, nil
}

// Validate reports whether the reference names a run.
func (r RunRef) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return fmt.Errorf("%w: run id is required", core.ErrInvalidRunSpec)
	}

	return nil
}

// Matches reports whether the reference may reach the recorded ownership.
//
// An empty AccountID is the in-process case: the caller has no request-derived
// identity to enforce, and says so rather than asserting one. A named account
// must match, and a named project must match as well — a caller that knows the
// project narrows what it can reach.
func (r RunRef) Matches(ownership *RunBackendOwnership) bool {
	accountID := strings.TrimSpace(r.AccountID)
	if accountID != "" && accountID != strings.TrimSpace(ownership.AccountID) {
		return false
	}

	projectID := strings.TrimSpace(r.ProjectID)
	if projectID != "" && projectID != strings.TrimSpace(ownership.ProjectID) {
		return false
	}

	return true
}

// PlanRuntime is the durable cross-backend AgentOS planning boundary.
type PlanRuntime interface {
	StartPlan(ctx context.Context, spec *RunPlanSpec) (RunPlanStatus, error)
	StatusPlan(ctx context.Context, ref PlanRef) (RunPlanStatus, error)
	DescribePlan(ctx context.Context, ref PlanRef) (RunPlanDescription, error)
	SignalPlan(ctx context.Context, ref PlanRef, signal *core.Signal) error
	ControlPlan(ctx context.Context, ref PlanRef, control *core.ControlRequest) error
	IngestExternalPlanEvent(ctx context.Context, event *ExternalPlanEvent) (PlanEvent, error)
	SubscribePlan(ctx context.Context, scope *PlanStreamScope) (core.Subscription, error)
	ListPlanEvents(ctx context.Context, scope *PlanEventScope) ([]PlanEvent, error)
	ListPlanDebugTraces(ctx context.Context, scope *PlanDebugTraceScope) ([]PlanDebugTrace, error)
	ListPlanArtifacts(ctx context.Context, scope *PlanArtifactScope) ([]core.ArtifactRef, error)
	GetPlanArtifact(ctx context.Context, scope *PlanArtifactScope) (core.Artifact, error)
	ListPlanAudits(ctx context.Context, scope *PlanAuditScope) ([]PlanAuditRecord, error)
}
