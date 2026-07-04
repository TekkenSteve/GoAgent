package agentosaction

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Runtime coordinates governed action lifecycle state.
type Runtime struct {
	store Store
}

// NewRuntime creates a generic governed action use case.
func NewRuntime(store Store) (*Runtime, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: action store is required", agentos.ErrInvalidGovernedActionScope)
	}

	return &Runtime{store: store}, nil
}

// RequestAction claims one governed action and returns its initial lifecycle
// projection.
func (r *Runtime) RequestAction(ctx context.Context, spec *agentos.GovernedActionSpec) (agentos.GovernedActionStatus, error) {
	if err := agentos.ValidateGovernedActionSpec(spec); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	status := initialActionStatus(spec)

	status, _, err := r.store.CreateAction(ctx, spec, &status)

	return status, err
}

// StatusAction returns the latest action projection.
func (r *Runtime) StatusAction(ctx context.Context, ref agentos.ActionRef) (agentos.GovernedActionStatus, error) {
	_, status, exists, err := r.store.GetAction(ctx, ref)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if !exists {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q not found", agentos.ErrInvalidGovernedActionScope, ref.ActionID)
	}

	return status, nil
}

// ListActions returns action projections for a tenant-scoped query.
func (r *Runtime) ListActions(ctx context.Context, scope *agentos.ActionScope) ([]agentos.GovernedActionStatus, error) {
	if err := agentos.ValidateActionScope(scope); err != nil {
		return nil, err
	}

	return r.store.ListActions(ctx, scope)
}

// RecordActionDryRun records a non-mutating preview result.
func (r *Runtime) RecordActionDryRun(
	ctx context.Context,
	ref agentos.ActionRef,
	result *agentos.ActionDryRunResult,
) (agentos.GovernedActionStatus, error) {
	spec, status, err := r.requireAction(ctx, ref)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if err := validateDryRunResult(result); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if status.DryRunState == "" {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q does not require dry-run", agentos.ErrInvalidGovernedAction, ref.ActionID)
	}

	next := status

	next.Risk = result.Risk
	if next.Risk.Level == "" {
		next.Risk = status.Risk
	}

	next.UpdatedAt = actionTimestamp(result.RecordedAt)
	if result.Succeeded {
		next.DryRunState = agentos.ActionDryRunSucceeded
		next.LifecycleState = lifecycleAfterDryRun(&spec)
	} else {
		next.DryRunState = agentos.ActionDryRunFailed
		next.LifecycleState = agentos.ActionFailed
		next.ExecutionState = agentos.ActionExecutionFailed
		next.Reason = result.Summary
	}

	return r.store.UpdateActionStatus(ctx, &next, result.IdempotencyKey)
}

// ResolveActionApproval records approval gate resolution.
func (r *Runtime) ResolveActionApproval(
	ctx context.Context,
	ref agentos.ActionRef,
	decision *agentos.ActionApprovalDecision,
) (agentos.GovernedActionStatus, error) {
	spec, status, err := r.requireAction(ctx, ref)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if err := validateApprovalDecision(decision); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if status.ApprovalState == "" {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q does not require approval", agentos.ErrInvalidGovernedAction, ref.ActionID)
	}

	next := status

	next.UpdatedAt = actionTimestamp(decision.DecidedAt)
	if decision.Approved {
		next.ApprovalState = agentos.ActionApprovalApproved
		next.LifecycleState = lifecycleAfterApproval(&spec, next.DryRunState)
	} else {
		next.ApprovalState = agentos.ActionApprovalRejected
		next.LifecycleState = agentos.ActionCanceled
		next.Reason = decision.Reason
	}

	return r.store.UpdateActionStatus(ctx, &next, decision.IdempotencyKey)
}

// CompleteAction records the external execution outcome.
func (r *Runtime) CompleteAction(
	ctx context.Context,
	ref agentos.ActionRef,
	result *agentos.ActionExecutionResult,
) (agentos.GovernedActionStatus, error) {
	_, status, err := r.requireAction(ctx, ref)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if err := validateExecutionResult(result); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if !actionReadyForExecution(&status) {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q is not ready for execution", agentos.ErrInvalidGovernedAction, ref.ActionID)
	}

	next := status

	next.UpdatedAt = actionTimestamp(result.RecordedAt)
	if result.Succeeded {
		next.LifecycleState = agentos.ActionExecuted
		next.ExecutionState = agentos.ActionExecutionSucceeded
	} else {
		next.LifecycleState = agentos.ActionFailed
		next.ExecutionState = agentos.ActionExecutionFailed
		next.Reason = result.Summary
	}

	return r.store.UpdateActionStatus(ctx, &next, result.IdempotencyKey)
}

// CancelAction records an action cancellation.
func (r *Runtime) CancelAction(
	ctx context.Context,
	ref agentos.ActionRef,
	req *agentos.ActionCancelRequest,
) (agentos.GovernedActionStatus, error) {
	_, status, err := r.requireAction(ctx, ref)
	if err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if err := validateCancelRequest(req); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	if status.LifecycleState == agentos.ActionExecuted {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q is already executed", agentos.ErrInvalidGovernedAction, ref.ActionID)
	}

	next := status
	next.LifecycleState = agentos.ActionCanceled
	next.Reason = req.Reason
	next.UpdatedAt = actionTimestamp(req.RequestedAt)

	return r.store.UpdateActionStatus(ctx, &next, req.IdempotencyKey)
}

func (r *Runtime) requireAction(
	ctx context.Context,
	ref agentos.ActionRef,
) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, error) {
	if err := agentos.ValidateActionRef(ref); err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, err
	}

	spec, status, exists, err := r.store.GetAction(ctx, ref)
	if err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, err
	}

	if !exists {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q not found", agentos.ErrInvalidGovernedActionScope, ref.ActionID)
	}

	return spec, status, nil
}

func initialActionStatus(spec *agentos.GovernedActionSpec) agentos.GovernedActionStatus {
	now := actionTimestamp(spec.RequestedAt)
	status := agentos.GovernedActionStatus{
		ActionID:       spec.ActionID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		ProcessID:      spec.ProcessID,
		Resource:       spec.Resource,
		Kind:           spec.Kind,
		LifecycleState: agentos.ActionReady,
		ExecutionState: agentos.ActionExecutionPending,
		Risk:           spec.Risk,
		UpdatedAt:      now,
	}

	if spec.DryRunRequired {
		status.DryRunState = agentos.ActionDryRunPending
		status.LifecycleState = agentos.ActionWaitingDryRun
	}

	if spec.ApprovalRequired {
		status.ApprovalState = agentos.ActionApprovalPending
		if !spec.DryRunRequired {
			status.LifecycleState = agentos.ActionWaitingApproval
		}
	}

	return status
}

func lifecycleAfterDryRun(spec *agentos.GovernedActionSpec) string {
	if spec.ApprovalRequired {
		return agentos.ActionWaitingApproval
	}

	return agentos.ActionReady
}

func lifecycleAfterApproval(spec *agentos.GovernedActionSpec, dryRunState string) string {
	if spec.DryRunRequired && dryRunState != agentos.ActionDryRunSucceeded {
		return agentos.ActionWaitingDryRun
	}

	return agentos.ActionReady
}

func actionReadyForExecution(status *agentos.GovernedActionStatus) bool {
	if status.LifecycleState != agentos.ActionReady && status.LifecycleState != agentos.ActionExecuting {
		return false
	}

	if status.DryRunState != "" && status.DryRunState != agentos.ActionDryRunSucceeded {
		return false
	}

	if status.ApprovalState != "" && status.ApprovalState != agentos.ActionApprovalApproved {
		return false
	}

	return true
}

func validateDryRunResult(result *agentos.ActionDryRunResult) error {
	if result == nil {
		return fmt.Errorf("%w: dry-run result is required", agentos.ErrInvalidGovernedAction)
	}

	if result.IdempotencyKey == "" {
		return fmt.Errorf("%w: dry-run idempotency key is required", agentos.ErrInvalidGovernedAction)
	}

	return nil
}

func validateApprovalDecision(decision *agentos.ActionApprovalDecision) error {
	if decision == nil {
		return fmt.Errorf("%w: approval decision is required", agentos.ErrInvalidGovernedAction)
	}

	if decision.IdempotencyKey == "" {
		return fmt.Errorf("%w: approval idempotency key is required", agentos.ErrInvalidGovernedAction)
	}

	return nil
}

func validateExecutionResult(result *agentos.ActionExecutionResult) error {
	if result == nil {
		return fmt.Errorf("%w: execution result is required", agentos.ErrInvalidGovernedAction)
	}

	if result.IdempotencyKey == "" {
		return fmt.Errorf("%w: execution idempotency key is required", agentos.ErrInvalidGovernedAction)
	}

	return nil
}

func validateCancelRequest(req *agentos.ActionCancelRequest) error {
	if req == nil {
		return fmt.Errorf("%w: cancel request is required", agentos.ErrInvalidGovernedAction)
	}

	if req.IdempotencyKey == "" {
		return fmt.Errorf("%w: cancel idempotency key is required", agentos.ErrInvalidGovernedAction)
	}

	return nil
}

func actionTimestamp(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Now().UTC()
	}

	return ts
}
