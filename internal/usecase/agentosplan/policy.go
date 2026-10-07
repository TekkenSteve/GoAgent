package agentosplan

import (
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
)

// ContinuationSnapshot is deterministic input for RunPlan continuation policy.
type ContinuationSnapshot struct {
	AppliedTransitions int32
	HistoryEvents      int
}

// IterationSnapshot is deterministic input for RunPlan loop guard policy.
type IterationSnapshot struct {
	Iterations int32
}

// ContinuationDecision explains whether the current workflow run should
// continue as new before accumulating more history.
type ContinuationDecision struct {
	ShouldContinue bool
	Reason         string
}

// IterationDecision explains whether the current workflow run exceeded its
// bounded execution policy.
type IterationDecision struct {
	ShouldFail bool
	Reason     string
}

// EvaluateContinuationPolicy evaluates PlanPolicy history guards from
// deterministic workflow snapshots.
func EvaluateContinuationPolicy(policy agentos.PlanPolicy, snapshot ContinuationSnapshot) ContinuationDecision {
	if policy.ContinueAsNewEvents > 0 && snapshot.AppliedTransitions >= policy.ContinueAsNewEvents {
		return ContinuationDecision{ShouldContinue: true, Reason: "continue_as_new_events"}
	}

	if policy.MaxHistoryEvents > 0 && snapshot.HistoryEvents >= int(policy.MaxHistoryEvents) {
		return ContinuationDecision{ShouldContinue: true, Reason: "max_history_events"}
	}

	return ContinuationDecision{}
}

// EvaluateIterationPolicy evaluates PlanPolicy loop guards.
func EvaluateIterationPolicy(policy agentos.PlanPolicy, snapshot IterationSnapshot) IterationDecision {
	if policy.MaxIterations > 0 && snapshot.Iterations > policy.MaxIterations {
		return IterationDecision{ShouldFail: true, Reason: "max_iterations"}
	}

	return IterationDecision{}
}

// BudgetExceeded reports whether a plan has spent beyond its configured budget.
func BudgetExceeded(policy agentos.PlanPolicy, usage agentos.PlanBudgetUsage) bool {
	return policy.BudgetCents > 0 && usage.SpentCents > policy.BudgetCents
}

// BudgetExceededReason returns a stable public failure reason for budget guard
// failures.
func BudgetExceededReason(policy agentos.PlanPolicy, usage agentos.PlanBudgetUsage) string {
	return fmt.Sprintf("plan budget exceeded: spent %d cents exceeds budget %d cents", usage.SpentCents, policy.BudgetCents)
}

// PlanTimedOut reports whether the plan-level wall-clock timeout has elapsed.
func PlanTimedOut(policy agentos.PlanPolicy, startedAt, now time.Time) bool {
	if policy.TimeoutSeconds <= 0 || startedAt.IsZero() {
		return false
	}

	return !now.Before(startedAt.Add(time.Duration(policy.TimeoutSeconds) * time.Second))
}

// PlanTimeoutReason returns a stable public failure reason for plan-level
// timeout failures.
func PlanTimeoutReason(policy agentos.PlanPolicy) string {
	return fmt.Sprintf("plan timed out after %d seconds", policy.TimeoutSeconds)
}

// PlanBlockedTimedOut reports whether a plan awaiting human approval has
// exceeded its ApprovalTimeoutSeconds while blocked. The clock starts when
// the plan entered the blocked state, not when it started.
func PlanBlockedTimedOut(policy agentos.PlanPolicy, blockedAt, now time.Time) bool {
	if policy.ApprovalTimeoutSeconds <= 0 || blockedAt.IsZero() {
		return false
	}

	return !now.Before(blockedAt.Add(time.Duration(policy.ApprovalTimeoutSeconds) * time.Second))
}

// PlanBlockedTimeoutReason returns a stable public failure reason for
// approval-timeout auto-rejections.
func PlanBlockedTimeoutReason(policy agentos.PlanPolicy) string {
	return fmt.Sprintf("plan approval timed out after %d seconds", policy.ApprovalTimeoutSeconds)
}
