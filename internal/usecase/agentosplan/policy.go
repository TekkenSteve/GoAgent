package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ContinuationSnapshot is deterministic input for RunPlan continuation policy.
type ContinuationSnapshot struct {
	AppliedTransitions int32
}

// ContinuationDecision explains whether the current workflow run should
// continue as new before accumulating more history.
type ContinuationDecision struct {
	ShouldContinue bool
	Reason         string
}

// EvaluateContinuationPolicy evaluates PlanPolicy history guards using reducer
// transitions as the deterministic proxy for workflow history growth.
func EvaluateContinuationPolicy(policy agentos.PlanPolicy, snapshot ContinuationSnapshot) ContinuationDecision {
	if snapshot.AppliedTransitions <= 0 {
		return ContinuationDecision{}
	}
	if policy.ContinueAsNewEvents > 0 && snapshot.AppliedTransitions >= policy.ContinueAsNewEvents {
		return ContinuationDecision{ShouldContinue: true, Reason: "continue_as_new_events"}
	}
	if policy.MaxHistoryEvents > 0 && snapshot.AppliedTransitions >= policy.MaxHistoryEvents {
		return ContinuationDecision{ShouldContinue: true, Reason: "max_history_events"}
	}

	return ContinuationDecision{}
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
