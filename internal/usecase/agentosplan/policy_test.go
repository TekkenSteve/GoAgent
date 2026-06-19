package agentosplan

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestEvaluateContinuationPolicy(t *testing.T) {
	decision := EvaluateContinuationPolicy(agentos.PlanPolicy{
		ContinueAsNewEvents: 3,
		MaxHistoryEvents:    10,
	}, ContinuationSnapshot{AppliedTransitions: 3})
	if !decision.ShouldContinue || decision.Reason != "continue_as_new_events" {
		t.Fatalf("decision = %#v", decision)
	}

	decision = EvaluateContinuationPolicy(agentos.PlanPolicy{
		MaxHistoryEvents: 5,
	}, ContinuationSnapshot{HistoryEvents: 5})
	if !decision.ShouldContinue || decision.Reason != "max_history_events" {
		t.Fatalf("decision = %#v", decision)
	}

	decision = EvaluateContinuationPolicy(agentos.PlanPolicy{
		ContinueAsNewEvents: 3,
	}, ContinuationSnapshot{AppliedTransitions: 2})
	if decision.ShouldContinue {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestEvaluateIterationPolicy(t *testing.T) {
	decision := EvaluateIterationPolicy(agentos.PlanPolicy{
		MaxIterations: 2,
	}, IterationSnapshot{Iterations: 3})
	if !decision.ShouldFail || decision.Reason != "max_iterations" {
		t.Fatalf("decision = %#v", decision)
	}

	decision = EvaluateIterationPolicy(agentos.PlanPolicy{
		MaxIterations: 2,
	}, IterationSnapshot{Iterations: 2})
	if decision.ShouldFail {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestBudgetExceeded(t *testing.T) {
	policy := agentos.PlanPolicy{BudgetCents: 100}
	if BudgetExceeded(policy, agentos.PlanBudgetUsage{SpentCents: 100}) {
		t.Fatal("budget should not be exceeded at the exact limit")
	}
	if !BudgetExceeded(policy, agentos.PlanBudgetUsage{SpentCents: 101}) {
		t.Fatal("budget should be exceeded above the limit")
	}
}
