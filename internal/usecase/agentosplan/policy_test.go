package agentosplan

import (
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
)

func TestEvaluateContinuationPolicy(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	policy := agentos.PlanPolicy{BudgetCents: 100}
	if BudgetExceeded(policy, agentos.PlanBudgetUsage{SpentCents: 100}) {
		t.Fatal("budget should not be exceeded at the exact limit")
	}

	if !BudgetExceeded(policy, agentos.PlanBudgetUsage{SpentCents: 101}) {
		t.Fatal("budget should be exceeded above the limit")
	}
}

// assertDeadlinePolicy checks the four corners of a plan deadline predicate:
// not expired just before the deadline, expired at the deadline, no deadline
// configured (never expires), and no anchor time (never expires).
func assertDeadlinePolicy(t *testing.T, policy agentos.PlanPolicy, anchor time.Time, timedOut func(agentos.PlanPolicy, time.Time, time.Time) bool) {
	t.Helper()

	if timedOut(policy, anchor, anchor.Add(9*time.Second)) {
		t.Fatal("plan should not time out before the configured deadline")
	}

	if !timedOut(policy, anchor, anchor.Add(10*time.Second)) {
		t.Fatal("plan should time out at the configured deadline")
	}

	if timedOut(agentos.PlanPolicy{}, anchor, anchor.Add(time.Hour)) {
		t.Fatal("plan without deadline policy timed out")
	}

	if timedOut(policy, time.Time{}, anchor.Add(time.Hour)) {
		t.Fatal("plan without anchor time timed out")
	}
}

func TestPlanTimedOut(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	assertDeadlinePolicy(t, agentos.PlanPolicy{TimeoutSeconds: 10}, startedAt, PlanTimedOut)
}

func TestPlanBlockedTimedOut(t *testing.T) {
	t.Parallel()

	blockedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	assertDeadlinePolicy(t, agentos.PlanPolicy{ApprovalTimeoutSeconds: 10}, blockedAt, PlanBlockedTimedOut)
}

func TestPlanBlockedTimeoutReason(t *testing.T) {
	t.Parallel()

	got := PlanBlockedTimeoutReason(agentos.PlanPolicy{ApprovalTimeoutSeconds: 30})
	want := "plan approval timed out after 30 seconds"

	if got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
}
