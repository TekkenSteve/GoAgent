// examples/scientific/main.go
//
// Scientific Method Pattern (with Human-in-the-Loop)
//
// A rigorous scientific workflow: hypothesis → experiment design →
// expert review (HITL) → experiment → analysis → conclusion.
//
// Flow:
//
//	hypothesis → design → review(HITL, 7-day timeout) → experiment → analyze → conclude
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (wait type)
//   - client.WaitForCompletion
//
// Prerequisites: running GoAgent instance, LLM configured.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/examples/client"
)

const (
	pollInterval       = 2 * time.Second
	pollTimeout        = 4 * time.Minute
	reviewTimeoutNanos = 604800000000000
)

func main() {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	accountID := os.Getenv("ACCOUNT_ID")
	if accountID == "" {
		accountID = "e2e-test-account"
	}

	runID := fmt.Sprintf("scientific-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== Scientific Method Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Fprintln(os.Stdout, "Flow: hypothesis → design → review(HITL) → experiment → analyze → conclude")
	fmt.Fprintln(os.Stdout, "The 'review' step blocks until 'expert-approval' signal or 7-day timeout.")
	fmt.Fprintln(os.Stdout)

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: scientificSteps(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Fprintln(os.Stdout, "Polling for completion...")
	fmt.Fprintln(os.Stdout, "(The 'review' step blocks until 'expert-approval' signal arrives)")
	fmt.Fprintln(os.Stdout, "Send it via: tctl workflow signal --name expert-approval --run_id", runID)

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func scientificSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "hypothesis", "type": "agent", "agent_id": "scientist",
			"input": map[string]any{"message": "Formulate a hypothesis about the relationship between temperature and CPU performance"},
		},
		{
			"id": "design", "type": "agent", "agent_id": "scientist",
			"input":      map[string]any{"message": "Design an experiment to test the hypothesis"},
			"depends_on": []string{"hypothesis"},
		},
		{
			"id": "review", "type": "wait",
			"wait_for": map[string]any{
				"signal_name": "expert-approval",
				// 604800000000000 = 7 days in nanoseconds
				"timeout":    reviewTimeoutNanos,
				"on_timeout": "fail",
			},
			"depends_on": []string{"design"},
		},
		{
			"id": "run-experiment", "type": "tool", "tool": "calculator",
			"input":      map[string]any{"expression": "85 * 1.5 + 12"},
			"depends_on": []string{"review"},
		},
		{
			"id": "analyze", "type": "agent", "agent_id": "scientist",
			"input":      map[string]any{"message": "Analyze the experimental results and draw conclusions"},
			"depends_on": []string{"run-experiment"},
		},
		{
			"id": "conclude", "type": "eval",
			"input":      map[string]any{"condition": "hypothesis_supported"},
			"depends_on": []string{"analyze"},
		},
	}
}
