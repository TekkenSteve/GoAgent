// examples/router/main.go
//
// # Router Pattern
//
// A classifier step evaluates the input and uses OnResult to inject
// the appropriate branch steps dynamically. This enables conditional
// routing based on content type.
//
// Flow:
//
//	classify → evaluate (route) → [branch-A or branch-B]
//	  → finalize
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (eval type + OnResult mutation)
//   - client.WaitForOrchestrationCompletion
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
	pollInterval = 2 * time.Second
	pollTimeout  = 4 * time.Minute
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

	runID := fmt.Sprintf("router-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== Router Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Fprintln(os.Stdout, "Flow: classify → route(branch) → finalize")
	fmt.Fprintln(os.Stdout, "The eval step dynamically appends branch-specific steps via OnResult.")
	fmt.Fprintln(os.Stdout)

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: routerSteps(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Fprintln(os.Stdout, "Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func routerSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "classify", "type": "agent", "agent_id": "classifier",
			"input": map[string]any{"message": "Classify the following input as either 'technical' or 'business': 'We need to migrate our database to PostgreSQL and set up replication.'"},
		},
		{
			"id": "route", "type": "eval",
			"input": map[string]any{"condition": "classify_result"},
			"on_result": map[string]any{
				"append_after": "route",
				"insert_steps": []map[string]any{
					{
						"id": "technical-branch", "type": "agent", "agent_id": "engineer",
						"input":      map[string]any{"message": "Create a detailed technical migration plan for PostgreSQL including replication setup"},
						"depends_on": []string{"classify"},
					},
				},
			},
		},
		{
			"id": "finalize", "type": "agent", "agent_id": "classifier",
			"input":      map[string]any{"message": "Summarize the output and format it as a final response"},
			"depends_on": []string{"route"},
		},
	}
}
