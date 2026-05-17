// examples/reflexion/main.go
//
// Reflexion Pattern (Self-Critique & Refine)
//
// An agent generates output, then an eval step critiques it. If the
// quality check fails, the OnResult mutation injects a refinement
// step with feedback for improvement.
//
// Flow:
//
//	generate → evaluate quality → [refine if needed] → conclude
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (eval + OnResult mutation)
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

func main() {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	accountID := os.Getenv("ACCOUNT_ID")
	if accountID == "" {
		accountID = "e2e-test-account"
	}
	runID := fmt.Sprintf("reflexion-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Printf("=== Reflexion Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Flow: generate → evaluate → [refine if needed] → conclude")
	fmt.Println("The eval step checks quality; OnResult appends a refine step on failure.")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: reflexionSteps(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Println("Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, 2*time.Second, 4*time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func reflexionSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "generate", "type": "agent", "agent_id": "writer",
			"input": map[string]any{"message": "Write a technical explanation of how distributed consensus works (Raft algorithm)"},
		},
		{
			"id": "evaluate", "type": "eval",
			"input": map[string]any{"condition": "quality_check"},
			"on_result": map[string]any{
				"append_after": "evaluate",
				"insert_steps": []map[string]any{
					{
						"id": "refine", "type": "agent", "agent_id": "writer",
						"input":      map[string]any{"message": "The previous output needs improvement. Make it more detailed, add examples, and clarify the leader election process."},
						"depends_on": []string{"evaluate"},
					},
				},
			},
		},
		{
			"id": "conclude", "type": "agent", "agent_id": "writer",
			"input":      map[string]any{"message": "Format the final version as a well-structured markdown document"},
			"depends_on": []string{"evaluate"},
		},
	}
}
