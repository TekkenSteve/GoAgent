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
	"log"
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

	runID := fmt.Sprintf("reflexion-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Reflexion Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Flow: generate → evaluate → [refine if needed] → conclude")

	writeStdoutLine("The eval step checks quality; OnResult appends a refine step on failure.")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: reflexionSteps(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
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

func writeStdoutf(format string, args ...any) {
	if _, werr := fmt.Fprintf(os.Stdout, format, args...); werr != nil {
		log.Fatalf("write stdout: %v", werr)
	}
}

func writeStderrf(format string, args ...any) {
	if _, werr := fmt.Fprintf(os.Stderr, format, args...); werr != nil {
		log.Fatalf("write stderr: %v", werr)
	}
}

func writeStdoutLine(args ...any) {
	if _, werr := fmt.Fprintln(os.Stdout, args...); werr != nil {
		log.Fatalf("write stdout: %v", werr)
	}
}
