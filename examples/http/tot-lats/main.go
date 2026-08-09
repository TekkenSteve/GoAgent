// examples/tot-lats/main.go
//
// Tree-of-Thoughts / LATS Pattern
//
// Multiple reasoning paths are explored in parallel via Split, then
// evaluated to select the best path. An eval step with OnResult
// injects the final synthesis step based on which path performed best.
//
// Flow:
//
//	Split(reasoning-1, reasoning-2, reasoning-3) → Join
//	  → evaluate-best → synthesize-final
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (split/join/eval types)
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

	runID := fmt.Sprintf("tot-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Tree-of-Thoughts / LATS Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Flow: Split(reasoning paths) → Join → evaluate-best → synthesize")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: totLatsSteps(),
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

func totLatsSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "explore", "type": "split",
			"input": map[string]any{
				"children": []map[string]any{
					{
						"id": "reasoning-optimistic", "type": "agent", "agent_id": "thinker",
						"input": map[string]any{"message": "Solve this problem with an optimistic approach: 'How should a startup decide whether to build or buy their core technology?' Assume rapid iteration."},
					},
					{
						"id": "reasoning-pessimistic", "type": "agent", "agent_id": "thinker",
						"input": map[string]any{"message": "Solve this problem with a conservative approach: 'How should a startup decide whether to build or buy their core technology?' Focus on risk mitigation."},
					},
					{
						"id": "reasoning-hybrid", "type": "agent", "agent_id": "thinker",
						"input": map[string]any{"message": "Solve this problem with a balanced approach: 'How should a startup decide whether to build or buy their core technology?' Consider both speed and risk."},
					},
				},
			},
		},
		{
			"id": "gather", "type": "join",
			"input": map[string]any{"_join_group": "explore"},
		},
		{
			"id": "evaluate-best", "type": "eval",
			"input": map[string]any{"condition": "select_best_reasoning"},
			"on_result": map[string]any{
				"append_after": "evaluate-best",
				"insert_steps": []map[string]any{
					{
						"id": "synthesize-final", "type": "agent", "agent_id": "thinker",
						"input":      map[string]any{"message": "Synthesize the best reasoning path into a final recommendation with actionable steps"},
						"depends_on": []string{"evaluate-best"},
					},
				},
			},
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
