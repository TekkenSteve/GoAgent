// examples/exploratory/main.go
//
// Exploratory / Self-Modifying Workflow Pattern
//
// After initial exploration, a StepEval evaluates whether enough
// information has been gathered. If not, its OnResult mutation
// injects additional steps into the queue.
//
// Flow:
//
//	plan → Split(automation, jobs, quality) → Join
//	→ evaluate → [inject more steps if needed] → conclude
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (eval + on_result mutation)
//   - client.WaitForCompletion
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
	pollInterval    = 2 * time.Second
	pollTimeout     = 4 * time.Minute
	maxSearchResult = 3
	_kID            = "id"
	_kType          = "type"
	_kInput         = "input"
	_kTool          = "tool"
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

	runID := fmt.Sprintf("exploratory-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Exploratory Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Key feature: self-modifying step queue")

	writeStdoutLine("  When 'evaluate' determines more exploration is needed,")

	writeStdoutLine("  its OnResult injects additional steps after itself.")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: exploratorySteps(),
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

func exploratorySteps() []map[string]any {
	return []map[string]any{
		{
			_kID: "plan", _kType: "agent", "agent_id": "planner",
			_kInput: map[string]any{"message": "Create an exploration plan for: 'Impact of AI on software development'"},
		},
		{
			_kID: "explore", _kType: "split",
			_kInput: map[string]any{
				"children": []map[string]any{
					{
						_kID: "explore-automation", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "AI code automation tools 2026", "max_results": maxSearchResult},
					},
					{
						_kID: "explore-jobs", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "AI impact developer jobs 2026", "max_results": maxSearchResult},
					},
					{
						_kID: "explore-quality", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "AI code quality 2026", "max_results": maxSearchResult},
					},
				},
			},
		},
		{
			_kID: "gather", _kType: "join",
			_kInput: map[string]any{"_join_group": "explore"},
		},
		{
			_kID: "evaluate", _kType: "eval",
			_kInput: map[string]any{"condition": "enough_information"},
			"on_result": map[string]any{
				"append_after": "evaluate",
				"insert_steps": []map[string]any{
					{
						_kID: "explore-future", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "AI future predictions software engineering", "max_results": maxSearchResult},
					},
				},
			},
		},
		{
			_kID: "conclude", _kType: "agent", "agent_id": "planner",
			_kInput:      map[string]any{"message": "Summarize all exploration findings"},
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
