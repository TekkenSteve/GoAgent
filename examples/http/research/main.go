// examples/research/main.go
//
// # Research Pattern
//
// A research workflow that fans out into parallel exploration
// subtasks and then synthesizes the results.
//
// Flow:
//
//	discover → Split(impacts, solutions, policy) → Join
//	→ synthesize → review (HITL with 10s timeout)
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (split/join/wait types)
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
	pollInterval       = 2 * time.Second
	pollTimeout        = 4 * time.Minute
	maxSearchResult    = 5
	reviewTimeoutNanos = 10000000000
	childSearchResult  = 3
)

const (
	_kID    = "id"
	_kType  = "type"
	_kInput = "input"
	_kTool  = "tool"
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

	runID := fmt.Sprintf("research-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Research Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Flow: discover → Split(impacts, solutions, policy) → Join → synthesize → review (HITL)")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: researchSteps(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Polling for completion...")

	writeStdoutLine("(The 'review' step blocks until 'review-approved' signal or 10s timeout)")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func researchSteps() []map[string]any {
	return []map[string]any{
		{
			_kID: "discover", _kType: _kTool, _kTool: "web_search",
			_kInput: map[string]any{"query": "climate change latest research 2026", "max_results": maxSearchResult},
		},
		{
			_kID: "explore", _kType: "split",
			_kInput: map[string]any{
				"children": []map[string]any{
					{
						_kID: "explore-impacts", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "climate change impacts 2026", "max_results": childSearchResult},
					},
					{
						_kID: "explore-solutions", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "climate change solutions 2026", "max_results": childSearchResult},
					},
					{
						_kID: "explore-policy", _kType: _kTool, _kTool: "web_search",
						_kInput: map[string]any{"query": "climate policy 2026", "max_results": childSearchResult},
					},
				},
			},
		},
		{
			_kID: "gather", _kType: "join",
			_kInput: map[string]any{"_join_group": "explore"},
		},
		{
			_kID: "synthesize", _kType: "agent", "agent_id": "analyst",
			_kInput: map[string]any{"message": "Synthesize all research findings into a summary report"},
		},
		{
			_kID: "review", _kType: "wait",
			"wait_for": map[string]any{
				"signal_name": "review-approved",
				"timeout":     reviewTimeoutNanos, // 10 seconds in nanoseconds
				"on_timeout":  "skip",
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
