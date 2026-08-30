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
	"log"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/examples/client"
)

const (
	pollInterval       = 2 * time.Second
	pollTimeout        = 4 * time.Minute
	reviewTimeoutNanos = 604800000000000
	_kAgentID          = "agent_id"
	_kMessage          = "message"
)

const (
	_kID        = "id"
	_kType      = "type"
	_kInput     = "input"
	_kDependsOn = "depends_on"
	_kAgent     = "agent"
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

	writeStdoutf("=== Scientific Method Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Flow: hypothesis → design → review(HITL) → experiment → analyze → conclude")

	writeStdoutLine("The 'review' step blocks until 'expert-approval' signal or 7-day timeout.")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: scientificSteps(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Polling for completion...")

	writeStdoutLine("(The 'review' step blocks until 'expert-approval' signal arrives)")

	writeStdoutLine("Send it via: tctl workflow signal --name expert-approval --run_id", runID)

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func scientificSteps() []map[string]any {
	return []map[string]any{
		{
			_kID: "hypothesis", _kType: _kAgent, _kAgentID: "scientist",
			_kInput: map[string]any{_kMessage: "Formulate a hypothesis about the relationship between temperature and CPU performance"},
		},
		{
			_kID: "design", _kType: _kAgent, _kAgentID: "scientist",
			_kInput:     map[string]any{_kMessage: "Design an experiment to test the hypothesis"},
			_kDependsOn: []string{"hypothesis"},
		},
		{
			_kID: "review", _kType: "wait",
			"wait_for": map[string]any{
				"signal_name": "expert-approval",
				// 604800000000000 = 7 days in nanoseconds
				"timeout":    reviewTimeoutNanos,
				"on_timeout": "fail",
			},
			_kDependsOn: []string{"design"},
		},
		{
			_kID: "run-experiment", _kType: "tool", "tool": "calculator",
			_kInput:     map[string]any{"expression": "85 * 1.5 + 12"},
			_kDependsOn: []string{"review"},
		},
		{
			_kID: "analyze", _kType: _kAgent, _kAgentID: "scientist",
			_kInput:     map[string]any{_kMessage: "Analyze the experimental results and draw conclusions"},
			_kDependsOn: []string{"run-experiment"},
		},
		{
			_kID: "conclude", _kType: "eval",
			_kInput:     map[string]any{"condition": "hypothesis_supported"},
			_kDependsOn: []string{"analyze"},
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
