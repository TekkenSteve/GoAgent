// examples/supervisor-worker/main.go
//
// # Supervisor-Worker Pattern
//
// A supervisor agent decomposes a task and delegates sub-tasks to
// worker agents running in parallel via Split/Join.
//
// Flow:
//
//	decompose (supervisor) → Split(worker-1, worker-2, worker-3)
//	  → Join → synthesize (supervisor)
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (split/join types)
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
	_kAgentID    = "agent_id"
	_kMessage    = "message"
)

const (
	_kID    = "id"
	_kType  = "type"
	_kInput = "input"
	_kAgent = "agent"
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

	runID := fmt.Sprintf("supervisor-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Supervisor-Worker Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Flow: decompose → Split(worker tasks) → Join → synthesize")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID: runID,
		Steps: supervisorWorkerSteps(),
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

func supervisorWorkerSteps() []map[string]any {
	return []map[string]any{
		{
			_kID: "decompose", _kType: _kAgent, _kAgentID: "supervisor",
			_kInput: map[string]any{_kMessage: "Decompose the task 'Build a web application' into 3 parallel work items: frontend, backend, and database design"},
		},
		{
			_kID: "execute", _kType: "split",
			_kInput: map[string]any{
				"children": []map[string]any{
					{
						_kID: "worker-frontend", _kType: _kAgent, _kAgentID: "worker",
						_kInput: map[string]any{_kMessage: "Design the frontend architecture: React components, state management, and routing"},
					},
					{
						_kID: "worker-backend", _kType: _kAgent, _kAgentID: "worker",
						_kInput: map[string]any{_kMessage: "Design the backend architecture: API endpoints, database models, and authentication"},
					},
					{
						_kID: "worker-database", _kType: _kAgent, _kAgentID: "worker",
						_kInput: map[string]any{_kMessage: "Design the database schema: tables, indexes, and migration strategy"},
					},
				},
			},
		},
		{
			_kID: "gather", _kType: "join",
			_kInput: map[string]any{"_join_group": "execute"},
		},
		{
			_kID: "synthesize", _kType: _kAgent, _kAgentID: "supervisor",
			_kInput:      map[string]any{_kMessage: "Synthesize the worker outputs into a cohesive architecture plan"},
			"depends_on": []string{"gather"},
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
