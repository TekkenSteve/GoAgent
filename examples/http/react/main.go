// examples/react/main.go
//
// ReAct (Reasoning + Acting) Pattern
//
// A single agent reasons about a task, calls tools, and continues
// reasoning with results until a final answer is produced.
//
// Framework APIs demonstrated:
//   - client.StartRun — start an AgentOS run
//   - client.WaitForCompletion — poll until terminal state
//
// Prerequisites: running GoAgent instance (docker compose up), LLM configured.
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

	runID := fmt.Sprintf("react-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== ReAct Pattern ===\nRun ID: %s\n\n", runID)

	status, err := c.StartRun(ctx, &client.AgentOSRunRequest{
		RunID:       runID,
		ThreadID:    runID,
		UserMessage: "What is 25 * 4 + 10? Calculate it and then search the web for cool facts about the result.",
		Backend: client.BackendRef{
			Kind: "native",
			Name: "goagent-native",
		},
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Polling for completion...")

	status, err = c.WaitForCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
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
