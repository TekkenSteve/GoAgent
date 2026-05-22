// examples/react/main.go
//
// ReAct (Reasoning + Acting) Pattern
//
// A single agent reasons about a task, calls tools, and continues
// reasoning with results until a final answer is produced.
//
// Framework APIs demonstrated:
//   - client.ExecuteAgent — start a ReAct agent run
//   - client.WaitForCompletion — poll until terminal state
//   - client.ListMessages — read the conversation
//
// Prerequisites: running GoAgent instance (docker compose up), LLM configured.
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

	runID := fmt.Sprintf("react-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== ReAct Pattern ===\nRun ID: %s\n\n", runID)

	status, err := c.ExecuteAgent(ctx, client.ExecuteRequest{
		RunID:       runID,
		UserMessage: "What is 25 * 4 + 10? Calculate it and then search the web for cool facts about the result.",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Fprintln(os.Stdout, "Polling for completion...")

	status, err = c.WaitForCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)

	messages, err := c.ListMessages(ctx, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: ListMessages: %v\n", err)

		return
	}

	fmt.Fprintf(os.Stdout, "Messages: %d\n", len(messages))

	for i, m := range messages {
		fmt.Fprintf(os.Stdout, "  [%d] %s\n", i, m)
	}
}
