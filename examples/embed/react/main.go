// examples/embed/react/main.go
//
// Starts a generic ReAct-style prompt through the public AgentOS Runtime.
//
//	go run examples/embed/react/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	cfg := agentostemporal.RuntimeConfig{
		TemporalAddress:    env("AGENTFW_TEMPORAL_ADDRESS", "127.0.0.1:7233"),
		TemporalNamespace:  env("AGENTFW_TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueues: agentostemporal.DefaultTaskQueues(),
		PostgresURL:        os.Getenv("PG_URL"),
		RedisURL:           os.Getenv("REDIS_URL"),
	}

	rt, err := agentostemporal.NewRuntime(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("new runtime: %w", err)
	}
	defer rt.Close()

	runID := fmt.Sprintf("embed-react-%d", time.Now().UnixMilli())

	spec := agentos.RunSpec{
		RunID:        runID,
		ThreadID:     runID,
		AccountID:    "demo-account",
		ModelRef:     "gpt-4.1-mini",
		SystemPrompt: "Show your reasoning briefly before answering.",
		UserMessage:  "Calculate 25 * 4 + 10.",
		RequestedAt:  time.Now().UTC(),
		Backend:      agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}

	status, err := rt.Start(ctx, &spec)
	if err != nil {
		return fmt.Errorf("start run: %w", err)
	}

	fmt.Fprintf(os.Stdout, "run started: id=%s state=%s\n", status.RunID, status.LifecycleState)

	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
