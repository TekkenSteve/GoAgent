// examples/embed/conversation/main.go
//
// Embedded runtime usage through the public AgentOS boundary.
//
//	go run examples/embed/conversation/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

func main() {
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
		log.Fatalf("new runtime: %v", err)
	}

	runID := fmt.Sprintf("embed-conv-%d", time.Now().UnixMilli())

	spec := agentos.RunSpec{
		RunID:        runID,
		ThreadID:     runID,
		AccountID:    "demo-account",
		ModelRef:     "gpt-4.1-mini",
		SystemPrompt: "You are a concise assistant.",
		UserMessage:  "What is clean architecture?",
		RequestedAt:  time.Now().UTC(),
		Backend:      agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}

	status, err := rt.Start(ctx, &spec)
	if err != nil {
		rt.Close()

		log.Fatalf("start run: %v", err)
	}
	defer rt.Close()

	fmt.Fprintf(os.Stdout, "run started: id=%s state=%s\n", status.RunID, status.LifecycleState)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
