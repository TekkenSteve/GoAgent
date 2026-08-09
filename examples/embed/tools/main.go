// examples/embed/tools/main.go
//
// Starts a tool-capable prompt through the public AgentOS Runtime.
//
//	go run examples/embed/tools/main.go
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

	defer func() {
		if err := rt.Close(); err != nil {
			log.Printf("close runtime: %v", err)
		}
	}()

	runID := fmt.Sprintf("embed-tools-%d", time.Now().UnixMilli())

	spec := agentos.RunSpec{
		RunID:        runID,
		ThreadID:     runID,
		AccountID:    "demo-account",
		ModelRef:     "gpt-4.1-mini",
		SystemPrompt: "Use available tools when they are relevant.",
		UserMessage:  "Search the web for the latest Go release and summarize it.",
		RequestedAt:  time.Now().UTC(),
		Backend:      agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}

	status, err := rt.Start(ctx, &spec)
	if err != nil {
		log.Printf("start run: %v", err)

		return
	}

	if _, werr := fmt.Fprintf(os.Stdout, "run started: id=%s state=%s\n", status.RunID, status.LifecycleState); werr != nil {
		log.Printf("write stdout: %v", werr)

		return
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
