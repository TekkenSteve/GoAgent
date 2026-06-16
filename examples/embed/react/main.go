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

	"github.com/TekkenSteve/GoAgent/agentos"
	agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

func main() {
	ctx := context.Background()

	rt, err := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
		TemporalAddress:   env("AGENTFW_TEMPORAL_ADDRESS", "127.0.0.1:7233"),
		TemporalNamespace: env("AGENTFW_TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueue: env("AGENTFW_TEMPORAL_TASK_QUEUE", "agent-framework"),
		RedisURL:          os.Getenv("REDIS_URL"),
	}, agentostemporal.WithRunBackendIndex(agentostemporal.NewEphemeralRunBackendIndex()))
	if err != nil {
		log.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()

	runID := fmt.Sprintf("embed-react-%d", time.Now().UnixMilli())
	status, err := rt.Start(ctx, agentos.RunSpec{
		RunID:        runID,
		ThreadID:     runID,
		AccountID:    "demo-account",
		ModelRef:     "gpt-4.1-mini",
		SystemPrompt: "Show your reasoning briefly before answering.",
		UserMessage:  "Calculate 25 * 4 + 10.",
		RequestedAt:  time.Now().UTC(),
		Backend:      agentos.DefaultBackendRef(),
	})
	if err != nil {
		log.Fatalf("start run: %v", err)
	}

	fmt.Fprintf(os.Stdout, "run started: id=%s state=%s\n", status.RunID, status.LifecycleState)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
