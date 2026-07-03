// examples/embed/plan/main.go
//
// Embedded RunPlan usage through the public AgentOS boundary.
//
//	go run examples/embed/plan/main.go
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

const (
	exampleMaxParallelNodes    = 2
	exampleContinueAsNewEvents = 100
	exampleMaxHistoryEvents    = 1000
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	cfg := agentostemporal.RuntimeConfig{
		TemporalAddress:   env("AGENTFW_TEMPORAL_ADDRESS", "127.0.0.1:7233"),
		TemporalNamespace: env("AGENTFW_TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueue: env("AGENTFW_TEMPORAL_TASK_QUEUE", "agent-framework"),
		PostgresURL:       os.Getenv("PG_URL"),
		RedisURL:          os.Getenv("REDIS_URL"),
		ArtifactStore:     agentostemporal.ArtifactStoreConfig{Backend: agentostemporal.ArtifactStoreBackend(env("AGENTOS_ARTIFACT_STORE_BACKEND", "local")), Local: agentostemporal.LocalArtifactStoreConfig{Root: env("AGENTOS_ARTIFACT_STORE_LOCAL_ROOT", ".data/agentos-artifacts")}},
	}

	rt, err := agentostemporal.NewPlanRuntime(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("new plan runtime: %w", err)
	}
	defer closePlanRuntime(rt)

	planID := fmt.Sprintf("embed-plan-%d", time.Now().UnixMilli())
	accountID := "demo-account"
	projectID := "demo-project"
	native := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: planID, ThreadID: planID, AccountID: accountID, ProjectID: projectID, IdempotencyKey: planID + ":start", RequestedAt: time.Now().UTC(),
		Inputs: map[string]any{"topic": "durable cross-backend agent orchestration"},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "research", Run: agentos.RunSpec{RunID: planID + "-research", ThreadID: planID, AccountID: accountID, ProjectID: projectID, ModelRef: "gpt-4.1-mini", UserMessage: "Summarize the core idea of durable agent orchestration.", RequestedAt: time.Now().UTC(), Backend: native, IdempotencyKey: planID + ":research"}},
			{NodeID: "verify", Run: agentos.RunSpec{RunID: planID + "-verify", ThreadID: planID, AccountID: accountID, ProjectID: projectID, ModelRef: "gpt-4.1-mini", UserMessage: "Check the previous result for missing operational constraints.", RequestedAt: time.Now().UTC(), Backend: native, IdempotencyKey: planID + ":verify"}},
		},
		Edges:  []agentos.PlanEdgeSpec{{EdgeID: "research-verify", From: "research", To: "verify", On: agentos.EdgeOnSuccess}},
		Policy: agentos.PlanPolicy{MaxParallelNodes: exampleMaxParallelNodes, ContinueAsNewEvents: exampleContinueAsNewEvents, MaxHistoryEvents: exampleMaxHistoryEvents},
	}

	status, err := rt.StartPlan(ctx, &spec)
	if err != nil {
		return fmt.Errorf("start plan: %w", err)
	}

	fmt.Fprintf(os.Stdout, "plan started: id=%s state=%s\n", status.PlanID, status.LifecycleState)

	current, err := rt.StatusPlan(ctx, agentos.PlanRef{PlanID: planID, AccountID: accountID, ProjectID: projectID})
	if err != nil {
		return fmt.Errorf("status plan: %w", err)
	}

	fmt.Fprintf(os.Stdout, "plan status: id=%s state=%s active_runs=%d\n", current.PlanID, current.LifecycleState, len(current.ActiveRunIDs))

	return nil
}

func closePlanRuntime(rt agentos.PlanRuntime) {
	closeable, ok := rt.(interface {
		Close() error
	})
	if !ok {
		return
	}

	if err := closeable.Close(); err != nil {
		log.Printf("close plan runtime: %v", err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
