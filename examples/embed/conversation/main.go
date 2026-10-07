// Package main demonstrates embedding the AgentOS conversation runtime.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentosconversation "github.com/TekkenSteve/GoAgent/agentos/conversation"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	runtime, err := agentosconversation.NewRuntime(ctx, agentosconversation.Config{
		PostgresURL: os.Getenv("AGENTOS_PG_URL"),
		// An embedded host shares the deployment's Postgres, so it is a
		// producer on the event backbone's conversation outbox: enabling this
		// records every event for the deployment's drainer to publish. Leave
		// AGENTFW_NATS_URL unset and the outbox stays untouched, which is the
		// right setting when no drainer is running.
		EventOutbox: os.Getenv("AGENTFW_NATS_URL") != "",
	})
	if err != nil {
		return err
	}

	defer func() {
		if err := runtime.Close(); err != nil {
			log.Printf("close conversation runtime: %v", err)
		}
	}()

	run, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		ThreadID:       "example-thread",
		AccountID:      "example-account",
		ProjectID:      "example-project",
		UserMessage:    "Create a concise study plan.",
		IdempotencyKey: "example-request",
	})
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(os.Stdout, run.RunID)

	return err
}
