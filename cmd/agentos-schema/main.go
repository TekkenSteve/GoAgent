// Command agentos-schema checks a deployment's database against the schema this
// build requires.
//
// The application checks the same thing at startup and refuses to serve; this
// command exists for the moments around that: a pre-flight check in a pipeline,
// or a diagnosis of a host that will not start. It reads PG_URL.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	agentosroot "github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
)

const (
	exitServable   = 0
	exitUnservable = 1
	exitFailure    = 2

	defaultTimeout     = 15 * time.Second
	connectionAttempts = 3
)

func main() {
	os.Exit(run())
}

func run() int {
	postgresURL := os.Getenv("PG_URL")
	if postgresURL == "" {
		fmt.Fprintln(os.Stderr, "PG_URL is required")

		return exitFailure
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	pg, err := postgres.New(postgresURL, postgres.ConnAttempts(connectionAttempts))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)

		return exitFailure
	}

	defer pg.Close()

	status, err := agentosroot.CheckSchema(ctx, pg.Pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unservable (database version %d, dirty=%t): %v\n", status.Version, status.Dirty, err)

		return exitUnservable
	}

	fmt.Fprintf(os.Stdout, "servable: database version %d, library needs %d\n", status.Version, agentosroot.MinSchemaVersion)

	return exitServable
}
