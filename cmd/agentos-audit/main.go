// Command agentos-audit verifies the control plane's audit trail.
//
// The trail is hash-chained per tenant, so an edited row is visible: its stored
// hash no longer matches its contents, and every later row stops matching its
// predecessor. Verification is what turns that property into an answer, and it
// is deliberately a separate command rather than a query inside the service —
// the database owner is exactly the party the check exists to catch.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
)

const (
	exitIntact  = 0
	exitBroken  = 1
	exitFailure = 2

	defaultTimeout = 30 * time.Second
	// connectionAttempts is how many times the command retries the initial
	// connection before reporting the database unreachable.
	connectionAttempts = 3
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		accountID = flag.String("account", "", "tenant account id whose audit chain to verify")
		projectID = flag.String("project", "", "tenant project id whose audit chain to verify")
	)

	flag.Parse()

	if *accountID == "" || *projectID == "" {
		fmt.Fprintln(os.Stderr, "usage: agentos-audit -account <account-id> -project <project-id>")
		fmt.Fprintln(os.Stderr, "  the connection string comes from PG_URL")

		return exitFailure
	}

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

	verification, err := temporalrepo.NewAgentOSPlanRepo(pg).VerifyAuditChain(ctx, *accountID, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)

		return exitFailure
	}

	fmt.Fprintf(os.Stdout, "tenant %s/%s: %d rows, %d chained, %d before the chain\n",
		verification.AccountID, verification.ProjectID,
		verification.Rows, verification.Chained, verification.UnchainedPrefix)

	switch {
	case verification.BrokenAt != "":
		fmt.Fprintf(os.Stderr, "BROKEN at audit %s: %s\n", verification.BrokenAt, verification.Reason)

		return exitBroken
	case verification.UnchainedPrefix > 0:
		fmt.Fprintf(os.Stderr, "%d row(s) predate the chain and cannot be verified\n", verification.UnchainedPrefix)

		return exitFailure
	default:
		fmt.Fprintln(os.Stdout, "intact")

		return exitIntact
	}
}
