package app

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentosconversation "github.com/TekkenSteve/GoAgent/agentos/conversation"
	"github.com/TekkenSteve/GoAgent/config"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testEventBackbonePrefix isolates a test's subjects from every other test and
// from any stream that was provisioned for real. Publishing into it therefore
// fails, which is what makes "the row moved" attributable to the claim rather
// than to a successful delivery.
const testEventBackbonePrefix = "agentostestbackbone"

// idlePublisher stands in for the log where a test only needs the drainer to
// exist, so no log is required.
type idlePublisher struct{}

func (p *idlePublisher) Publish(context.Context, *eventlog.Record) error { return nil }

func testEventBackboneConfig(backboneURL string) *config.Config {
	cfg := &config.Config{}
	cfg.App.Name = "agentos-test"
	cfg.AgentFW.NatsURL = backboneURL
	cfg.AgentFW.NatsSubjectPrefix = testEventBackbonePrefix
	cfg.AgentFW.NatsShards = 4

	return cfg
}

// testEventBackboneURL is the log this suite drains onto. It is skipped rather
// than faked: the assembly being tested is the connection between Postgres and
// a real server, and a fake cannot fail the way a real one does.
func testEventBackboneURL(t *testing.T) string {
	t.Helper()

	backboneURL := os.Getenv("AGENTFW_NATS_TEST_URL")
	if backboneURL == "" {
		t.Skip("AGENTFW_NATS_TEST_URL is not set")
	}

	return backboneURL
}

// TestStartEventBackboneDisabledWithoutURL locks the opt-in: with no URL the
// backbone is nil rather than a half-built component, and every dependency it
// would need is left untouched.
func TestStartEventBackboneDisabledWithoutURL(t *testing.T) {
	t.Parallel()

	backbone, err := startEventBackbone(t.Context(), logger.New("error"), testEventBackboneConfig(""), nil)
	if err != nil {
		t.Fatalf("startEventBackbone without a URL: %v", err)
	}

	if backbone != nil {
		t.Fatalf("startEventBackbone built %T without a URL, want nil so the log stays opt-in", backbone)
	}

	// The disabled case is represented by nil, so Stop has to tolerate it.
	backbone.Stop()
}

// TestEventBackboneConfigFromApp locks the environment mapping, so an operator
// setting has exactly one reader and the shard count reaches the client.
func TestEventBackboneConfigFromApp(t *testing.T) {
	t.Parallel()

	disabled := eventBackboneConfig(testEventBackboneConfig(""))
	if disabled.Enabled() {
		t.Fatal("an empty AGENTFW_NATS_URL enabled the backbone")
	}

	configured := eventBackboneConfig(testEventBackboneConfig("nats://nats:4222"))
	if !configured.Enabled() {
		t.Fatal("a configured URL did not enable the backbone")
	}

	if configured.SubjectPrefix != testEventBackbonePrefix {
		t.Fatalf("subject prefix = %q, want %q", configured.SubjectPrefix, testEventBackbonePrefix)
	}

	if configured.Shards != 4 {
		t.Fatalf("shards = %d, want the configured 4", configured.Shards)
	}

	if configured.ClientName != "agentos-test" {
		t.Fatalf("client name = %q, want the application name", configured.ClientName)
	}
}

// TestNewDomainDrainersRegisterDomains locks the wiring that makes an outbox
// reachable: a domain whose source is built here is drained, and a domain whose
// source is not is silently never published. The assertions name every domain,
// so adding a source without registering it fails here.
func TestNewDomainDrainersRegisterDomains(t *testing.T) {
	t.Parallel()

	// The wiring assertion needs no database: an empty handle satisfies the
	// sources' constructors, which only read the pool reference.
	drainers, err := newDomainDrainers(
		logger.New("error"),
		&idlePublisher{},
		&postgres.Postgres{},
	)
	if err != nil {
		t.Fatalf("newDomainDrainers: %v", err)
	}

	if len(drainers) != 2 {
		t.Fatalf("registered %d domains, want 2", len(drainers))
	}

	for i, name := range []string{conversationOutboxDomain, runTimelineOutboxDomain} {
		if drainers[i].domain.name != name {
			t.Fatalf("domain %d = %q, want %q", i, drainers[i].domain.name, name)
		}

		if drainers[i].domain.source == nil {
			t.Fatalf("domain %q was registered without an outbox source", name)
		}

		if drainers[i].drainer == nil {
			t.Fatalf("domain %q was registered without a drainer", name)
		}
	}
}

// TestEventBackboneDrainsConversationOutbox locks the assembly end to end: a
// conversation runtime opted into the outbox writes rows, and the running
// backbone claims them.
//
// The test's subject prefix is one that no stream was provisioned for, so
// publishing fails on purpose. Claiming happens before publishing, so the rows
// moving forward proves the loop is live without a successful delivery — and
// the count staying put proves a claim is a lease that cannot drop an event.
// What a *failed* publish records is asserted deterministically by the outbox
// source's own integration tests, which can control the publisher.
func TestEventBackboneDrainsConversationOutbox(t *testing.T) {
	t.Parallel()

	backboneURL := testEventBackboneURL(t)
	postgresURL := newEventBackboneTestURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queued := queueConversationEvents(ctx, t, postgresURL)

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect test schema: %v", err)
	}

	t.Cleanup(pool.Close)

	pg, err := postgres.New(postgresURL, postgres.MaxPoolSize(2))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(pg.Close)

	backbone, err := startEventBackbone(ctx, logger.New("error"), testEventBackboneConfig(backboneURL), pg)
	if err != nil {
		t.Fatalf("startEventBackbone: %v", err)
	}

	if backbone == nil {
		t.Fatal("startEventBackbone returned a disabled backbone for a configured URL")
	}

	t.Cleanup(backbone.Stop)

	assertConversationOutboxClaimed(ctx, t, pool)
	assertConversationOutboxCount(ctx, t, pool, queued)

	// Stop is idempotent: the deferred call runs it a second time.
	backbone.Stop()
}

// queueConversationEvents writes one run through an opted-in runtime and parks
// its outbox rows an hour in the past, returning how many were queued. Only a
// claim or a failed attempt moves available_at, so a parked row leaves "the
// drain loop ran" unambiguous where a freshly queued row would prove nothing.
func queueConversationEvents(ctx context.Context, t *testing.T, postgresURL string) int {
	t.Helper()

	runtime, err := agentosconversation.NewRuntime(ctx, agentosconversation.Config{
		PostgresURL: postgresURL,
		EventOutbox: true,
	})
	if err != nil {
		t.Fatalf("conversation.NewRuntime: %v", err)
	}

	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close conversation runtime: %v", err)
		}
	})

	suffix := fmt.Sprintf("%x", time.Now().UnixNano())

	if _, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		ThreadID: "thread-" + suffix, AccountID: "account-" + suffix, ProjectID: "project-" + suffix,
		UserMessage: "hello", IdempotencyKey: "key-" + suffix,
	}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect test schema: %v", err)
	}

	defer pool.Close()

	var queued int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM agentos_conversation_event_outbox`).Scan(&queued); err != nil {
		t.Fatalf("count queued rows: %v", err)
	}

	if queued == 0 {
		t.Fatal("the opted-in conversation runtime queued no outbox rows")
	}

	if _, err := pool.Exec(ctx, `
UPDATE agentos_conversation_event_outbox SET available_at = NOW() - INTERVAL '1 hour'`); err != nil {
		t.Fatalf("park queued rows: %v", err)
	}

	return queued
}

// assertConversationOutboxClaimed waits for the drain loop to claim every
// parked row, which it does by moving available_at out of the parked window.
func assertConversationOutboxClaimed(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for {
		var parked int

		if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM agentos_conversation_event_outbox WHERE available_at < NOW() - INTERVAL '30 minutes'`).Scan(&parked); err != nil {
			t.Fatalf("count parked rows: %v", err)
		}

		if parked == 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%d outbox rows were never claimed by the running backbone", parked)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// assertConversationOutboxCount checks that claiming is a lease and not a
// handoff: rows are deleted only by a successful publish, so a backbone with no
// broker may not shrink the queue.
func assertConversationOutboxCount(ctx context.Context, t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()

	var queued int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM agentos_conversation_event_outbox`).Scan(&queued); err != nil {
		t.Fatalf("count queued rows: %v", err)
	}

	if queued != want {
		t.Fatalf("queued rows = %d, want the %d that were written: a claim must not drop an event", queued, want)
	}
}

// newEventBackboneTestURL creates an isolated schema inside AGENTOS_TEST_PG_URL
// and returns a URL bound to it.
//
// A schema rather than a database keeps the app-level tests free of a database
// harness: both the conversation runtime and postgres.New read search_path from
// the URL, so one connection string isolates every pool the test opens.
func newEventBackboneTestURL(t *testing.T) string {
	t.Helper()

	adminURL := os.Getenv("AGENTOS_TEST_PG_URL")
	if adminURL == "" {
		t.Skip("AGENTOS_TEST_PG_URL is not set")
	}

	ctx := t.Context()

	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}

	t.Cleanup(admin.Close)

	schema := fmt.Sprintf("backbone_%x", time.Now().UnixNano())

	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("create test schema: %v", err)
	}

	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("drop test schema %q: %v", schema, err)
		}
	})

	isolated, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse postgres test URL: %v", err)
	}

	query := isolated.Query()
	query.Set("search_path", schema)
	isolated.RawQuery = query.Encode()

	applyConversationSchema(t, isolated.String())

	return isolated.String()
}

// applyConversationSchema applies the conversation migrations in dependency
// order: the outbox row references the event row it publishes.
func applyConversationSchema(t *testing.T, isolatedURL string) {
	t.Helper()

	pool, err := pgxpool.New(t.Context(), isolatedURL)
	if err != nil {
		t.Fatalf("connect isolated schema: %v", err)
	}

	defer pool.Close()

	for _, migration := range []string{
		"20260507000001_create_messages.up.sql",
		"20260725000001_create_agentos_conversations.up.sql",
		"20260726000001_create_agentos_conversation_event_outbox.up.sql",
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}

		if _, err := pool.Exec(t.Context(), string(data)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}
}
