//go:build postgres_integration

package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/outbox"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
)

// agentosConversationOutboxPostgresIntegrationPrefix names the throwaway
// databases this suite creates, so a leaked database identifies its suite.
const agentosConversationOutboxPostgresIntegrationPrefix = "goagent_conversation_outbox_"

// The two failures a drainer must tell apart. A poison record is rejected by a
// reachable log — the record's own fault, and it may eventually be dead-lettered.
// An outage is the log being gone; no record may be charged for it.
var (
	errConversationPoisonRecord = errors.New("conversation outbox test: record rejected")
	errConversationLogDown      = fmt.Errorf("conversation outbox test: log down: %w", eventlog.ErrLogUnavailable)
)

// conversationOutboxIntegrationSeed is a conversation written straight to SQL.
// Tests seed through SQL rather than through the runtime so the outbox under
// test is the only thing being exercised.
type conversationOutboxIntegrationSeed struct {
	threadID string
	runID    string
	events   int
}

// recordingPublisher captures the records a drain produced and can refuse
// writes to one topic with a chosen failure, which is what separates a broker
// outage (retry, budget untouched) from a poison record (retry, budget spent).
type recordingPublisher struct {
	refusedTopic string
	refusal      error
	records      []eventlog.Record
}

func (p *recordingPublisher) Publish(_ context.Context, record *eventlog.Record) error {
	if record.Domain == p.refusedTopic {
		return p.refusal
	}

	p.records = append(p.records, *record)

	return nil
}

func (p *recordingPublisher) topics() []string {
	topics := make([]string, 0, len(p.records))
	for i := range p.records {
		topics = append(topics, p.records[i].Domain)
	}

	return topics
}

// TestAgentOSConversationOutboxPostgresClaimShape locks the row-to-record
// mapping: the handle is the event's own ID, the record is keyed by thread, the
// payload is read from the event table rather than copied into the outbox, a
// thread's events come back in sequence order, and the ready predicate is what
// decides who is claimed.
func TestAgentOSConversationOutboxPostgresClaimShape(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSConversationOutboxPostgresIntegrationDB(t)

	first := conversationOutboxIntegrationSeed{threadID: "thread-a-" + suffix, runID: "run-a-" + suffix, events: 3}
	second := conversationOutboxIntegrationSeed{threadID: "thread-b-" + suffix, runID: "run-b-" + suffix, events: 2}
	seedConversationOutbox(t, pg, first)
	seedConversationOutbox(t, pg, second)

	source := NewAgentOSConversationOutboxSource(pg)

	pending, err := source.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}

	if len(pending) != 5 {
		t.Fatalf("claimed %d records, want 5", len(pending))
	}

	// Ordering: one thread's events are claimed in sequence order. The two
	// threads never share a partition, so their order relative to each other is
	// unconstrained, but each thread's own run must be contiguous and ascending.
	wantKeys := []string{
		first.threadID, first.threadID, first.threadID,
		second.threadID, second.threadID,
	}

	keys := make([]string, 0, len(pending))
	for i := range pending {
		keys = append(keys, string(pending[i].Key))
	}

	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("record keys = %v, want %v", keys, wantKeys)
	}

	for i := range pending {
		record := &pending[i]

		if record.Domain != agentosConversationEventsDomain {
			t.Fatalf("record %d topic = %q, want %q", i, record.Domain, agentosConversationEventsDomain)
		}

		if record.Attempts != 0 {
			t.Fatalf("record %d attempts = %d, want 0 for a never-attempted row", i, record.Attempts)
		}

		envelope, err := eventlog.DecodeEnvelope(record.Payload)
		if err != nil {
			t.Fatalf("decode record %d envelope: %v", i, err)
		}

		if record.ID != envelope.EventID {
			t.Fatalf("record %d handle = %q, want the envelope's event id %q", i, record.ID, envelope.EventID)
		}

		if envelope.Entity.Kind != eventlog.EntityKindThread || envelope.Entity.ID != string(record.Key) {
			t.Fatalf("record %d entity = %s/%s, want the thread %q the key names",
				i, envelope.Entity.Kind, envelope.Entity.ID, record.Key)
		}

		if envelope.Tenant.AccountID != "account-"+string(record.Key) || envelope.Tenant.ProjectID != "project-"+string(record.Key) {
			t.Fatalf("record %d tenant = %+v, want the tenant of thread %q", i, envelope.Tenant, record.Key)
		}

		var event agentos.ConversationEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			t.Fatalf("decode record %d fact body: %v", i, err)
		}

		if event.SchemaVersion != agentos.ConversationSchemaVersion {
			t.Fatalf("record %d schema version = %q, want %q", i, event.SchemaVersion, agentos.ConversationSchemaVersion)
		}

		if event.ThreadID != string(record.Key) {
			t.Fatalf("record %d event thread = %q, want key %q", i, event.ThreadID, record.Key)
		}

		if event.Payload["delta"] == nil {
			t.Fatalf("record %d lost its payload: %+v", i, event.Payload)
		}
	}

	// The outbox holds no bytes of its own: rewriting the event's payload
	// changes what the next claim publishes, which is only true if the payload is
	// read from agentos_conversation_events.
	if _, err := pg.Pool.Exec(ctx, `
UPDATE agentos_conversation_events SET payload = jsonb_build_object('delta', 'rewritten')
WHERE thread_id = $1 AND sequence = 2`, first.threadID); err != nil {
		t.Fatalf("rewrite event payload: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `
UPDATE agentos_conversation_event_outbox SET available_at = NOW() - INTERVAL '1 second'
WHERE thread_id = $1`, first.threadID); err != nil {
		t.Fatalf("release claimed rows: %v", err)
	}

	pending, err = source.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending after rewrite: %v", err)
	}

	var rewritten agentos.ConversationEvent

	for i := range pending {
		if string(pending[i].Key) != first.threadID {
			continue
		}

		envelope, err := eventlog.DecodeEnvelope(pending[i].Payload)
		if err != nil {
			t.Fatalf("decode rewritten envelope: %v", err)
		}

		var event agentos.ConversationEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			t.Fatalf("decode rewritten fact body: %v", err)
		}

		if event.Sequence == 2 {
			rewritten = event
		}
	}

	if rewritten.Payload["delta"] != "rewritten" {
		t.Fatalf("claimed payload delta = %v, want the rewritten value read from the event row", rewritten.Payload["delta"])
	}

	// The claim is the ready predicate: everything was just leased, so a further
	// claim over both threads finds nothing.
	pending, err = source.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending with nothing ready: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("claimed %d records from a fully leased outbox, want 0", len(pending))
	}
}

// TestAgentOSConversationOutboxPostgresDrain runs the real drainer against the
// real outbox: every queued event reaches the log once, and the rows it covered
// are gone, so an outbox with no backlog reports itself empty.
func TestAgentOSConversationOutboxPostgresDrain(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSConversationOutboxPostgresIntegrationDB(t)

	seedConversationOutbox(t, pg, conversationOutboxIntegrationSeed{threadID: "thread-" + suffix, runID: "run-" + suffix, events: 4})

	source := NewAgentOSConversationOutboxSource(pg)
	publisher := &recordingPublisher{}

	// The dead-letter mapping is the log's own — the same function the
	// backbone wires — so the domain a drainer dead-letters to here is the
	// domain the provisioned dead-letter stream carries.
	drainer, err := outbox.NewDrainer(source, publisher, outbox.WithDeadLetter(eventlog.DeadLetterDomain))
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}

	result, err := drainer.DrainOnce(ctx)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}

	if result != (outbox.Result{Claimed: 4, Published: 4}) {
		t.Fatalf("drain result = %+v, want 4 claimed and 4 published", result)
	}

	if got := publisher.topics(); !slices.Equal(got, []string{
		agentosConversationEventsDomain, agentosConversationEventsDomain,
		agentosConversationEventsDomain, agentosConversationEventsDomain,
	}) {
		t.Fatalf("published topics = %v, want four %q records", got, agentosConversationEventsDomain)
	}

	if got := publisher.records[0].Headers[attemptsHeader]; got != "1" {
		t.Fatalf("first-delivery attempts header = %q, want \"1\"", got)
	}

	// Success deletes: the outbox keeps unpublished work and nothing else, which
	// is why it cannot grow without bound while a drainer is running.
	var remaining int
	if err := pg.Pool.QueryRow(ctx, `
SELECT COUNT(*) FROM agentos_conversation_event_outbox WHERE thread_id = $1`, "thread-"+suffix).Scan(&remaining); err != nil {
		t.Fatalf("count remaining rows: %v", err)
	}

	if remaining != 0 {
		t.Fatalf("outbox rows after a successful drain = %d, want 0", remaining)
	}

	result, err = drainer.DrainOnce(ctx)
	if err != nil {
		t.Fatalf("DrainOnce on an empty outbox: %v", err)
	}

	if result != (outbox.Result{}) {
		t.Fatalf("drain result on an empty outbox = %+v, want the zero result", result)
	}
}

// TestAgentOSConversationOutboxPostgresClaimLease locks the crash story. A
// claimed row is hidden from other drainers for the lease, a lease is not an
// attempt, and once the lease lapses the event is redelivered — at-least-once,
// never at-most-once.
func TestAgentOSConversationOutboxPostgresClaimLease(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSConversationOutboxPostgresIntegrationDB(t)

	threadID := "thread-lease-" + suffix
	seedConversationOutbox(t, pg, conversationOutboxIntegrationSeed{threadID: threadID, runID: "run-lease-" + suffix, events: 2})

	source := NewAgentOSConversationOutboxSource(pg)

	claimed, err := source.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("first ClaimPending: %v", err)
	}

	if len(claimed) != 2 {
		t.Fatalf("first claim returned %d records, want 2", len(claimed))
	}

	// A second drainer arriving mid-flight must not see the in-flight rows.
	other := NewAgentOSConversationOutboxSource(pg)

	overlapping, err := other.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("overlapping ClaimPending: %v", err)
	}

	if len(overlapping) != 0 {
		t.Fatalf("a second drainer claimed %d in-flight rows, want 0", len(overlapping))
	}

	assertConversationOutboxRows(t, pg, threadID, conversationOutboxRowExpectation{attempts: 0, lastError: ""})

	// The lease lapses, standing in for a drainer that crashed mid-publish.
	if _, err := pg.Pool.Exec(ctx, `
UPDATE agentos_conversation_event_outbox SET available_at = NOW() - INTERVAL '1 second'
WHERE thread_id = $1`, threadID); err != nil {
		t.Fatalf("lapse the lease: %v", err)
	}

	redelivered, err := other.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending after the lease lapsed: %v", err)
	}

	if len(redelivered) != 2 {
		t.Fatalf("after the lease lapsed %d records were claimable, want 2", len(redelivered))
	}

	if redelivered[0].Attempts != 0 {
		t.Fatalf("attempts after a lease cycle = %d, want 0: a lease is not a delivery attempt", redelivered[0].Attempts)
	}
}

// TestAgentOSConversationOutboxPostgresRetryBackoff locks the poison path: a
// record a reachable log keeps rejecting costs an attempt, records why, defers
// the row by a growing backoff, and eventually moves to the dead-letter topic
// instead of blocking every record behind it.
func TestAgentOSConversationOutboxPostgresRetryBackoff(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSConversationOutboxPostgresIntegrationDB(t)

	threadID := "thread-retry-" + suffix
	seedConversationOutbox(t, pg, conversationOutboxIntegrationSeed{threadID: threadID, runID: "run-retry-" + suffix, events: 1})

	source := NewAgentOSConversationOutboxSource(pg)
	publisher := &recordingPublisher{
		refusedTopic: agentosConversationEventsDomain,
		refusal:      errConversationPoisonRecord,
	}

	const maxAttempts = 3

	drainer, err := outbox.NewDrainer(source, publisher,
		outbox.WithMaxAttempts(maxAttempts),
		outbox.WithDeadLetter(eventlog.DeadLetterDomain),
	)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}

	for attempt := 1; attempt < maxAttempts; attempt++ {
		result, err := drainer.DrainOnce(ctx)
		if !errors.Is(err, errConversationPoisonRecord) {
			t.Fatalf("drain %d error = %v, want the publish failure", attempt, err)
		}

		if result != (outbox.Result{Claimed: 1, Failed: 1}) {
			t.Fatalf("drain %d result = %+v, want 1 claimed and 1 failed", attempt, result)
		}

		row := readConversationOutboxRow(t, pg, threadID)

		if row.attempts != attempt {
			t.Fatalf("attempts after drain %d = %d, want %d", attempt, row.attempts, attempt)
		}

		if row.lastError == "" {
			t.Fatalf("attempts after drain %d recorded no cause", attempt)
		}

		assertBackoffWithin(t, row.readyInSeconds, conversationOutboxExpectedBackoffSeconds(attempt))

		// The deferral just asserted is the point of the backoff: it is what
		// keeps the next attempt from happening immediately. Release it so the
		// test does not have to wait the delay out.
		makeConversationOutboxRowsDue(t, pg, threadID)
	}

	// The row must still be queued: a refused publish is a deferral, not a loss.
	assertConversationOutboxRowCount(t, pg, threadID, 1)

	// The final attempt exhausts the budget, so the record goes to the
	// dead-letter topic and leaves the queue. The refused topic no longer
	// matches, which is what lets the handoff succeed.
	result, err := drainer.DrainOnce(ctx)
	if err != nil {
		t.Fatalf("dead-letter drain: %v", err)
	}

	if result != (outbox.Result{Claimed: 1, DeadLettered: 1}) {
		t.Fatalf("dead-letter result = %+v, want 1 claimed and 1 dead-lettered", result)
	}

	if len(publisher.records) != 1 {
		t.Fatalf("dead-letter published %d records, want 1", len(publisher.records))
	}

	if got := publisher.records[0].Domain; got != eventlog.DeadLetterDomain(agentosConversationEventsDomain) {
		t.Fatalf("dead-letter topic = %q, want %q", got, eventlog.DeadLetterDomain(agentosConversationEventsDomain))
	}

	assertConversationOutboxRowCount(t, pg, threadID, 0)
}

// TestAgentOSConversationOutboxPostgresOutageKeepsBudget locks the outage
// path: a publish that fails because the log is unreachable defers the row
// behind the backoff but never advances the attempt counter. An outage must
// not spend the dead-letter budget, or a broker outage long enough to exhaust
// it would divert every pending fact to the dead letter on recovery.
func TestAgentOSConversationOutboxPostgresOutageKeepsBudget(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSConversationOutboxPostgresIntegrationDB(t)

	threadID := "thread-outage-" + suffix
	seedConversationOutbox(t, pg, conversationOutboxIntegrationSeed{threadID: threadID, runID: "run-outage-" + suffix, events: 1})

	source := NewAgentOSConversationOutboxSource(pg)
	publisher := &recordingPublisher{
		refusedTopic: agentosConversationEventsDomain,
		refusal:      errConversationLogDown,
	}

	drainer, err := outbox.NewDrainer(source, publisher,
		outbox.WithMaxAttempts(3),
		outbox.WithDeadLetter(eventlog.DeadLetterDomain),
	)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}

	// Four outage drains against a budget of three: every one of them must
	// land on the primary topic — never the dead letter — because none of them
	// spends the budget.
	for range 4 {
		result, err := drainer.DrainOnce(ctx)
		if !errors.Is(err, eventlog.ErrLogUnavailable) {
			t.Fatalf("drain error = %v, want the outage", err)
		}

		if result != (outbox.Result{Claimed: 1, Failed: 1}) {
			t.Fatalf("drain result = %+v, want 1 claimed and 1 failed", result)
		}

		if len(publisher.records) != 0 {
			t.Fatalf("an outage drain published %d records, want 0: the dead letter is not armed", len(publisher.records))
		}

		row := readConversationOutboxRow(t, pg, threadID)

		if row.attempts != 0 {
			t.Fatalf("attempts after an outage drain = %d, want 0: the outage is not the record's fault", row.attempts)
		}

		if row.lastError == "" {
			t.Fatal("an outage drain recorded no cause")
		}

		assertBackoffWithin(t, row.readyInSeconds, conversationOutboxExpectedBackoffSeconds(0))

		makeConversationOutboxRowsDue(t, pg, threadID)
	}

	assertConversationOutboxRowCount(t, pg, threadID, 1)
}

// conversationOutboxRow is one outbox row as the retry assertions need it.
type conversationOutboxRow struct {
	attempts       int
	lastError      string
	readyInSeconds float64
}

type conversationOutboxRowExpectation struct {
	attempts  int
	lastError string
}

// attemptsHeader is the drainer's delivery-attempt header. It is referenced
// here because the header name is part of the record contract a consumer reads.
const attemptsHeader = "attempts"

// makeConversationOutboxRowsDue releases a thread's deferral, standing in for
// the wait a real drainer would do between retries.
func makeConversationOutboxRowsDue(t *testing.T, pg *postgres.Postgres, threadID string) {
	t.Helper()

	if _, err := pg.Pool.Exec(t.Context(), `
UPDATE agentos_conversation_event_outbox SET available_at = NOW() - INTERVAL '1 second'
WHERE thread_id = $1`, threadID); err != nil {
		t.Fatalf("release deferred rows: %v", err)
	}
}

func readConversationOutboxRow(t *testing.T, pg *postgres.Postgres, threadID string) conversationOutboxRow {
	t.Helper()

	var row conversationOutboxRow

	if err := pg.Pool.QueryRow(t.Context(), `
SELECT attempts, last_error, EXTRACT(EPOCH FROM (available_at - NOW()))
FROM agentos_conversation_event_outbox WHERE thread_id = $1`, threadID).Scan(
		&row.attempts, &row.lastError, &row.readyInSeconds,
	); err != nil {
		t.Fatalf("read outbox row: %v", err)
	}

	return row
}

func assertConversationOutboxRows(t *testing.T, pg *postgres.Postgres, threadID string, want conversationOutboxRowExpectation) {
	t.Helper()

	row := readConversationOutboxRow(t, pg, threadID)

	if row.attempts != want.attempts || row.lastError != want.lastError {
		t.Fatalf("outbox row = attempts %d, last_error %q; want attempts %d, last_error %q",
			row.attempts, row.lastError, want.attempts, want.lastError)
	}
}

func assertConversationOutboxRowCount(t *testing.T, pg *postgres.Postgres, threadID string, want int) {
	t.Helper()

	var count int
	if err := pg.Pool.QueryRow(t.Context(), `
SELECT COUNT(*) FROM agentos_conversation_event_outbox WHERE thread_id = $1`, threadID).Scan(&count); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}

	if count != want {
		t.Fatalf("outbox rows = %d, want %d", count, want)
	}
}

// conversationOutboxExpectedBackoffSeconds is the delay the source must apply
// after a given attempt, derived from the same base, cap and floor the shared
// outbox backoff uses — an outage drain charges no attempt, and its backoff is
// the floor.
func conversationOutboxExpectedBackoffSeconds(attempts int) float64 {
	return outboxBackoff(attempts).Seconds()
}

// assertBackoffWithin compares a measured deferral against the expected one with
// enough slack for the clock to move between the write and the read, while still
// failing if the backoff stops growing or stops being applied at all.
func assertBackoffWithin(t *testing.T, got, want float64) {
	t.Helper()

	const slack = 0.5

	if got < want-slack || got > want+slack {
		t.Fatalf("retry deferral = %.2fs, want about %.2fs", got, want)
	}
}

// seedConversationOutbox writes a thread, a run, its events, and one queued
// outbox row per event.
func seedConversationOutbox(t *testing.T, pg *postgres.Postgres, seed conversationOutboxIntegrationSeed) {
	t.Helper()

	ctx := t.Context()

	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_threads (thread_id, account_id, project_id, next_sequence)
VALUES ($1, 'account-'||$1, 'project-'||$1, $2)`, seed.threadID, seed.events); err != nil {
		t.Fatalf("seed thread: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_conversation_runs (run_id, thread_id, account_id, project_id, status, idempotency_key, created_at)
VALUES ($1, $2, 'account-'||$2, 'project-'||$2, 'running', $1||'-key', NOW())`, seed.runID, seed.threadID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_conversation_events (thread_id, sequence, event_id, run_id, event_type, occurred_at, payload)
SELECT $1, g, $1||':event:'||g, $2, $3, NOW() - (($4 - g) * INTERVAL '1 second'),
       jsonb_build_object('delta', g::text, 'message_id', $1||':message')
FROM generate_series(1, $4) g`,
		seed.threadID, seed.runID, string(agentos.ConversationEventTextMessageContent), seed.events); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_conversation_event_outbox (thread_id, sequence)
SELECT $1, g FROM generate_series(1, $2) g`, seed.threadID, seed.events); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
}

func newAgentOSConversationOutboxPostgresIntegrationDB(t *testing.T) (context.Context, *postgres.Postgres, string) {
	t.Helper()

	pgURL := os.Getenv("GOAGENT_POSTGRES_TEST_URL")
	if pgURL == "" {
		t.Fatal("GOAGENT_POSTGRES_TEST_URL is required for postgres_integration tests")
	}

	suffix := postgresIntegrationSuffix(t, agentosConversationOutboxPostgresIntegrationPrefix)
	isolatedURL, cleanup := createPostgresIntegrationDatabase(
		t,
		pgURL,
		agentosConversationOutboxPostgresIntegrationPrefix+suffix,
	)
	t.Cleanup(cleanup)

	pg, err := postgres.New(isolatedURL, postgres.MaxPoolSize(1), postgres.ConnAttempts(1), postgres.ConnTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	t.Cleanup(pg.Close)

	waitForPostgres(t, pg)
	applyAgentOSConversationOutboxMigrations(t, pg)

	return t.Context(), pg, suffix
}

// applyAgentOSConversationOutboxMigrations applies the conversation schema in
// dependency order: the outbox row references the event row it publishes.
func applyAgentOSConversationOutboxMigrations(t *testing.T, pg *postgres.Postgres) {
	t.Helper()

	for _, migration := range []string{
		"20260507000001_create_messages.up.sql",
		"20260725000001_create_agentos_conversations.up.sql",
		"20260726000001_create_agentos_conversation_event_outbox.up.sql",
	} {
		path := filepath.Join("..", "..", "..", "migrations", migration)

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}

		if _, err := pg.Pool.Exec(t.Context(), string(data)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}
}
