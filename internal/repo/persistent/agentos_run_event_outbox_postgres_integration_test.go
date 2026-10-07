//go:build postgres_integration

package persistent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/outbox"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/require"
)

// agentosRunOutboxIntegrationPrefix names the throwaway databases this suite
// creates, so a leaked database identifies its suite.
const agentosRunOutboxIntegrationPrefix = "goagent_run_outbox_"

// TestAgentOSRunOutboxAppendQueuesOnce locks the write-side contract: with the
// outbox enabled, one append stores the event and queues it exactly once in
// the same transaction; a replayed append queues nothing; without the option —
// a deployment with no drainer — nothing is queued at all.
func TestAgentOSRunOutboxAppendQueuesOnce(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSRunOutboxIntegrationDB(t)

	runID := "run-append-" + suffix
	registerRunBackend(t, pg, runID, "account-"+suffix, "project-"+suffix)

	repo := NewAgentOSRunEventRepo(pg, WithRunEventOutbox())

	ev := &agentoscore.Event{
		EventID: "evt-" + suffix, EventType: agentoscore.EventRunStarted,
		RunID: runID, Sequence: 1, Timestamp: time.Now().UTC(),
		Payload: map[string]any{"message": "hi"},
	}

	require.NoError(t, repo.AppendRunEvent(ctx, ev))
	require.NoError(t, repo.AppendRunEvent(ctx, ev), "a replayed append is a no-op, not an error")

	assertAgentOSRunOutboxCount(t, pg, runID, 1, "one append queues exactly one row")

	// A repo without the option is the no-drainer deployment: durable, and
	// deliberately not queued.
	plain := NewAgentOSRunEventRepo(pg)

	next := &agentoscore.Event{
		EventID: "evt-2-" + suffix, EventType: agentoscore.EventRunCompleted,
		RunID: runID, Sequence: 2, Timestamp: time.Now().UTC(),
	}

	require.NoError(t, plain.AppendRunEvent(ctx, next))

	assertAgentOSRunOutboxCount(t, pg, runID, 1, "a no-drainer deployment stores without queueing")
}

// TestAgentOSRunOutboxClaimShape locks the row-to-record mapping: the handle is
// the event's own ID, the record is keyed by run, the payload is read from the
// event table rather than copied into the outbox, a run's facts come back in
// sequence order, and the tenant comes from the run's registration — while an
// unregistered run still publishes, with an empty tenant rather than not at
// all.
func TestAgentOSRunOutboxClaimShape(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSRunOutboxIntegrationDB(t)

	registered := "run-registered-" + suffix
	registerRunBackend(t, pg, registered, "account-"+suffix, "project-"+suffix)
	seedRunEvents(t, pg, registered, 2)

	unregistered := "run-unregistered-" + suffix
	seedRunEvents(t, pg, unregistered, 1)

	source := NewAgentOSRunOutboxSource(pg)

	pending, err := source.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}

	if len(pending) != 3 {
		t.Fatalf("claim returned %d records, want 3", len(pending))
	}

	// The registered run's two facts arrive sequence-ordered: that order is
	// what puts a run's timeline in order inside its partition.
	sequences := make([]int64, 0, 2)

	for i := range pending {
		record := &pending[i]

		if record.Domain != agentosRunTimelineDomain {
			t.Fatalf("record %d domain = %q, want %q", i, record.Domain, agentosRunTimelineDomain)
		}

		envelope, err := eventlog.DecodeEnvelope(record.Payload)
		if err != nil {
			t.Fatalf("decode record %d envelope: %v", i, err)
		}

		if record.ID != envelope.EventID {
			t.Fatalf("record %d handle = %q, want the envelope's event id %q", i, record.ID, envelope.EventID)
		}

		if envelope.Entity.Kind != eventlog.EntityKindRun || envelope.Entity.ID != string(record.Key) {
			t.Fatalf("record %d entity = %s/%s, want the run %q the key names",
				i, envelope.Entity.Kind, envelope.Entity.ID, record.Key)
		}

		var event agentoscore.Event
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			t.Fatalf("decode record %d fact body: %v", i, err)
		}

		if event.RunID != string(record.Key) || event.Sequence <= 0 {
			t.Fatalf("record %d fact = run %q sequence %d, want the key's run and a positive sequence",
				i, event.RunID, event.Sequence)
		}

		if event.Payload["message"] == nil {
			t.Fatalf("record %d lost its payload: %+v", i, event.Payload)
		}

		if string(record.Key) == registered {
			sequences = append(sequences, event.Sequence)

			want := eventlog.TenantRef{AccountID: "account-" + suffix, ProjectID: "project-" + suffix}
			if envelope.Tenant != want {
				t.Fatalf("registered run tenant = %+v, want %+v", envelope.Tenant, want)
			}
		} else if envelope.Tenant != (eventlog.TenantRef{}) {
			t.Fatalf("unregistered run tenant = %+v, want the empty tenant: a fact is never stranded", envelope.Tenant)
		}
	}

	if len(sequences) != 2 || sequences[0] >= sequences[1] {
		t.Fatalf("registered run sequences = %v, want two ascending", sequences)
	}
}

// TestAgentOSRunOutboxDrain runs the real drainer against the real outbox:
// every queued fact reaches the run.timeline domain once, the rows it covered
// are gone, and the claimed lease hides in-flight rows from a second claim.
func TestAgentOSRunOutboxDrain(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSRunOutboxIntegrationDB(t)

	runID := "run-drain-" + suffix
	registerRunBackend(t, pg, runID, "account-"+suffix, "project-"+suffix)
	seedRunEvents(t, pg, runID, 3)

	source := NewAgentOSRunOutboxSource(pg)
	publisher := &recordingPublisher{}

	// The dead-letter mapping is the log's own — the same function the
	// backbone wires — so the domain a drainer dead-letters to here is the
	// domain the provisioned dead-letter stream carries.
	drainer, err := outbox.NewDrainer(source, publisher, outbox.WithDeadLetter(eventlog.DeadLetterDomain))
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}

	// A claim is a lease: before the drain finishes, a second source sees none
	// of the in-flight rows.
	claimed, err := source.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	if len(claimed) != 3 {
		t.Fatalf("first claim returned %d records, want 3", len(claimed))
	}

	overlapping, err := NewAgentOSRunOutboxSource(pg).ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("overlapping claim: %v", err)
	}

	if len(overlapping) != 0 {
		t.Fatalf("a second drainer claimed %d in-flight rows, want 0", len(overlapping))
	}

	// The lease lapses, standing in for a drainer that crashed mid-publish:
	// the rows become claimable again — at-least-once, never at-most-once.
	makeAgentOSRunOutboxRowsDue(t, pg, runID)

	result, err := drainer.DrainOnce(ctx)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}

	if result != (outbox.Result{Claimed: 3, Published: 3}) {
		t.Fatalf("drain result = %+v, want 3 claimed and 3 published", result)
	}

	for i := range publisher.records {
		if publisher.records[i].Domain != agentosRunTimelineDomain {
			t.Fatalf("published domain = %q, want %q", publisher.records[i].Domain, agentosRunTimelineDomain)
		}
	}

	assertAgentOSRunOutboxCount(t, pg, runID, 0, "a successful drain empties the outbox")
}

// registerRunBackend registers a run in the router's index, which is where a
// run fact's tenant is read from at claim time.
func registerRunBackend(t *testing.T, pg *postgres.Postgres, runID, accountID, projectID string) {
	t.Helper()

	if _, err := pg.Pool.Exec(t.Context(), `
INSERT INTO run_backend_index (run_id, account_id, project_id, backend_kind, backend_name, idempotency_key)
VALUES ($1, $2, $3, 'native', 'goagent-native', $1||':bind')`, runID, accountID, projectID); err != nil {
		t.Fatalf("seed run backend index: %v", err)
	}
}

// seedRunEvents stores one run's milestones and queues each of them, the way
// an outbox-enabled append does.
func seedRunEvents(t *testing.T, pg *postgres.Postgres, runID string, events int) {
	t.Helper()

	if _, err := pg.Pool.Exec(t.Context(), `
INSERT INTO agentos_run_events (run_id, sequence, event_id, event_type, occurred_at, payload)
SELECT $1, g, $1||':event:'||g, $2, NOW() - (($3 - g) * INTERVAL '1 second'),
       jsonb_build_object('message', g::text)
FROM generate_series(1, $3) g`,
		runID, string(agentoscore.EventRunStarted), events); err != nil {
		t.Fatalf("seed run events: %v", err)
	}

	if _, err := pg.Pool.Exec(t.Context(), `
INSERT INTO agentos_run_event_outbox (run_id, sequence)
SELECT $1, g FROM generate_series(1, $2) g`, runID, events); err != nil {
		t.Fatalf("seed run outbox: %v", err)
	}
}

// makeAgentOSRunOutboxRowsDue releases a run's deferral or lease, standing in
// for the wait a real drainer would do.
func makeAgentOSRunOutboxRowsDue(t *testing.T, pg *postgres.Postgres, runID string) {
	t.Helper()

	if _, err := pg.Pool.Exec(t.Context(), `
UPDATE agentos_run_event_outbox SET available_at = NOW() - INTERVAL '1 second'
WHERE run_id = $1`, runID); err != nil {
		t.Fatalf("release run outbox rows: %v", err)
	}
}

func assertAgentOSRunOutboxCount(t *testing.T, pg *postgres.Postgres, runID string, want int, because string) {
	t.Helper()

	var count int
	if err := pg.Pool.QueryRow(t.Context(), `
SELECT COUNT(*) FROM agentos_run_event_outbox WHERE run_id = $1`, runID).Scan(&count); err != nil {
		t.Fatalf("count run outbox rows: %v", err)
	}

	if count != want {
		t.Fatalf("run outbox rows = %d, want %d (%s)", count, want, because)
	}
}

func newAgentOSRunOutboxIntegrationDB(t *testing.T) (context.Context, *postgres.Postgres, string) {
	t.Helper()

	pgURL := os.Getenv("GOAGENT_POSTGRES_TEST_URL")
	if pgURL == "" {
		t.Fatal("GOAGENT_POSTGRES_TEST_URL is required for postgres_integration tests")
	}

	suffix := postgresIntegrationSuffix(t, agentosRunOutboxIntegrationPrefix)
	isolatedURL, cleanup := createPostgresIntegrationDatabase(
		t,
		pgURL,
		agentosRunOutboxIntegrationPrefix+suffix,
	)
	t.Cleanup(cleanup)

	pg, err := postgres.New(isolatedURL, postgres.MaxPoolSize(1), postgres.ConnAttempts(1), postgres.ConnTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(pg.Close)

	waitForPostgres(t, pg)
	applyAgentOSRunOutboxMigrations(t, pg)

	return t.Context(), pg, suffix
}

// applyAgentOSRunOutboxMigrations applies the baseline schema.
func applyAgentOSRunOutboxMigrations(t *testing.T, pg *postgres.Postgres) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "20261010000001_baseline.up.sql"))
	if err != nil {
		t.Fatalf("read baseline schema: %v", err)
	}

	if _, err := pg.Pool.Exec(t.Context(), string(data)); err != nil {
		t.Fatalf("apply baseline schema: %v", err)
	}
}
