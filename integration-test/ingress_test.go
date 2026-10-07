package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	eventlognats "github.com/TekkenSteve/GoAgent/pkg/eventlog/nats"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
)

// The integration stack configures a cross-town route for exactly these values
// (docker-compose-integration-test.yml, app service). Nothing polls the probe
// queue on purpose: what the bridge owes is the delivery into Temporal, and a
// started execution is that delivery — what the workflow then does is the
// business's concern, not the bridge's.
const (
	ingressProbeFactType = "cross.city.probe"
	ingressProbeWorkflow = "crossCityIngressProbe"
	ingressProbeAccount  = "e2e-test-account"
	ingressProbeProject  = "e2e-test-project"
	// ingressProbeFactSequence is the run-event sequence the published payload
	// carries, so the projected row is addressable.
	ingressProbeFactSequence = 1
)

// Errors a wait reports while it is still waiting: static, so a failure names
// the condition rather than only the last attempt's text.
var (
	// errIngressProbeWorkflowType reports a workflow started for the fact under
	// a type the route did not name.
	errIngressProbeWorkflowType = errors.New("ingress probe workflow has the wrong type")
	// errProjectedFactMissing reports a fact the log projector has not landed.
	errProjectedFactMissing = errors.New("the fact is not projected yet")
)

// TestCrossCityIngressDeliversFact locks the cross-town bridge end to end: one
// fact published on the run timeline domain is consumed by the app's ingress
// consumer, matched by the deployment's route table, and delivered into
// Temporal as a signal-with-start whose workflow id is the fact's entity.
//
// The projected row proves the same fact also reached the log projector, which
// is the other consumer of the domain: two independent readers, one fact.
func TestCrossCityIngressDeliversFact(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	runID := fmt.Sprintf("e2e-ingress-%d", time.Now().UnixNano())

	publishIngressFact(ctx, t, runID)
	waitForIngressWorkflow(ctx, t, runID)
	waitForProjectedFact(ctx, t, runID)
}

// natsURL is the backbone the app publishes to, overridable for a host run.
func natsURL() string {
	if url := os.Getenv("INTEGRATION_TEST_NATS_URL"); url != "" {
		return url
	}

	return "nats://guest:guest@nats:4222"
}

// temporalAddress is where the app's workers and the bridge deliver.
func temporalAddress() string {
	if address := os.Getenv("INTEGRATION_TEST_TEMPORAL_ADDRESS"); address != "" {
		return address
	}

	return "temporal:7233"
}

// publishIngressFact puts one fact on the run timeline through the publisher
// the drainer itself uses, so the subject, the shard and the envelope are the
// production shapes rather than a hand-rolled approximation.
//
// The envelope's type is the probe type the stack routes on, while its payload
// is a real run event: the projector consumes the same record and lands it,
// which is what makes this one fact exercise both readers.
func publishIngressFact(ctx context.Context, t *testing.T, runID string) {
	t.Helper()

	eventID := runID + "-started"

	payload, err := json.Marshal(agentoscore.Event{
		EventID:   eventID,
		EventType: "run.started",
		RunID:     runID,
		Sequence:  ingressProbeFactSequence,
		Timestamp: time.Now().UTC(),
		Payload:   map[string]any{"source": "cross-city-ingress-test"},
	})
	if err != nil {
		t.Fatalf("marshal run event: %v", err)
	}

	envelope := eventlog.Envelope{
		EventID:    eventID,
		EventType:  ingressProbeFactType,
		OccurredAt: time.Now().UTC(),
		Entity:     eventlog.EntityRef{Kind: eventlog.EntityKindRun, ID: runID},
		Tenant:     eventlog.TenantRef{AccountID: ingressProbeAccount, ProjectID: ingressProbeProject},
		Payload:    payload,
	}

	value, err := envelope.Encode()
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}

	publisher, err := eventlognats.NewPublisher(eventlognats.Config{
		URL:           natsURL(),
		SubjectPrefix: "agentos",
		Shards:        16,
		ClientName:    "integration-cross-city-probe",
	})
	if err != nil {
		t.Fatalf("nats publisher: %v", err)
	}

	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close publisher: %v", err)
		}
	})

	if err := publisher.Publish(ctx, &eventlog.Record{
		Domain: eventlog.DomainRunTimeline,
		Key:    runID,
		ID:     eventID,
		Value:  value,
	}); err != nil {
		t.Fatalf("publish fact: %v", err)
	}
}

// waitForIngressWorkflow waits for the execution the bridge derives from the
// fact's entity. The workflow id is the entity id, which is the key the whole
// routing model hinges on: the same entity always lands on the same execution.
func waitForIngressWorkflow(ctx context.Context, t *testing.T, runID string) {
	t.Helper()

	temporal, err := client.Dial(client.Options{
		HostPort:  temporalAddress(),
		Namespace: "default",
	})
	if err != nil {
		t.Fatalf("temporal client: %v", err)
	}

	defer temporal.Close()

	waitFor(ctx, t, fmt.Sprintf("workflow %q started by the ingress bridge", runID), func() error {
		described, describeErr := temporal.DescribeWorkflowExecution(ctx, runID, "")
		if describeErr != nil {
			return describeErr
		}

		if described.GetWorkflowExecutionInfo().GetType().GetName() != ingressProbeWorkflow {
			return fmt.Errorf("%w: got %q, want %q", errIngressProbeWorkflowType,
				described.GetWorkflowExecutionInfo().GetType().GetName(), ingressProbeWorkflow)
		}

		return nil
	})
}

// waitForProjectedFact waits for the projector to land the same fact, which is
// the other half of "both readers saw it".
func waitForProjectedFact(ctx context.Context, t *testing.T, runID string) {
	t.Helper()

	pool, err := pgxpool.New(ctx, getPGURL())
	if err != nil {
		t.Fatalf("postgres pool: %v", err)
	}

	defer pool.Close()

	waitFor(ctx, t, fmt.Sprintf("run event %q projected", runID), func() error {
		var count int

		scanErr := pool.QueryRow(ctx,
			`SELECT count(*) FROM agentos_run_events WHERE run_id = $1 AND sequence = $2`,
			runID, ingressProbeFactSequence,
		).Scan(&count)
		if scanErr != nil {
			return scanErr
		}

		if count == 0 {
			return errProjectedFactMissing
		}

		return nil
	})
}

// waitFor polls check until it reports success or the context ends.
func waitFor(ctx context.Context, t *testing.T, what string, check func() error) {
	t.Helper()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error

	for {
		if lastErr = check(); lastErr == nil {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", what, lastErr)
		case <-ticker.C:
		}
	}
}
