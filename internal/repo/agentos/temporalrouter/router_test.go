package temporalrouter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDomain = eventlog.DomainCommands
	testKey    = "run-a"
)

var (
	errTestTemporalUnavailable = errors.New("test: temporal unavailable")
	errTestAckFailure          = errors.New("test: ack failure")
)

// fakeBatch is one polled batch and the ack it recorded.
type fakeBatch struct {
	records  []eventlog.ConsumedRecord
	acks     int
	ackError error
}

func (b *fakeBatch) Records() []eventlog.ConsumedRecord { return b.records }

func (b *fakeBatch) Ack(context.Context) error {
	b.acks++

	return b.ackError
}

// fakeReader hands out prepared batches, then empty ones.
type fakeReader struct {
	batches []*fakeBatch
}

func (r *fakeReader) Poll(context.Context) (eventlog.Batch, error) {
	if len(r.batches) == 0 {
		return &fakeBatch{}, nil
	}

	batch := r.batches[0]
	r.batches = r.batches[1:]

	return batch, nil
}

// fakeIngress records the deliveries Temporal would have received.
type fakeIngress struct {
	deliveries []*Delivery
	err        error
}

func (i *fakeIngress) SignalWithStart(_ context.Context, delivery *Delivery) error {
	if i.err != nil {
		return i.err
	}

	i.deliveries = append(i.deliveries, delivery)

	return nil
}

// factRecord builds the record a peer town's publisher would have produced:
// an eventlog envelope around the fact, exactly as every producer writes one.
//
// The event id is a parameter because it is the fact's identity, and the
// sequence is not: a test that conflated them could not tell a redelivery from
// a new fact.
func factRecord(t *testing.T, eventID, key, factType string, payload map[string]any) eventlog.ConsumedRecord {
	t.Helper()

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	envelope := eventlog.Envelope{
		EventID:   eventID,
		EventType: factType,
		Entity:    eventlog.EntityRef{Kind: eventlog.EntityKindRun, ID: key},
		Payload:   body,
	}

	value, err := envelope.Encode()
	require.NoError(t, err)

	return eventlog.ConsumedRecord{
		Domain:    testDomain,
		Sequence:  2,
		Timestamp: time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC),
		Key:       key,
		Value:     value,
	}
}

func testRoute() Route {
	return Route{
		Domain:       testDomain,
		Type:         "run.requested",
		WorkflowType: "RunSupervisorWorkflow",
		SignalName:   "run.requested",
	}
}

func newTestRouter(t *testing.T, reader eventlog.Reader, ingress Ingress, routes ...Route) *Router {
	t.Helper()

	router, err := NewRouter(Config{Reader: reader, Ingress: ingress, Routes: routes})
	require.NoError(t, err)

	return router
}

func TestNewRouter_ValidatesConfiguration(t *testing.T) {
	t.Parallel()

	_, err := NewRouter(Config{})
	require.ErrorIs(t, err, ErrReaderRequired)

	_, err = NewRouter(Config{Reader: &fakeReader{}})
	require.ErrorIs(t, err, ErrIngressRequired)

	_, err = NewRouter(Config{Reader: &fakeReader{}, Ingress: &fakeIngress{}})
	require.ErrorIs(t, err, ErrRoutesRequired)

	// A route that names no signal could never be delivered; finding that at
	// start-up beats finding it on the first real fact.
	_, err = NewRouter(Config{
		Reader:  &fakeReader{},
		Ingress: &fakeIngress{},
		Routes:  []Route{{Type: "run.requested", WorkflowType: "RunSupervisorWorkflow"}},
	})
	require.ErrorIs(t, err, ErrRouteUndeliverable)
}

// TestRouteBatch_DeliversEveryRoutedFact locks the routing contract: the record
// key becomes the workflow ID, the fact's token travels with it, and the batch is
// acknowledged only once Temporal has taken every delivery.
func TestRouteBatch_DeliversEveryRoutedFact(t *testing.T) {
	t.Parallel()

	batch := &fakeBatch{records: []eventlog.ConsumedRecord{
		factRecord(t, "evt-11", "run-a", "run.requested", map[string]any{"plan": "p-1"}),
		factRecord(t, "evt-12", "run-b", "run.requested", map[string]any{"plan": "p-2"}),
	}}

	ingress := &fakeIngress{}
	router := newTestRouter(t, &fakeReader{batches: []*fakeBatch{batch}}, ingress, testRoute())

	result, err := router.RouteBatch(t.Context())
	require.NoError(t, err)

	assert.Equal(t, Result{Records: 2, Delivered: 2}, result)
	assert.Equal(t, 1, batch.acks, "a fully delivered batch is acknowledged once")
	require.Len(t, ingress.deliveries, 2)

	first := ingress.deliveries[0]
	assert.Equal(t, "run-a", first.WorkflowID, "the record key is the workflow ID")
	assert.Equal(t, "RunSupervisorWorkflow", first.WorkflowType)
	assert.Equal(t, "run.requested", first.SignalName)
	assert.Equal(t, "evt-11", first.Input.Token, "the token is the producer's event id")
	assert.Equal(t, testDomain, first.Input.Domain, "the domain travels with the fact")
	assert.Equal(t, "run-a", first.Input.Key)
	assert.Equal(t, "run.requested", first.Input.Type)
	assert.JSONEq(t, `{"plan":"p-1"}`, string(first.Input.Payload))
	assert.Equal(t, "run-b", ingress.deliveries[1].WorkflowID)
}

// rawEnvelope builds a record from hand-written envelope JSON, for the shapes
// a well-formed producer could never produce but a consumer must still face:
// broken bytes, and envelopes that are missing — or ahead of — the contract.
func rawEnvelope(sequence uint64, key, envelope string) eventlog.ConsumedRecord {
	return eventlog.ConsumedRecord{
		Domain:   testDomain,
		Sequence: sequence,
		Key:      key,
		Value:    []byte(envelope),
	}
}

// TestRouteBatch_SkipsFactsNoRouteClaims locks the subscription model: a town
// reads a domain, not a private copy of it, so a fact it does not route is
// skipped and the batch still progresses. It also covers the five ways a record
// is refused: unclaimed type, undecodable body, no entity, no identity, and a
// schema this build cannot read.
func TestRouteBatch_SkipsFactsNoRouteClaims(t *testing.T) {
	t.Parallel()

	batch := &fakeBatch{records: []eventlog.ConsumedRecord{
		factRecord(t, "evt-1", "run-a", "run.requested", nil),
		factRecord(t, "evt-2", "run-b", "shipment.dispatched", nil),
		rawEnvelope(3, "run-c", "{not json"),
		rawEnvelope(4, "run-c", `{"schema":1,"event_id":"evt-4","event_type":"run.requested"}`),
		rawEnvelope(5, "run-d", `{"schema":1,"event_type":"run.requested","entity":{"kind":"run","id":"run-d"}}`),
		rawEnvelope(6, "run-e", `{"schema":2,"event_id":"evt-6","event_type":"run.requested","entity":{"kind":"run","id":"run-e"}}`),
	}}

	ingress := &fakeIngress{}
	router := newTestRouter(t, &fakeReader{batches: []*fakeBatch{batch}}, ingress, testRoute())

	result, err := router.RouteBatch(t.Context())
	require.NoError(t, err)

	assert.Equal(t, Result{Records: 6, Delivered: 1, Skipped: 5}, result)
	assert.Equal(t, 1, batch.acks)
	require.Len(t, ingress.deliveries, 1)
	assert.Equal(t, "run-a", ingress.deliveries[0].WorkflowID)
}

// TestRouteBatch_DoesNotAckWhenTemporalRefuses locks the acknowledgement
// boundary: the consumer's position advances only on ack, so a fact Temporal
// never accepted must be redelivered rather than skipped past.
func TestRouteBatch_DoesNotAckWhenTemporalRefuses(t *testing.T) {
	t.Parallel()

	batch := &fakeBatch{records: []eventlog.ConsumedRecord{
		factRecord(t, "evt-21", "run-a", "run.requested", nil),
	}}

	ingress := &fakeIngress{err: errTestTemporalUnavailable}
	router := newTestRouter(t, &fakeReader{batches: []*fakeBatch{batch}}, ingress, testRoute())

	result, err := router.RouteBatch(t.Context())
	require.ErrorIs(t, err, errTestTemporalUnavailable)

	assert.Equal(t, Result{Records: 1, Failed: 1}, result)
	assert.Zero(t, batch.acks, "a fact Temporal refused must stay unacknowledged")
}

// TestRouteBatch_ReportsAckFailure locks that a failed commit is surfaced:
// silently continuing would redeliver the batch forever without telling anyone.
func TestRouteBatch_ReportsAckFailure(t *testing.T) {
	t.Parallel()

	batch := &fakeBatch{
		records:  []eventlog.ConsumedRecord{factRecord(t, "evt-1", testKey, "run.requested", nil)},
		ackError: errTestAckFailure,
	}

	router := newTestRouter(t, &fakeReader{batches: []*fakeBatch{batch}}, &fakeIngress{}, testRoute())

	_, err := router.RouteBatch(t.Context())
	require.ErrorIs(t, err, errTestAckFailure)
}

// TestRouteBatch_UsesRouteWorkflowIDOverride locks that the key is a default, not
// a cage: a route may derive the workflow ID when the entity key is not the
// workflow identity.
func TestRouteBatch_UsesRouteWorkflowIDOverride(t *testing.T) {
	t.Parallel()

	batch := &fakeBatch{records: []eventlog.ConsumedRecord{factRecord(t, "evt-1", "run-a", "run.requested", nil)}}

	route := testRoute()
	route.WorkflowID = func(fact *Fact) string { return "agentos-run-" + fact.Key }

	ingress := &fakeIngress{}
	router := newTestRouter(t, &fakeReader{batches: []*fakeBatch{batch}}, ingress, route)

	_, err := router.RouteBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, ingress.deliveries, 1)
	assert.Equal(t, "agentos-run-run-a", ingress.deliveries[0].WorkflowID)
}

// TestRouteBatch_DomainScopedRouteDoesNotClaimAnotherDomain locks that an empty
// domain matches anything while a named domain matches only itself, so a route
// registered for commands cannot fire on a peer's timeline.
func TestRouteBatch_DomainScopedRouteDoesNotClaimAnotherDomain(t *testing.T) {
	t.Parallel()

	record := factRecord(t, "evt-1", "run-a", "run.requested", nil)
	record.Domain = eventlog.DomainRunTimeline

	ingress := &fakeIngress{}
	router := newTestRouter(t, &fakeReader{batches: []*fakeBatch{{records: []eventlog.ConsumedRecord{record}}}}, ingress, testRoute())

	result, err := router.RouteBatch(t.Context())
	require.NoError(t, err)

	assert.Equal(t, Result{Records: 1, Skipped: 1}, result)
	assert.Empty(t, ingress.deliveries)
}

func TestRouter_RunStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, &fakeReader{}, &fakeIngress{}, testRoute())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, router.Run(ctx))
}

// TestFactTokenIsStableAcrossRedelivery locks the dedupe contract: the token is
// the producer's event id, so it survives a redelivery — and a copy of the same
// fact in another town, where the log position is different. That is why the
// position is deliberately not part of it.
func TestFactTokenIsStableAcrossRedelivery(t *testing.T) {
	t.Parallel()

	here := Fact{Domain: testDomain, Sequence: 42, EventID: "evt-42"}
	there := Fact{Domain: eventlog.DomainPlanEvents, Sequence: 7, EventID: "evt-42"}

	assert.Equal(t, "evt-42", here.Token())
	assert.Equal(t, here.Token(), there.Token(), "the same fact has one token wherever it is read")
}
