package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAttempts  = 3
	testBatchSize = 10
)

var (
	errTestLogDown   = errors.New("outbox test: log unavailable")
	errTestClaimDown = errors.New("outbox test: claim unavailable")
)

type fakeSource struct {
	mu sync.Mutex

	pending    []Pending
	claimErr   error
	published  []string
	failed     []failedRecord
	publishErr error
}

type failedRecord struct {
	id       string
	cause    string
	attempts int
}

func (s *fakeSource) ClaimPending(context.Context, int) ([]Pending, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimErr != nil {
		return nil, s.claimErr
	}

	return append([]Pending(nil), s.pending...), nil
}

func (s *fakeSource) MarkPublished(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.publishErr != nil {
		return s.publishErr
	}

	s.published = append(s.published, ids...)

	return nil
}

func (s *fakeSource) MarkFailed(_ context.Context, id, cause string, attempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failed = append(s.failed, failedRecord{id: id, cause: cause, attempts: attempts})

	return nil
}

type fakePublisher struct {
	mu       sync.Mutex
	records  []eventlog.Record
	failures map[string]error
}

func (p *fakePublisher) Publish(_ context.Context, record *eventlog.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.failures[record.Domain]; err != nil {
		return err
	}

	p.records = append(p.records, *record)

	return nil
}

func newTestDrainer(t *testing.T, source PendingSource, publisher eventlog.Publisher, options ...Option) *Drainer {
	t.Helper()

	drainer, err := NewDrainer(source, publisher, options...)
	require.NoError(t, err)

	return drainer
}

func TestNewDrainer_ValidatesDependencies(t *testing.T) {
	t.Parallel()

	_, err := NewDrainer(nil, &fakePublisher{})
	require.ErrorIs(t, err, ErrSourceRequired)

	_, err = NewDrainer(&fakeSource{}, nil)
	require.ErrorIs(t, err, ErrPublisherRequired)
}

func TestDrainOnce_PublishesAndMarksPublished(t *testing.T) {
	t.Parallel()

	source := &fakeSource{pending: []Pending{
		{ID: "1", Domain: eventlog.DomainRunTimeline, Key: "run-1", Payload: []byte(`{"a":1}`)},
		{ID: "2", Domain: eventlog.DomainRunTimeline, Key: "run-2", Payload: []byte(`{"a":2}`)},
	}}
	publisher := &fakePublisher{}

	result, err := newTestDrainer(t, source, publisher).DrainOnce(t.Context())
	require.NoError(t, err)

	assert.Equal(t, Result{Claimed: 2, Published: 2}, result)
	assert.Equal(t, []string{"1", "2"}, source.published)
	require.Len(t, publisher.records, 2)
	assert.Equal(t, "run-1", publisher.records[0].Key)
	assert.Equal(t, "1", publisher.records[0].Headers[headerAttempts], "first delivery is attempt 1")
}

func TestDrainOnce_FailureLeavesRowForRetry(t *testing.T) {
	t.Parallel()

	source := &fakeSource{pending: []Pending{{ID: "1", Domain: eventlog.DomainRunTimeline, Attempts: testAttempts - 2}}}
	publisher := &fakePublisher{failures: map[string]error{"run.timeline": errTestLogDown}}

	result, err := newTestDrainer(t, source, publisher).DrainOnce(t.Context())
	require.ErrorIs(t, err, errTestLogDown)

	assert.Equal(t, Result{Claimed: 1, Failed: 1}, result)
	assert.Empty(t, source.published, "a failed record must stay unpublished")
	require.Len(t, source.failed, 1)
	assert.Equal(t, testAttempts-1, source.failed[0].attempts, "the attempt counter advances")
}

// TestDrainOnce_OutageDoesNotSpendDeadLetterBudget locks the outage rule: a
// failure the log itself caused — not the record — retries behind the backoff
// without advancing the attempt counter. Without this, a broker outage long
// enough to exhaust the budget would divert every pending fact to the dead
// letter the moment the broker came back.
func TestDrainOnce_OutageDoesNotSpendDeadLetterBudget(t *testing.T) {
	t.Parallel()

	source := &fakeSource{pending: []Pending{{ID: "1", Domain: eventlog.DomainRunTimeline, Attempts: testAttempts - 2}}}
	publisher := &fakePublisher{failures: map[string]error{
		"run.timeline": fmt.Errorf("outbox test: broker down: %w", eventlog.ErrLogUnavailable),
	}}

	drainer := newTestDrainer(t, source, publisher, WithMaxAttempts(testAttempts))

	result, err := drainer.DrainOnce(t.Context())
	require.ErrorIs(t, err, eventlog.ErrLogUnavailable)

	assert.Equal(t, Result{Claimed: 1, Failed: 1}, result)
	require.Len(t, source.failed, 1)
	assert.Equal(t, testAttempts-2, source.failed[0].attempts,
		"an outage is not the record's fault, so the budget is untouched")
}

func TestDrainOnce_DeadLettersPoisonRecord(t *testing.T) {
	t.Parallel()

	source := &fakeSource{pending: []Pending{{ID: "1", Domain: eventlog.DomainRunTimeline, Attempts: testAttempts - 1}}}
	publisher := &fakePublisher{}

	drainer := newTestDrainer(t, source, publisher,
		WithMaxAttempts(testAttempts),
		WithDeadLetter(eventlog.DeadLetterDomain),
	)

	result, err := drainer.DrainOnce(t.Context())
	require.NoError(t, err)

	assert.Equal(t, Result{Claimed: 1, DeadLettered: 1}, result)
	require.Len(t, publisher.records, 1)
	assert.Equal(t, eventlog.DeadLetterDomain(eventlog.DomainRunTimeline), publisher.records[0].Domain,
		"the record is moved aside, not dropped")
	assert.Equal(t, []string{"1"}, source.published)
}

func TestDrainOnce_EmptyBatchIsNoop(t *testing.T) {
	t.Parallel()

	source := &fakeSource{}

	result, err := newTestDrainer(t, source, &fakePublisher{}).DrainOnce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, Result{}, result)
	assert.Empty(t, source.published)
}

func TestDrainOnce_ReportsClaimFailure(t *testing.T) {
	t.Parallel()

	source := &fakeSource{claimErr: errTestClaimDown}

	_, err := newTestDrainer(t, source, &fakePublisher{}).DrainOnce(t.Context())
	require.ErrorIs(t, err, errTestClaimDown)
}

func TestRun_StopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	source := &fakeSource{}

	require.NoError(t, newTestDrainer(t, source, &fakePublisher{}, WithInterval(time.Millisecond)).Run(ctx))
}
