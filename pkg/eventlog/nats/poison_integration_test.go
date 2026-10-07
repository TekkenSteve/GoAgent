package nats

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// createDeadLetterStream provisions the shared dead-letter stream the parking
// path publishes into. It mirrors scripts/nats/create-streams.sh: one stream for
// every domain's poison records.
func createDeadLetterStream(t *testing.T, config Config) {
	t.Helper()

	connection, err := nats.Connect(config.URL, nats.Timeout(config.ConnectTimeout))
	require.NoError(t, err, "connect to provision the dead-letter stream")

	stream, err := jetstream.New(connection)
	require.NoError(t, err, "jetstream context")

	name := config.streamFor(eventlog.DeadLetterDomain(_testDomain))
	_, err = stream.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       name,
		Subjects:   []string{config.SubjectPrefix + "." + eventlog.DeadLetterDomain(_testDomain) + ".>"},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     _testMaxAge,
		Duplicates: _testDupeWindow,
		Storage:    jetstream.FileStorage,
	})
	require.NoError(t, err, "create dead-letter stream %s", name)

	t.Cleanup(func() {
		if err := stream.DeleteStream(context.Background(), name); err != nil {
			t.Logf("deleting dead-letter stream %s: %v", name, err)
		}

		connection.Close()
	})
}

// publishPoisonFact puts one record on the test domain for the poison contract
// to fail on.
func publishPoisonFact(ctx context.Context, t *testing.T, config Config) {
	t.Helper()

	publisher, err := NewPublisher(config)
	require.NoError(t, err)

	publish(ctx, t, publisher, "run-poison", "evt-poison", 1)
	require.NoError(t, publisher.Close())
}

// awaitParkedRecord drives the failing consumer until the poison policy parks
// the record, then returns the dead-letter batch holding it.
//
// A pull consumer only moves when someone fetches from it, so the wait has to
// keep polling the original consumer: the redelivery is what carries the
// delivery count that triggers the park.
func awaitParkedRecord(ctx context.Context, t *testing.T, reader, deadLetter *Reader) eventlog.Batch {
	t.Helper()

	var parked eventlog.Batch

	require.Eventually(t, func() bool {
		if _, pollErr := reader.Poll(ctx); pollErr != nil {
			return false
		}

		batch, pollErr := deadLetter.Poll(ctx)
		if pollErr != nil || len(batch.Records()) == 0 {
			return false
		}

		parked = batch

		return true
	}, _testPollBudget*3, 50*time.Millisecond, "the exhausted record must be parked")

	return parked
}

// TestNATSParksExhaustedRecordInDeadLetterDomain locks the poison contract end
// to end: the record is handed to the consumer until its delivery budget is
// spent, the last delivery is used to park it in the domain's dead-letter
// stream, and it is acknowledged there so the shard keeps moving.
func TestNATSParksExhaustedRecordInDeadLetterDomain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget*3)
	defer cancel()

	config := testConfig(t)
	createStream(t, config)
	createDeadLetterStream(t, config)
	publishPoisonFact(ctx, t, config)

	reader, err := NewReader(ctx, &ReaderConfig{
		Config:     config,
		Domain:     _testDomain,
		Consumer:   "poison-park",
		MaxWait:    _testMaxWait,
		AckWait:    _testAckWait,
		BatchSize:  10,
		MaxDeliver: 2,
		OnPoison:   PoisonDeadLetter,
	})
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	first := poll(ctx, t, reader)
	require.Len(t, first.Records(), 1, "the first delivery is handed to the consumer")

	// Deliberately not acknowledged: this consumer keeps failing the record, so
	// the next delivery is its last.

	deadLetter, err := NewReader(ctx, &ReaderConfig{
		Config:    config,
		Domain:    eventlog.DeadLetterDomain(_testDomain),
		Consumer:  "poison-park-dead-letter",
		MaxWait:   _testMaxWait,
		AckWait:   _testAckWait,
		BatchSize: 10,
	})
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, deadLetter.Close()) })

	parkedBatch := awaitParkedRecord(ctx, t, reader, deadLetter)

	record := parkedBatch.Records()[0]
	require.Equal(t, "run-poison", record.Key)
	require.Equal(t, "2", record.Headers[HeaderPoisonDeliveries])
	require.Contains(t, record.Headers[HeaderPoisonSource], config.SubjectPrefix)

	require.NoError(t, parkedBatch.Ack(ctx))

	require.Eventually(t, func() bool {
		batch, pollErr := reader.Poll(ctx)
		if pollErr != nil {
			return false
		}

		for _, lingering := range batch.Records() {
			if lingering.Key == "run-poison" {
				return false
			}
		}

		return true
	}, _testPollBudget, 50*time.Millisecond, "a parked record must not be handed over again")
}

// TestNATSReaderReportsMissingStream locks the sentinel a host waits on: a
// consumer whose stream has not been provisioned is distinguishable from every
// other failure, which is what makes "wait for the stream" a policy instead of
// a blind retry.
func TestNATSReaderReportsMissingStream(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget)
	defer cancel()

	config := testConfig(t)

	_, err := NewReader(ctx, &ReaderConfig{
		Config:   config,
		Domain:   _testDomain,
		Consumer: "stream-missing",
	})
	require.ErrorIs(t, err, ErrStreamNotFound)
}
