package nats

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// These tests drive a real NATS server, because the properties worth locking
// here — that an unacknowledged record comes back, that the duplicate window
// collapses a re-publish, that a shard keeps its entity's order — are
// properties of the server, not of this package.
//
// They are skipped unless AGENTFW_NATS_TEST_URL points at a server with
// JetStream enabled (docker run -d -p 4222:4222 nats:2-alpine -js).

const (
	_testDomain        = eventlog.DomainRunTimeline
	_testMaxAge        = time.Hour
	_testMaxWait       = 500 * time.Millisecond
	_testAckWait       = time.Second
	_testPollBudget    = 10 * time.Second
	_testDupeWindow    = 2 * time.Minute
	_testConnectBudget = 10 * time.Second
)

// testConfig builds a configuration with a prefix unique to this test run, so
// the suite can run against a shared server, in parallel, without tests
// seeing each other's records.
func testConfig(t *testing.T) Config {
	t.Helper()

	url := os.Getenv("AGENTFW_NATS_TEST_URL")
	if url == "" {
		t.Skip("set AGENTFW_NATS_TEST_URL to run the NATS event backbone tests")
	}

	return Config{
		URL:            url,
		SubjectPrefix:  "agentostest" + strconv.FormatInt(time.Now().UnixNano(), 36),
		Shards:         4,
		ClientName:     "agentos-eventlog-test",
		ConnectTimeout: _testConnectBudget,
	}
}

// createStream provisions the domain the same way scripts/nats/create-streams.sh
// does — age-based retention, a duplicate window, the domain's subjects — and
// removes it when the test ends.
func createStream(t *testing.T, config Config) {
	t.Helper()

	connection, err := nats.Connect(config.URL, nats.Timeout(config.ConnectTimeout))
	require.NoError(t, err, "connect to provision the stream")

	stream, err := jetstream.New(connection)
	require.NoError(t, err, "jetstream context")

	name := config.streamFor(_testDomain)
	_, err = stream.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       name,
		Subjects:   []string{config.SubjectPrefix + "." + _testDomain + ".>"},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     _testMaxAge,
		Duplicates: _testDupeWindow,
		Storage:    jetstream.FileStorage,
	})
	require.NoError(t, err, "create stream %s", name)

	t.Cleanup(func() {
		if err := stream.DeleteStream(context.Background(), name); err != nil {
			t.Logf("deleting stream %s: %v", name, err)
		}

		connection.Close()
	})
}

// newTestReader builds a reader over the test domain and closes it on cleanup.
func newTestReader(t *testing.T, config Config, consumer string, shards []uint32) *Reader {
	t.Helper()

	reader, err := NewReader(context.Background(), &ReaderConfig{
		Config:    config,
		Domain:    _testDomain,
		Consumer:  consumer,
		Shards:    shards,
		MaxWait:   _testMaxWait,
		AckWait:   _testAckWait,
		BatchSize: 10,
	})
	require.NoError(t, err, "reader %s", consumer)

	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Logf("closing reader %s: %v", consumer, err)
		}
	})

	return reader
}

// poll reads until at least one record arrives, within the poll budget. It
// stops at the first non-empty batch on purpose: a test that keeps pulling
// without acknowledging spends the consumer's delivery budget (MaxDeliver) in
// milliseconds, which hides the very thing it is trying to observe.
func poll(ctx context.Context, t *testing.T, reader *Reader) eventlog.Batch {
	t.Helper()

	deadline := time.Now().Add(_testPollBudget)

	for time.Now().Before(deadline) {
		batch, err := reader.Poll(ctx)
		require.NoError(t, err, "poll")

		if len(batch.Records()) > 0 {
			return batch
		}
	}

	require.FailNow(t, "no record arrived within the poll budget")

	return nil
}

// requireNoRecords asserts that nothing arrives for a window wide enough to
// have caught a redelivery, so the assertion means something.
func requireNoRecords(ctx context.Context, t *testing.T, reader *Reader, window time.Duration) {
	t.Helper()

	deadline := time.Now().Add(window)

	for time.Now().Before(deadline) {
		batch, err := reader.Poll(ctx)
		require.NoError(t, err, "poll")
		require.Empty(t, batch.Records(), "expected no records, got %d", len(batch.Records()))
	}
}

// publish stores one fact for an entity.
func publish(ctx context.Context, t *testing.T, publisher *Publisher, key, id string, index int) {
	t.Helper()

	require.NoError(t, publisher.Publish(ctx, &eventlog.Record{
		Domain:  _testDomain,
		Key:     key,
		ID:      id,
		Value:   fmt.Appendf(nil, `{"entity":%q,"index":%d}`, key, index),
		Headers: map[string]string{"event_type": "agentos.run.updated"},
	}), "publish %s/%d", id, index)
}

func TestNATSPublishAndConsume(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget*3)
	defer cancel()

	config := testConfig(t)
	createStream(t, config)

	publisher, err := NewPublisher(config)
	require.NoError(t, err, "publisher")

	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Logf("closing publisher: %v", err)
		}
	})

	publish(ctx, t, publisher, "run-abc", "evt-1", 1)

	reader := newTestReader(t, config, "roundtrip", nil)
	batch := poll(ctx, t, reader)
	records := batch.Records()
	require.Len(t, records, 1, "one record")

	require.Equal(t, _testDomain, records[0].Domain, "the domain comes back")
	require.Equal(t, "run-abc", records[0].Key, "the entity key travels in a header")
	require.Equal(t, "agentos.run.updated", records[0].Headers["event_type"], "publisher headers survive")
	require.NotContains(t, records[0].Headers, "Nats-Stream", "transport headers are not the consumer's business")
	require.Positive(t, records[0].Sequence, "the stream assigns a sequence")
	require.False(t, records[0].Timestamp.IsZero(), "and a timestamp")
	require.JSONEq(t, `{"entity":"run-abc","index":1}`, string(records[0].Value), "the payload is untouched")

	require.NoError(t, batch.Ack(ctx), "ack")

	// The window covers the ack wait: a record that was not really
	// acknowledged would have been delivered again by now.
	requireNoRecords(ctx, t, reader, _testAckWait+_testMaxWait)
}

// TestNATSDeduplicatesByRecordID locks the property the drainer leans on: a
// re-publish of the same fact, with the same record id, must not duplicate it.
// This is what a crash between "published" and "outbox row deleted" produces.
func TestNATSDeduplicatesByRecordID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget*3)
	defer cancel()

	config := testConfig(t)
	createStream(t, config)

	publisher, err := NewPublisher(config)
	require.NoError(t, err, "publisher")

	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Logf("closing publisher: %v", err)
		}
	})

	publish(ctx, t, publisher, "run-abc", "evt-1", 1)
	publish(ctx, t, publisher, "run-abc", "evt-1", 1)
	publish(ctx, t, publisher, "run-abc", "evt-2", 2)

	batch := poll(ctx, t, newTestReader(t, config, "dedupe", nil))

	require.Len(t, batch.Records(), 2, "the duplicate is collapsed, the distinct record is kept")
	require.NoError(t, batch.Ack(ctx), "ack")
}

// TestNATSRedeliversUnacknowledgedRecords locks the ack boundary: the log
// promises at-least-once, so a record that was never acknowledged comes back.
// It is also why every consumer deduplicates on the envelope.
func TestNATSRedeliversUnacknowledgedRecords(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget*3)
	defer cancel()

	config := testConfig(t)
	createStream(t, config)

	publisher, err := NewPublisher(config)
	require.NoError(t, err, "publisher")

	publish(ctx, t, publisher, "run-abc", "evt-1", 1)

	require.NoError(t, publisher.Close(), "close the publisher before reading")

	first := newTestReader(t, config, "redelivery", nil)
	delivered := poll(ctx, t, first)
	require.Len(t, delivered.Records(), 1, "delivered once")

	require.NoError(t, first.Close(), "close without acknowledging")

	second := newTestReader(t, config, "redelivery", nil)
	again := poll(ctx, t, second)

	require.Len(t, again.Records(), 1, "the record is delivered again")
	require.Equal(t, delivered.Records()[0].Sequence, again.Records()[0].Sequence, "it is the same record")
	require.NoError(t, again.Ack(ctx), "acknowledging it settles the consumer")
}

// TestNATSShardsKeepEntityOrder is the reason the shard is part of the subject:
// two entities on different shards are read by two consumers, each of which
// sees its own entity's records in publish order.
func TestNATSShardsKeepEntityOrder(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget*3)
	defer cancel()

	config := testConfig(t)
	createStream(t, config)

	publisher, err := NewPublisher(config)
	require.NoError(t, err, "publisher")

	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Logf("closing publisher: %v", err)
		}
	})

	first, second := keysOnDifferentShards(t, config)

	for index := 1; index <= 3; index++ {
		publish(ctx, t, publisher, first, fmt.Sprintf("first-%d", index), index)
		publish(ctx, t, publisher, second, fmt.Sprintf("second-%d", index), index)
	}

	firstShard := newTestReader(t, config, "shard-first", []uint32{config.shardFor(first)})
	secondShard := newTestReader(t, config, "shard-second", []uint32{config.shardFor(second)})

	firstBatch := poll(ctx, t, firstShard)
	secondBatch := poll(ctx, t, secondShard)

	require.Equal(t, []string{first, first, first}, keysOf(firstBatch.Records()),
		"one entity, one shard, publish order")
	require.Equal(t, []string{second, second, second}, keysOf(secondBatch.Records()),
		"and the other entity on its own shard")

	require.NoError(t, firstBatch.Ack(ctx), "ack the first shard")
	require.NoError(t, secondBatch.Ack(ctx), "ack the second shard")
}

// TestNATSPublishWithoutStreamFailsLoudly locks the operational rule that
// streams are provisioned, not created by the first writer: publishing into a
// domain nobody provisioned must be an error, never a silent drop.
func TestNATSPublishWithoutStreamFailsLoudly(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), _testPollBudget)
	defer cancel()

	config := testConfig(t)

	publisher, err := NewPublisher(config)
	require.NoError(t, err, "publisher")

	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Logf("closing publisher: %v", err)
		}
	})

	err = publisher.Publish(ctx, &eventlog.Record{
		Domain: _testDomain,
		Key:    "run-abc",
		Value:  []byte(`{"entity":"run-abc"}`),
	})
	require.Error(t, err, "publishing to an unprovisioned domain must fail")
}

// keysOnDifferentShards returns two entity keys that the configured shard
// count places on different shards.
func keysOnDifferentShards(t *testing.T, config Config) (first, second string) {
	t.Helper()

	first = "run-abc"
	firstShard := config.shardFor(first)

	for suffix := range 100 {
		second = "run-xyz-" + strconv.Itoa(suffix)

		if config.shardFor(second) != firstShard {
			return first, second
		}
	}

	require.FailNow(t, "could not find two keys on different shards")

	return "", ""
}

func keysOf(records []eventlog.ConsumedRecord) []string {
	keys := make([]string, 0, len(records))

	for _, record := range records {
		keys = append(keys, record.Key)
	}

	return keys
}
