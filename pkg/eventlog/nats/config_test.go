package nats

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestConfigNormalizedDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	normalized, err := Config{URL: "nats://nats:4222"}.normalized()
	require.NoError(t, err, "a bare URL is a complete configuration")
	require.Equal(t, _defaultSubjectPrefix, normalized.SubjectPrefix, "subject prefix")
	require.Equal(t, uint32(DefaultShards), normalized.Shards, "shard count")
	require.Equal(t, _defaultConnectTimeout, normalized.ConnectTimeout, "connect timeout")
	require.True(t, normalized.Enabled(), "a configured backbone is enabled")

	_, err = Config{}.normalized()
	require.Error(t, err, "a missing URL cannot be normalized")

	var configErr *ConfigError
	require.True(t, errors.As(err, &configErr), "the failure is a ConfigError")
	require.Equal(t, "URL", configErr.Field, "the failing field is named")
}

func TestConfigEnabled(t *testing.T) {
	t.Parallel()

	require.False(t, Config{}.Enabled(), "the zero value means no backbone")
	require.True(t, Config{URL: "nats://nats:4222"}.Enabled(), "a URL enables it")
}

// TestConfigStreamForMatchesProvisioning locks the stream names against the
// ones scripts/nats/create-streams.sh creates: a mismatch would make the app
// publish into a stream nobody provisioned, and the publish would fail at
// runtime instead of here.
func TestConfigStreamForMatchesProvisioning(t *testing.T) {
	t.Parallel()

	config := Config{URL: "nats://nats:4222", SubjectPrefix: "agentos"}

	expected := map[string]string{
		"run.timeline":        "AGENTOS_RUN_TIMELINE",
		"plan.events":         "AGENTOS_PLAN_EVENTS",
		"conversation.events": "AGENTOS_CONVERSATION_EVENTS",
		"messages":            "AGENTOS_MESSAGES",
		"commands":            "AGENTOS_COMMANDS",
	}

	for domain, stream := range expected {
		require.Equal(t, stream, config.streamFor(domain), "stream for %s", domain)
	}

	require.Equal(t, "AGENTOS_DLQ", config.streamFor("dlq.run.timeline"),
		"every dead-letter domain shares one stream")
}

func TestConfigSubjectFor(t *testing.T) {
	t.Parallel()

	config := Config{URL: "nats://nats:4222", SubjectPrefix: "agentos", Shards: 16}

	// 11 is run-abc's pinned shard (see TestConfigShardForIsStable): the
	// subject carries both the entity's shard and the entity itself.
	require.Equal(t, "agentos.run.timeline.11.run-abc", config.subjectFor("run.timeline", "run-abc"),
		"subject shape")
	require.Equal(t,
		[]string{"agentos.messages.0.>", "agentos.messages.15.>"},
		config.subjectsFor("messages", []uint32{0, 15}),
		"a consumer filters by shard, every entity in it")
}

// TestConfigShardForIsStable pins the hash. The values are computed
// independently (FNV-1a 32, modulo the shard count) because every writer has
// to agree on where an entity lives: a dependency upgrade, or a "nicer" hash,
// would move entities and break ordering without failing anything.
func TestConfigShardForIsStable(t *testing.T) {
	t.Parallel()

	config := Config{URL: "nats://nats:4222", Shards: 16}

	require.Equal(t, uint32(11), config.shardFor("run-abc"), "shard of run-abc")
	require.Equal(t, uint32(3), config.shardFor("verify-thread"), "shard of verify-thread")
	require.Equal(t, uint32(5), config.shardFor(""), "an absent key still has a shard")

	require.Equal(t, config.shardFor("run-abc"), config.shardFor("run-abc"),
		"the same key always lands on the same shard")
}

func TestConfigShardForUsesConfiguredCount(t *testing.T) {
	t.Parallel()

	seven := Config{URL: "nats://nats:4222", Shards: 7}

	for index := range 500 {
		key := strings.Repeat("k", index%13) + string(rune('a'+index%26))
		require.Less(t, seven.shardFor(key), uint32(7), "shard of %q", key)
	}

	// 1738791275 is the FNV-1a 32 of "run-abc"; modulo 7 it is a different
	// shard than modulo 16, which is what makes the shard count a real choice.
	require.Equal(t, uint32(1738791275%7), seven.shardFor("run-abc"), "modulo the configured count")
	require.NotEqual(t, uint32(11), seven.shardFor("run-abc"), "not the default-count shard")
}

func TestConfigAllShards(t *testing.T) {
	t.Parallel()

	require.Equal(t, []uint32{0, 1, 2}, Config{Shards: 3}.allShards(), "every shard, in order")
}

func TestReaderConfigNormalizedDefaults(t *testing.T) {
	t.Parallel()

	normalized, err := (&ReaderConfig{
		Config:   Config{URL: "nats://nats:4222"},
		Domain:   "run.timeline",
		Consumer: "projection",
	}).normalized()
	require.NoError(t, err, "a domain and a consumer are enough")
	require.Equal(t, _defaultBatchSize, normalized.BatchSize, "batch size")
	require.Equal(t, _defaultMaxWait, normalized.MaxWait, "max wait")
	require.Equal(t, _defaultAckWait, normalized.AckWait, "ack wait")
	require.Equal(t, _defaultMaxDeliver, normalized.MaxDeliver, "max deliver")
	require.True(t, normalized.StartTime.IsZero(), "no replay point by default")
}

func TestReaderConfigNormalizedRequiresDomainAndConsumer(t *testing.T) {
	t.Parallel()

	config := Config{URL: "nats://nats:4222"}

	_, err := (&ReaderConfig{Config: config, Consumer: "projection"}).normalized()
	require.Error(t, err, "a consumer without a domain is rejected")

	_, err = (&ReaderConfig{Config: config, Domain: "run.timeline"}).normalized()
	require.Error(t, err, "a domain without a consumer name is rejected")
}

// TestReaderConfigDeliveryPlan locks the replay rule: a consumer that does not
// exist yet starts from the oldest retained record, so a new projection cannot
// silently skip history. Choosing a start time is the explicit opt-in.
func TestReaderConfigDeliveryPlan(t *testing.T) {
	t.Parallel()

	config := ReaderConfig{Config: Config{URL: "nats://nats:4222", Shards: 4}}

	require.Equal(t, jetstream.DeliverAllPolicy, config.deliverPolicy(),
		"an unset start time replays everything retained")
	require.Nil(t, config.optStartTime(), "and carries no start time")

	config.StartTime = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

	require.Equal(t, jetstream.DeliverByStartTimePolicy, config.deliverPolicy(),
		"a start time selects the time-based policy")
	require.Equal(t, config.StartTime, *config.optStartTime(), "and is passed through")

	require.Equal(t, config.allShards(), config.shards(), "an unset shard list is every shard")

	config.Shards = []uint32{2}

	require.Equal(t, []uint32{2}, config.shards(), "an explicit shard list wins")
}

// TestConsumerHeadersDropsTransportHeaders keeps the port's promise: consumers
// see what the publisher wrote, not how the transport carried it.
func TestConsumerHeadersDropsTransportHeaders(t *testing.T) {
	t.Parallel()

	headers := consumerHeaders(map[string][]string{
		"event_type":      {"agentos.run.completed"},
		"entity_key":      {"run-abc"},
		"Nats-Msg-Id":     {"evt-1"},
		"Nats-Stream":     {"AGENTOS_RUN_TIMELINE"},
		"without-values":  {},
		"with-many-value": {"first", "second"},
	})

	require.Equal(t, map[string]string{
		"event_type":      "agentos.run.completed",
		"entity_key":      "run-abc",
		"with-many-value": "first",
	}, headers, "internal headers and empty values are dropped, the first value wins")
}
