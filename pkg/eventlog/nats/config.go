// Package nats implements the AgentOS event backbone on NATS JetStream.
//
// This package is the only place that knows the backbone is NATS: everything
// else talks to pkg/eventlog, so the naming, sharding and deduplication rules
// stay behind the port.
//
// # Naming
//
// A record of domain D, shard S and entity E is published on
// <prefix>.D.S.E, and every shard of a domain lives in one stream named after
// the domain. The dead-letter domains share a single stream, because they are
// one operational concern rather than one per domain.
//
// The entity id is the subject's last segment on purpose: the subject then
// names the very thing the log orders — one entity's facts — which makes a
// subject self-describing in tooling and lets any consumer inspect or replay
// a single entity without decoding anything.
//
// # Sharding
//
// JetStream distributes the deliveries of one consumer across its workers
// without preserving order, so "ordered per entity" cannot come from the
// consumer side. It comes from the subject instead: every record of an entity
// carries the same shard, so a consumer owning that shard sees the entity's
// records in order.
//
// One consumer may own every shard — the default, and always correct. Owning
// fewer shards per consumer is what buys parallelism, at the cost of one
// sequential loop per consumer.
//
// Streams are provisioned, never auto-created: scripts/nats/create-streams.sh
// creates them with their retention and duplicate window, because a stream's
// limits are the contract every consumer relies on and must not be decided by
// the first writer.
package nats

import (
	"strconv"
	"strings"
	"time"
)

// DefaultShards is how many shards a domain is split into unless the caller
// chooses otherwise. It is a per-deployment decision: changing it moves
// existing entities to other shards, so it is safe to change only while no
// writer is active.
const DefaultShards = 16

const (
	_defaultSubjectPrefix  = "agentos"
	_defaultConnectTimeout = 5 * time.Second

	_deadLetterDomain  = "dlq."
	_deadLetterStream  = "dlq"
	_separator         = "."
	_shardFilterSuffix = ".>"

	// _fnvOffset32 and _fnvPrime32 are the FNV-1a parameters.
	_fnvOffset32 = 2166136261
	_fnvPrime32  = 16777619
)

// Config describes how to reach the backbone and how it names things.
type Config struct {
	// URL is the NATS server to connect to. Empty disables the backbone: the
	// service then runs without a fact log, which is a supported deployment.
	URL string

	// SubjectPrefix namespaces every subject and stream, so two deployments
	// can share one NATS cluster.
	SubjectPrefix string

	// Shards is how many shards each domain is split into.
	Shards uint32

	// ClientName identifies this process in the server's connection list.
	ClientName string

	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
}

// ConfigError reports a configuration the backbone cannot run with.
type ConfigError struct {
	Field  string
	Reason string
}

// Error implements error.
func (e *ConfigError) Error() string {
	return "eventlog/nats: " + e.Field + ": " + e.Reason
}

// Enabled reports whether a backbone is configured at all. The zero value
// means "no fact log", which callers check before connecting.
func (c Config) Enabled() bool {
	return c.URL != ""
}

// normalized applies the defaults and rejects a configuration that cannot
// work. Everything past this point handles only a normalized value.
func (c Config) normalized() (Config, error) {
	if c.URL == "" {
		return Config{}, &ConfigError{Field: "URL", Reason: "no server to connect to"}
	}

	if c.SubjectPrefix == "" {
		c.SubjectPrefix = _defaultSubjectPrefix
	}

	if c.Shards == 0 {
		c.Shards = DefaultShards
	}

	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = _defaultConnectTimeout
	}

	return c, nil
}

// streamFor is the stream holding a domain. Stream names cannot carry the dot
// that separates the parts of a domain, and every dead-letter domain shares
// one stream.
func (c Config) streamFor(domain string) string {
	stream := strings.ReplaceAll(domain, _separator, "_")

	if strings.HasPrefix(domain, _deadLetterDomain) {
		stream = _deadLetterStream
	}

	return strings.ToUpper(c.SubjectPrefix + "_" + stream)
}

// subjectFor is where a record of a domain and entity is published:
// <prefix>.<domain>.<shard>.<entity_id>. The shard is derived from the entity
// key here, so a caller cannot publish an entity onto a shard it does not
// hash to — the subject and the shard always agree.
func (c Config) subjectFor(domain, key string) string {
	shard := strconv.FormatUint(uint64(c.shardFor(key)), 10)

	return c.SubjectPrefix + _separator + domain + _separator + shard + _separator + key
}

// subjectsFor is the filter a consumer owning those shards subscribes with:
// every entity of a shard matches, because ordering is per entity and the
// consumer reads the whole shard.
func (c Config) subjectsFor(domain string, shards []uint32) []string {
	subjects := make([]string, 0, len(shards))

	for _, shard := range shards {
		subjects = append(subjects, c.SubjectPrefix+_separator+domain+_separator+
			strconv.FormatUint(uint64(shard), 10)+_shardFilterSuffix)
	}

	return subjects
}

// shardFor maps an entity key to its shard. FNV-1a is computed here instead of
// with hash/fnv because hash.Hash.Write returns an error that cannot happen,
// and ignoring a returned error needs either a lint exception or dead code.
//
// The hash must stay stable across processes and dependency upgrades: every
// writer has to agree on where an entity lives, or ordering breaks silently.
func (c Config) shardFor(key string) uint32 {
	hash := uint32(_fnvOffset32)

	for index := 0; index < len(key); index++ {
		hash ^= uint32(key[index])
		hash *= _fnvPrime32
	}

	return hash % c.Shards
}

// allShards is every shard of the domain: what a single consumer owns.
func (c Config) allShards() []uint32 {
	shards := make([]uint32, 0, c.Shards)

	for shard := uint32(0); shard < c.Shards; shard++ {
		shards = append(shards, shard)
	}

	return shards
}
