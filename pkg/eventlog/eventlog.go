// Package eventlog is the transport-neutral contract for the AgentOS event
// backbone: a durable, ordered, replayable log of facts.
//
// It exists so no AgentOS package names a vendor. The domain packages — the
// outbox drainer, the projections, the Temporal ingress router — depend on
// this port, and a concrete transport lives in a subpackage (nats). That is
// the same rule that keeps Temporal out of agentos/core|control|process: a
// vendor's semantics, its naming and its failure modes must not shape the
// contract.
//
// What the port promises, and what it does not:
//
//   - Delivery is at-least-once. Nothing here implies exactly-once: a
//     publisher that crashes after the log accepted a record will publish it
//     again, so consumers deduplicate on (entity, sequence).
//   - Records of one entity keep their order. Ordering across entities is not
//     promised, because the log parallelizes by entity.
//   - The log retains records for a time window independent of consumer
//     progress, so a consumer that was offline (or newly added) can replay
//     within that window. A transport whose retention is bounded by capacity
//     instead would silently truncate a lagging consumer, and does not
//     implement this port.
package eventlog

import (
	"context"
	"errors"
	"time"
)

// ErrLogUnavailable reports that the log itself could not be reached — the
// transport is down, not the record. Publishers wrap their connectivity
// failures with it, so a drainer can keep retrying an outage without spending
// its dead-letter budget on records that were never at fault.
var ErrLogUnavailable = errors.New("eventlog: log unavailable")

// Domains are the logical channels of the fact log. The value is a contract
// between the writer and every consumer: it is a logical name, not a subject
// or a topic, and an adapter maps it to whatever its transport calls things.
//
// A new domain is a new contract, so it is added here deliberately rather
// than invented at a call site.
const (
	DomainRunTimeline        = "run.timeline"
	DomainPlanEvents         = "plan.events"
	DomainConversationEvents = "conversation.events"
	DomainCommands           = "commands"
)

// AgentOSDomain reports whether domain names one of the domains above. The
// dead-letter domains it produces (see DeadLetterDomain) are deliberately not
// listed: they are derived, not declared.
func AgentOSDomain(domain string) bool {
	switch domain {
	case DomainRunTimeline, DomainPlanEvents, DomainConversationEvents,
		DomainCommands:
		return true
	default:
		return false
	}
}

// DeadLetterDomain is where a record of domain goes once publishing it has
// failed too many times. It is a plain transformation rather than an adapter
// method so the drainer can name the destination without knowing which
// transport is in use.
func DeadLetterDomain(domain string) string {
	return "dlq." + domain
}

// HeaderEntityKey carries the entity key alongside the payload. Consumers use
// it for routing and for idempotency without decoding the payload, which is
// what lets the ingress router map a fact to a workflow without knowing the
// fact's schema.
const HeaderEntityKey = "entity_key"

// Record is a fact on its way to the log.
type Record struct {
	// Domain is one of the domains above (or a dead-letter domain).
	Domain string

	// Key is the entity key — run id, plan id, thread id. It selects the
	// shard that carries the entity, so every record of one entity is
	// ordered behind the same shard, and it is the last segment of the
	// subject an entity's records are published on. It must therefore be a
	// non-empty string that is safe as one subject token.
	Key string

	// ID is an optional deduplication token. Transports that can deduplicate
	// in the broker do so on this value, which is why the drainer passes
	// envelope.event_id: a re-publish after a crash then collapses instead of
	// duplicating. The window is bounded, so consumers stay idempotent.
	ID string

	// Value is the encoded Envelope. Publishing anything else breaks every
	// reader that is not the record's author.
	Value []byte

	// Headers are transport-neutral string headers. They are copied as-is,
	// with HeaderEntityKey added from Key.
	Headers map[string]string
}

// ConsumedRecord is a fact read back from the log.
type ConsumedRecord struct {
	// Domain is the logical domain the record was published to.
	Domain string

	// Sequence is the log's own position for the record — monotonic within
	// the domain. It is not an identity: replaying or copying a log changes
	// it. Idempotency keys come from the envelope, never from here.
	Sequence uint64

	// Timestamp is when the log stored the record.
	Timestamp time.Time

	// Key is the entity key carried by the publisher.
	Key string

	Value   []byte
	Headers map[string]string
}

// Batch is a set of records handed to a consumer, which acknowledges them as
// a unit once they are durable on the consumer's side.
type Batch interface {
	Records() []ConsumedRecord

	// Ack tells the log the records of this batch are processed and need not
	// be delivered again. A consumer calls it only after its own write
	// committed; acknowledging first turns a crash into data loss.
	Ack(ctx context.Context) error
}

// Reader hands out batches for a fixed set of domains.
type Reader interface {
	// Poll returns the next batch, blocking until one is available, the
	// context is canceled, or the transport gives up waiting. An empty batch
	// with a nil error is not an error condition: a poll loop simply
	// continues.
	Poll(ctx context.Context) (Batch, error)
}

// Publisher puts records on the log and returns only once the log has
// accepted them, because a drainer deletes its outbox row on that basis.
//
// A Publisher that cannot reach the log reports the failure wrapped with
// ErrLogUnavailable; every other failure is the record's or the deployment's,
// and its caller's to keep or discard.
type Publisher interface {
	Publish(ctx context.Context, record *Record) error
}
