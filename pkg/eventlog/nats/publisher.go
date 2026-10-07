package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrRecordKeyInvalid reports a record whose key cannot be one subject token.
// The key is the subject's last segment, so a key carrying a separator or a
// wildcard would silently publish the entity onto the wrong subjects — or onto
// everyone's. Refusing it turns a producer's bug into a publish error.
var ErrRecordKeyInvalid = errors.New("eventlog/nats: record key must be one non-empty subject token (no '.', '*', '>' or space)")

// Publisher writes facts to JetStream. Publish returns only once the stream
// has stored the record, which is what lets the outbox drainer delete its row
// on the strength of that return.
//
// A Publisher is safe for concurrent use.
type Publisher struct {
	connection *nats.Conn
	stream     jetstream.JetStream
	config     Config
}

// NewPublisher connects to the backbone. The caller owns the result and must
// Close it.
//
// The connection is allowed to outlive a broker that is not there yet: the
// log is a derived plane — an unreachable broker degrades delivery, it does
// not stop the process — so connecting retries in the background and a
// Publish that lands before the connection does fails with
// eventlog.ErrLogUnavailable for the drainer to back off on.
func NewPublisher(config Config) (*Publisher, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}

	connection, err := nats.Connect(normalized.URL,
		nats.Name(normalized.ClientName),
		nats.Timeout(normalized.ConnectTimeout),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("eventlog/nats: connect to %s: %w", normalized.URL, err)
	}

	stream, err := jetstream.New(connection)
	if err != nil {
		connection.Close()

		return nil, fmt.Errorf("eventlog/nats: jetstream context: %w", err)
	}

	return &Publisher{connection: connection, stream: stream, config: normalized}, nil
}

// Publish stores the record and waits for the server's acknowledgement.
//
// The entity key picks the shard and closes the subject, and the record id
// becomes the duplicate window key, so re-publishing the same fact after a
// crash collapses instead of duplicating. That window is bounded in time, so
// consumers still deduplicate on the envelope.
func (p *Publisher) Publish(ctx context.Context, record *eventlog.Record) error {
	if !validSubjectToken(record.Key) {
		return ErrRecordKeyInvalid
	}

	message := p.message(record)

	if _, err := p.stream.PublishMsg(ctx, message); err != nil {
		if p.connection.Status() != nats.CONNECTED {
			// The log is unreachable, not the record: the caller must be able
			// to tell an outage from a poison record, because only the second
			// one may ever reach a dead letter.
			return fmt.Errorf("eventlog/nats: publish to %s: %w: %w",
				message.Subject, eventlog.ErrLogUnavailable, err)
		}

		return fmt.Errorf("eventlog/nats: publish to %s: %w", message.Subject, err)
	}

	return nil
}

// Close drains the connection, publishing what is buffered before it goes.
func (p *Publisher) Close() error {
	if p.connection == nil {
		return nil
	}

	if err := p.connection.Drain(); err != nil {
		return fmt.Errorf("eventlog/nats: close publisher: %w", err)
	}

	return nil
}

// message turns a port-level record into a JetStream message: the shard and
// the entity go into the subject, the entity key into a header (so a consumer
// can route without decoding the payload), and the record id into the
// duplicate window.
func (p *Publisher) message(record *eventlog.Record) *nats.Msg {
	headers := nats.Header{}

	for name, value := range record.Headers {
		headers.Set(name, value)
	}

	headers.Set(eventlog.HeaderEntityKey, record.Key)

	if record.ID != "" {
		headers.Set(jetstream.MsgIDHeader, record.ID)
	}

	return &nats.Msg{
		Subject: p.config.subjectFor(record.Domain, record.Key),
		Header:  headers,
		Data:    record.Value,
	}
}

// validSubjectToken reports whether a key is safe as the last segment of a
// subject: non-empty, and free of the separator and the wildcards, which
// would otherwise reshape or widen what the subject matches.
func validSubjectToken(key string) bool {
	if key == "" {
		return false
	}

	return !strings.ContainsAny(key, ".*> ")
}
