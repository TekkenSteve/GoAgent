package nats

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrStreamNotFound reports a consumer whose stream has not been provisioned.
// It is deliberately separate from every other failure: a host can wait for the
// stream to appear (an operator action, or a one-shot provisioning job) instead
// of crash-looping, while a wrong URL or a rejected configuration still fails
// immediately.
var ErrStreamNotFound = errors.New("eventlog/nats: stream is not provisioned")

const (
	_defaultBatchSize  = 16
	_defaultMaxWait    = time.Second
	_defaultAckWait    = 30 * time.Second
	_defaultMaxDeliver = 8

	// _internalHeaderPrefix marks the headers the server adds. They describe
	// the transport, not the fact, so they are not handed to consumers.
	_internalHeaderPrefix = "Nats-"
)

// ReaderConfig describes one consumer of the backbone.
type ReaderConfig struct {
	Config

	// Domain is the domain this consumer reads.
	Domain string

	// Consumer is the durable consumer name, which is the identity of the
	// consumer's progress: reusing it resumes where it stopped, while
	// changing it starts over from the delivery policy below.
	Consumer string

	// Shards are the shards this consumer owns. Empty means all of them,
	// which is always correct and is where every deployment starts.
	Shards []uint32

	// BatchSize is how many records a poll asks for, and MaxWait how long it
	// waits for them. A full batch returns immediately, so MaxWait is only
	// reached when traffic is light — which makes it the latency floor of a
	// quiet consumer as much as a batching knob. Raising BatchSize without
	// lowering MaxWait trades latency for throughput.
	BatchSize  int
	MaxWait    time.Duration
	AckWait    time.Duration
	MaxDeliver int

	// StartTime replays from a point in time, and only decides where a
	// consumer that does not exist yet begins. Zero starts from the oldest
	// record still retained, so a projection never skips facts silently.
	StartTime time.Time

	// OnPoison decides what happens to a record the consumer has failed
	// MaxDeliver-1 times. The delivery that reaches MaxDeliver is spent parking
	// the record, so a parked record is never handed to the consumer again.
	OnPoison PoisonPolicy

	// Logger is optional; nil keeps the parking diagnostics silent.
	Logger logger.Interface
}

// Reader reads one domain from the backbone.
//
// A Reader is deliberately not safe for concurrent use. Ordering per entity is
// a property of pulling a shard's batches and processing them in sequence, so
// one Reader belongs to one loop; more throughput means more Readers, each
// owning a disjoint set of shards.
type Reader struct {
	connection *nats.Conn
	consumer   jetstream.Consumer
	js         jetstream.JetStream
	config     ReaderConfig
}

// NewReader connects and creates the consumer, or resumes it if it exists.
func NewReader(ctx context.Context, config *ReaderConfig) (*Reader, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}

	connection, err := nats.Connect(normalized.URL,
		nats.Name(normalized.ClientName),
		nats.Timeout(normalized.ConnectTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("eventlog/nats: connect to %s: %w", normalized.URL, err)
	}

	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()

		return nil, fmt.Errorf("eventlog/nats: jetstream context: %w", err)
	}

	consumer, err := normalized.ensureConsumer(ctx, js)
	if err != nil {
		connection.Close()

		return nil, err
	}

	return &Reader{connection: connection, consumer: consumer, js: js, config: normalized}, nil
}

// Poll returns the next batch of the domain. An empty batch with no error
// means the wait elapsed without a record, which is not a failure.
func (r *Reader) Poll(ctx context.Context) (eventlog.Batch, error) {
	// The wait is expressed as a context deadline so a canceled caller stops the
	// fetch immediately instead of waiting it out.
	fetchCtx, cancel := context.WithTimeout(ctx, r.config.MaxWait)
	defer cancel()

	fetched, err := r.consumer.Fetch(r.config.BatchSize, jetstream.FetchContext(fetchCtx))
	if err != nil {
		// A quiet poll is an empty batch, never a nil one: a nil batch with a
		// nil error would panic the first caller that trusts the contract.
		if pollErr := r.pollError(err); pollErr != nil {
			return nil, pollErr
		}

		return emptyBatch{}, nil
	}

	records := make([]eventlog.ConsumedRecord, 0, r.config.BatchSize)
	messages := make([]jetstream.Msg, 0, r.config.BatchSize)

	for message := range fetched.Messages() {
		metadata, err := message.Metadata()
		if err != nil {
			// Without metadata the record has no sequence, so it cannot be
			// applied idempotently. Leaving it unacknowledged makes the server
			// redeliver it rather than losing it.
			return nil, fmt.Errorf("eventlog/nats: metadata of %s: %w", message.Subject(), err)
		}

		parked, parkErr := r.parkIfExhausted(ctx, message, metadata.NumDelivered)
		if parkErr != nil {
			return nil, parkErr
		}

		if parked {
			continue
		}

		messages = append(messages, message)
		records = append(records, eventlog.ConsumedRecord{
			Domain:    r.config.Domain,
			Sequence:  metadata.Sequence.Stream,
			Timestamp: metadata.Timestamp,
			Key:       message.Headers().Get(eventlog.HeaderEntityKey),
			Value:     message.Data(),
			Headers:   consumerHeaders(message.Headers()),
		})
	}

	// A timeout here only means the wait ended, so records fetched before it
	// still belong to the caller.
	if err := fetched.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		return nil, r.pollError(err)
	}

	if len(records) == 0 {
		return emptyBatch{}, nil
	}

	return &consumerBatch{messages: messages, records: records}, nil
}

// parkIfExhausted moves a record whose delivery budget is spent to its domain's
// dead-letter stream and acknowledges it.
//
// The delivery that reaches MaxDeliver is the one spent parking, so a parked
// record is never handed to the consumer again: MaxDeliver counts deliveries,
// and one of them is the park. A record whose entity key is missing is handed
// over instead — parking needs the shard the entity hashes to, and inventing
// one would file the record where no consumer of that domain looks.
//
// A park that cannot be published is an error, not a skip: the record stays
// unacknowledged and comes back, which is how a dead-letter stream that does
// not exist yet surfaces instead of silently swallowing records.
func (r *Reader) parkIfExhausted(ctx context.Context, message jetstream.Msg, deliveries uint64) (bool, error) {
	if r.config.OnPoison != PoisonDeadLetter ||
		deliveries < uint64(r.config.MaxDeliver) { //nolint:gosec // MaxDeliver is normalized to a positive int, so this cannot wrap
		return false, nil
	}

	key := message.Headers().Get(eventlog.HeaderEntityKey)
	if key == "" {
		r.warnf("eventlog/nats: cannot park %s: no %s header", message.Subject(), eventlog.HeaderEntityKey)

		return false, nil
	}

	subject := r.config.subjectFor(eventlog.DeadLetterDomain(r.config.Domain), key)

	headers := nats.Header{}
	maps.Copy(headers, message.Headers())

	headers.Set(HeaderPoisonSource, message.Subject())
	headers.Set(HeaderPoisonDeliveries, strconv.FormatUint(deliveries, 10))

	var options []jetstream.PublishOpt
	if msgID := message.Headers().Get(jetstream.MsgIDHeader); msgID != "" {
		options = append(options, jetstream.WithMsgID(msgID))
	}

	if _, err := r.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Header: headers, Data: message.Data()}, options...); err != nil {
		return false, fmt.Errorf("eventlog/nats: park %s in %s: %w", message.Subject(), subject, err)
	}

	if err := message.Ack(); err != nil {
		return false, fmt.Errorf("eventlog/nats: ack parked %s: %w", message.Subject(), err)
	}

	r.warnf("eventlog/nats: parked %s after %d deliveries in %s", message.Subject(), deliveries, subject)

	return true, nil
}

// warnf reports a parking diagnostic when the caller supplied a logger.
func (r *Reader) warnf(format string, args ...any) {
	if r.config.Logger != nil {
		r.config.Logger.Warn(format, args...)
	}
}

// Close releases the connection. Records already fetched but not acknowledged
// are redelivered to whoever owns the consumer next.
func (r *Reader) Close() error {
	if r.connection == nil {
		return nil
	}

	if err := r.connection.Drain(); err != nil {
		return fmt.Errorf("eventlog/nats: close reader: %w", err)
	}

	return nil
}

// pollError turns "nothing arrived" into an empty poll, and keeps everything
// else an error so a broken consumer is visible instead of silent. The fetch
// deadline is this package's own, so its expiry is the quiet case too.
func (r *Reader) pollError(err error) error {
	if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	return fmt.Errorf("eventlog/nats: fetch %s: %w", r.config.Domain, err)
}

// normalized applies the defaults and rejects a configuration that cannot
// consume anything.
func (c *ReaderConfig) normalized() (ReaderConfig, error) {
	config, err := c.Config.normalized()
	if err != nil {
		return ReaderConfig{}, err
	}

	if c.Domain == "" {
		return ReaderConfig{}, &ConfigError{Field: "Domain", Reason: "no domain to read"}
	}

	if c.Consumer == "" {
		return ReaderConfig{}, &ConfigError{Field: "Consumer", Reason: "a durable name is required"}
	}

	normalized := *c
	normalized.Config = config

	if normalized.BatchSize <= 0 {
		normalized.BatchSize = _defaultBatchSize
	}

	if normalized.MaxWait <= 0 {
		normalized.MaxWait = _defaultMaxWait
	}

	if normalized.AckWait <= 0 {
		normalized.AckWait = _defaultAckWait
	}

	if normalized.MaxDeliver <= 0 {
		normalized.MaxDeliver = _defaultMaxDeliver
	}

	return normalized, nil
}

// ensureConsumer creates the durable consumer or brings an existing one in
// line with the configuration.
func (c *ReaderConfig) ensureConsumer(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	consumer, err := js.CreateOrUpdateConsumer(ctx, c.streamFor(c.Domain), c.consumerConfig())
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrStreamNotFound, c.streamFor(c.Domain))
		}

		return nil, fmt.Errorf("eventlog/nats: consumer %s on %s: %w", c.Consumer, c.Domain, err)
	}

	return consumer, nil
}

func (c *ReaderConfig) consumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Name:           c.Consumer,
		Durable:        c.Consumer,
		Description:    "AgentOS " + c.Domain + " consumer",
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        c.AckWait,
		MaxDeliver:     c.MaxDeliver,
		FilterSubjects: c.subjectsFor(c.Domain, c.shards()),
		DeliverPolicy:  c.deliverPolicy(),
		OptStartTime:   c.optStartTime(),
	}
}

// shards is the set of shards this consumer owns, defaulting to all of them.
func (c *ReaderConfig) shards() []uint32 {
	if len(c.Shards) > 0 {
		return c.Shards
	}

	return c.allShards()
}

func (c *ReaderConfig) deliverPolicy() jetstream.DeliverPolicy {
	if !c.StartTime.IsZero() {
		return jetstream.DeliverByStartTimePolicy
	}

	return jetstream.DeliverAllPolicy
}

func (c *ReaderConfig) optStartTime() *time.Time {
	if c.StartTime.IsZero() {
		return nil
	}

	start := c.StartTime

	return &start
}

// emptyBatch is a poll that found nothing, so a caller can write a plain loop
// without a nil check.
type emptyBatch struct{}

// Records implements eventlog.Batch.
func (emptyBatch) Records() []eventlog.ConsumedRecord {
	return nil
}

// Ack implements eventlog.Batch.
func (emptyBatch) Ack(context.Context) error {
	return nil
}

// consumerBatch is a fetched set of messages, acknowledged as a unit.
type consumerBatch struct {
	messages []jetstream.Msg
	records  []eventlog.ConsumedRecord
}

// Records implements eventlog.Batch.
func (b *consumerBatch) Records() []eventlog.ConsumedRecord {
	return b.records
}

// Ack confirms the batch. It waits for the server, so an acknowledgement that
// never landed cannot pass silently; messages acked before a failure stay
// acked, which is why consumers deduplicate on the envelope.
func (b *consumerBatch) Ack(ctx context.Context) error {
	for _, message := range b.messages {
		if err := message.DoubleAck(ctx); err != nil {
			return fmt.Errorf("eventlog/nats: ack %s: %w", message.Subject(), err)
		}
	}

	return nil
}

// consumerHeaders drops the transport's own headers, leaving what the
// publisher wrote.
func consumerHeaders(headers nats.Header) map[string]string {
	flat := make(map[string]string, len(headers))

	for name, values := range headers {
		if len(values) == 0 || strings.HasPrefix(name, _internalHeaderPrefix) {
			continue
		}

		flat[name] = values[0]
	}

	return flat
}
