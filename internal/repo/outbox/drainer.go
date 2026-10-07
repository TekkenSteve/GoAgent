// Package outbox drains transactional outboxes onto the event backbone.
//
// An outbox is what makes the log a *derived* view of the system of record
// instead of a second write: a row lands in the outbox in the same transaction
// as the state it describes, and this drainer publishes it afterwards. Outbox
// rows reference their source record rather than copying its payload (claim
// check), so no bytes are stored twice.
//
// Each domain owns its outbox table and implements PendingSource; the drainer
// holds no SQL. Delivery is at-least-once — a crash between publish and
// MarkPublished redelivers — so consumers must be idempotent.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

const (
	// DefaultBatchSize bounds one claim so a large backlog cannot monopolize the
	// drainer or the broker.
	DefaultBatchSize = 100
	// DefaultMaxAttempts is the point where a record is treated as poison and
	// moved to the dead-letter domain instead of blocking the queue forever.
	DefaultMaxAttempts = 8
	// DefaultInterval is the idle poll interval.
	DefaultInterval = time.Second

	// headerAttempts records how often the drainer tried this record, which is
	// what a consumer needs to tell a first delivery from a redelivery.
	headerAttempts = "attempts"
)

var (
	// ErrSourceRequired reports a drainer without an outbox to drain.
	ErrSourceRequired = errors.New("outbox drainer: pending source is required")
	// ErrPublisherRequired reports a drainer without a log to publish to.
	ErrPublisherRequired = errors.New("outbox drainer: publisher is required")
)

// Pending is one outbox row waiting to be published. Payload is loaded from the
// source record, never stored in the outbox itself.
//
// ID is the record's identity: the source uses it for MarkPublished, and the
// drainer passes it on as the log's deduplication token, so a re-publish after
// a crash collapses instead of duplicating while the token is still tracked.
type Pending struct {
	ID       string
	Domain   string
	Key      string
	Payload  []byte
	Headers  map[string]string
	Attempts int
}

// PendingSource is one domain's outbox. ClaimPending must claim atomically
// (FOR UPDATE SKIP LOCKED) so several drainers never publish the same row.
type PendingSource interface {
	ClaimPending(ctx context.Context, limit int) ([]Pending, error)
	MarkPublished(ctx context.Context, ids []string) error
	MarkFailed(ctx context.Context, id, cause string, attempts int) error
}

// Result reports what one drain pass did.
type Result struct {
	Claimed      int
	Published    int
	Failed       int
	DeadLettered int
}

// Drainer publishes outbox rows onto the log.
type Drainer struct {
	source      PendingSource
	publisher   eventlog.Publisher
	deadLetter  func(domain string) string
	logger      logger.Interface
	batchSize   int
	maxAttempts int
	interval    time.Duration
}

// Option tunes a Drainer.
type Option func(*Drainer)

// WithBatchSize bounds how many rows one claim returns.
func WithBatchSize(size int) Option {
	return func(d *Drainer) {
		if size > 0 {
			d.batchSize = size
		}
	}
}

// WithMaxAttempts sets when a record is dead-lettered.
func WithMaxAttempts(attempts int) Option {
	return func(d *Drainer) {
		if attempts > 0 {
			d.maxAttempts = attempts
		}
	}
}

// WithInterval sets the idle poll interval.
func WithInterval(interval time.Duration) Option {
	return func(d *Drainer) {
		if interval > 0 {
			d.interval = interval
		}
	}
}

// WithDeadLetter maps a domain to its dead-letter domain. Without it, failing
// records are retried up to the attempt limit and never moved aside — a policy
// choice the composition root makes, not the drainer.
func WithDeadLetter(domain func(string) string) Option {
	return func(d *Drainer) {
		d.deadLetter = domain
	}
}

// WithLogger reports drain progress and per-record failures.
func WithLogger(l logger.Interface) Option {
	return func(d *Drainer) {
		d.logger = l
	}
}

// NewDrainer creates a drainer over one domain outbox.
func NewDrainer(source PendingSource, publisher eventlog.Publisher, options ...Option) (*Drainer, error) {
	if source == nil {
		return nil, ErrSourceRequired
	}

	if publisher == nil {
		return nil, ErrPublisherRequired
	}

	drainer := &Drainer{
		source:      source,
		publisher:   publisher,
		batchSize:   DefaultBatchSize,
		maxAttempts: DefaultMaxAttempts,
		interval:    DefaultInterval,
	}

	for _, option := range options {
		option(drainer)
	}

	return drainer, nil
}

// DrainOnce publishes one claimed batch and reports what happened. Publishing
// failures leave the row unpublished for the next pass; a record that has
// exhausted its attempts is moved to the dead-letter domain so one poison row
// cannot stall every record behind it.
//
// An unreachable log is not a poison record: a failure wrapped with
// eventlog.ErrLogUnavailable does not advance the attempt counter, so an
// outage retries behind its backoff without ever spending the dead-letter
// budget. Only failures that follow a reachable log are the record's own.
func (d *Drainer) DrainOnce(ctx context.Context) (Result, error) {
	pending, err := d.source.ClaimPending(ctx, d.batchSize)
	if err != nil {
		return Result{}, fmt.Errorf("outbox drainer - claim pending: %w", err)
	}

	result := Result{Claimed: len(pending)}

	published := make([]string, 0, len(pending))

	var failures []error

	for i := range pending {
		record := pending[i]

		deadLettered, err := d.drainOne(ctx, &record)
		if err != nil {
			failures = append(failures, err)

			if markErr := d.markFailed(ctx, &record, err, d.chargedAttempts(&record, err)); markErr != nil {
				failures = append(failures, markErr)
			}

			result.Failed++

			continue
		}

		published = append(published, record.ID)

		if deadLettered {
			result.DeadLettered++

			continue
		}

		result.Published++
	}

	if len(published) > 0 {
		if err := d.source.MarkPublished(ctx, published); err != nil {
			return result, fmt.Errorf("outbox drainer - mark published: %w", err)
		}
	}

	return result, errors.Join(failures...)
}

// drainOne publishes one claimed record — to the primary domain, or, once the
// record has exhausted its attempts, to the dead-letter domain — and reports
// which of the two took it.
func (d *Drainer) drainOne(ctx context.Context, record *Pending) (deadLettered bool, err error) {
	deadLettered = d.deadLetter != nil && record.Attempts+1 >= d.maxAttempts

	domain := record.Domain
	if deadLettered {
		domain = d.deadLetter(record.Domain)
	}

	return deadLettered, d.publish(ctx, domain, record, record.Attempts+1)
}

// chargedAttempts is the attempt count a failed publish records: the next one,
// unless the log itself was unreachable — an outage is not the record's fault,
// and spending the dead-letter budget on one would divert every pending fact
// to the dead letter on recovery.
func (d *Drainer) chargedAttempts(record *Pending, err error) int {
	if errors.Is(err, eventlog.ErrLogUnavailable) {
		return record.Attempts
	}

	return record.Attempts + 1
}

// Run drains until the context is canceled.
func (d *Drainer) Run(ctx context.Context) error {
	for {
		result, err := d.DrainOnce(ctx)
		if err != nil {
			d.warnf("outbox drainer - drain pass: %v", err)
		} else if result.Claimed > 0 {
			d.infof("outbox drainer - drained claimed=%d published=%d failed=%d dead_lettered=%d",
				result.Claimed, result.Published, result.Failed, result.DeadLettered)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.interval):
		}
	}
}

func (d *Drainer) publish(ctx context.Context, domain string, record *Pending, attempts int) error {
	headers := make(map[string]string, len(record.Headers)+1)
	maps.Copy(headers, record.Headers)

	headers[headerAttempts] = strconv.Itoa(attempts)

	if err := d.publisher.Publish(ctx, &eventlog.Record{
		Domain:  domain,
		Key:     record.Key,
		ID:      record.ID,
		Value:   record.Payload,
		Headers: headers,
	}); err != nil {
		return fmt.Errorf("outbox drainer - publish %s: %w", domain, err)
	}

	return nil
}

func (d *Drainer) markFailed(ctx context.Context, record *Pending, cause error, attempts int) error {
	if err := d.source.MarkFailed(ctx, record.ID, cause.Error(), attempts); err != nil {
		return fmt.Errorf("outbox drainer - mark failed %s: %w", record.ID, err)
	}

	return nil
}

func (d *Drainer) warnf(format string, args ...any) {
	if d.logger == nil {
		return
	}

	d.logger.Warn(format, args...)
}

func (d *Drainer) infof(format string, args ...any) {
	if d.logger == nil {
		return
	}

	d.logger.Info(format, args...)
}
