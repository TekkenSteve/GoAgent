package runprojection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var (
	// ErrLogReaderRequired reports a projector without a log to read.
	ErrLogReaderRequired = errors.New("run projection: log reader is required")
	// ErrLogStoreRequired reports a projector without a durable sink.
	ErrLogStoreRequired = errors.New("run projection: store is required")
	// ErrFactWithoutIdentity reports a decoded fact that carries no run
	// identity, so it cannot be keyed into the timeline.
	ErrFactWithoutIdentity = errors.New("run projection: fact carries no run identity")
	// ErrFactPayloadUndecodable reports an envelope whose payload is not the
	// domain event this projection consumes.
	ErrFactPayloadUndecodable = errors.New("run projection: fact payload undecodable")
	// ErrEnvelopeUndecodable reports a record that is not a valid envelope.
	ErrEnvelopeUndecodable = errors.New("run projection: envelope undecodable")
)

const (
	// logPollRetryDelay bounds the spin when polling fails. A poll blocks
	// while the log is empty, but an error returns immediately, and retrying
	// that in a tight loop would turn a broker hiccup into a hot loop — the
	// same bound the temporal router applies.
	logPollRetryDelay = time.Second
)

// AppendRunEventStore is the durable sink the log projector writes: the same
// idempotent append the writers use, so a replayed fact lands as a no-op
// rather than a duplicate.
type AppendRunEventStore interface {
	AppendRunEvent(ctx context.Context, ev *agentoscore.Event) error
}

// LogConfig wires a log-consuming run projector.
type LogConfig struct {
	// Reader is the pull consumer over the run timeline domain of the fact log.
	Reader eventlog.Reader
	// Store persists each replayed fact idempotently.
	Store AppendRunEventStore
	// Logger is optional; nil drops projection diagnostics.
	Logger logger.Interface
}

// LogProjector is the read role on the fact log: it consumes the run timeline
// domain, decodes each envelope's fact, and re-appends it to the durable
// timeline — the consumer side of the ruling that facts flow Postgres →
// outbox → log → projection.
//
// At steady state its appends are no-ops (the writer already made each fact
// durable before publishing), which is exactly what makes it safe to always
// run: it is the standing proof that the timeline is reconstructible from the
// log, the healing path for a fact the writer could not land, and the rebuild
// path after the table is dropped (delete the consumer, recreate it with a
// start time, replay).
//
// Acknowledgement follows landing: a batch is confirmed only once every fact
// in it is stored, so a crash in between redelivers — and the idempotent
// append makes that redelivery free.
type LogProjector struct {
	reader eventlog.Reader
	store  AppendRunEventStore
	logger logger.Interface
}

// NewLogProjector creates a projector over the fact log.
func NewLogProjector(cfg LogConfig) (*LogProjector, error) {
	if cfg.Reader == nil {
		return nil, ErrLogReaderRequired
	}

	if cfg.Store == nil {
		return nil, ErrLogStoreRequired
	}

	return &LogProjector{
		reader: cfg.Reader,
		store:  cfg.Store,
		logger: cfg.Logger,
	}, nil
}

// Result reports what one projected batch did.
type Result struct {
	// Records is the number of log records the batch carried.
	Records int
	// Projected is the number of facts appended (durable no-ops included).
	Projected int
	// Skipped counts records that could not be decoded as run facts; they are
	// acknowledged rather than stranded, mirroring the temporal router: the
	// log keeps what this consumer did not use.
	Skipped int
}

// ProjectBatch polls once, lands every decodable fact, and acknowledges the
// batch only after the store has taken all of them. A persist failure leaves
// the batch unacknowledged so the log redelivers it.
func (p *LogProjector) ProjectBatch(ctx context.Context) (Result, error) {
	batch, err := p.reader.Poll(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("run projection - poll: %w", err)
	}

	records := batch.Records()
	result := Result{Records: len(records)}

	for i := range records {
		ev, err := decodeRunFact(&records[i])
		if err != nil {
			result.Skipped++

			p.warnf("run projection - skipping undecodable record domain=%s sequence=%d: %v",
				records[i].Domain, records[i].Sequence, err)

			continue
		}

		if err := p.store.AppendRunEvent(ctx, ev); err != nil {
			// No ack: the batch is redelivered whole, and every append in it
			// is idempotent, so the retry costs nothing but the wait.
			return result, fmt.Errorf("run projection - append %s %s: %w", ev.RunID, ev.EventType, err)
		}

		result.Projected++
	}

	if err := batch.Ack(ctx); err != nil {
		return result, fmt.Errorf("run projection - ack: %w", err)
	}

	return result, nil
}

// Run polls until the context is canceled. A poll or persist failure is
// logged and retried after a bounded delay rather than stopping the loop: a
// projection that stops silently diverges from the log it is meant to mirror.
func (p *LogProjector) Run(ctx context.Context) error {
	for {
		_, err := p.ProjectBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			p.warnf("run projection: %v", err)

			if !sleepContext(ctx, logPollRetryDelay) {
				return nil
			}

			continue
		}

		if ctx.Err() != nil {
			return nil
		}
	}
}

// decodeRunFact turns one log record back into the durable event form. The
// envelope's payload is the domain event as the writer stored it — the run
// outbox publishes the event verbatim — so the decode is the writer's append
// inverted, and the event's own (run_id, sequence) is the idempotency key.
func decodeRunFact(record *eventlog.ConsumedRecord) (*agentoscore.Event, error) {
	envelope, err := eventlog.DecodeEnvelope(record.Value)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEnvelopeUndecodable, err)
	}

	var ev agentoscore.Event
	if err := json.Unmarshal(envelope.Payload, &ev); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrFactPayloadUndecodable, envelope.EventID, err)
	}

	if ev.RunID == "" || ev.Sequence <= 0 || ev.EventType == "" {
		return nil, fmt.Errorf("%w: %s", ErrFactWithoutIdentity, envelope.EventID)
	}

	return &ev, nil
}

// sleepContext waits for the duration or the context, reporting which one
// ended the wait.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *LogProjector) warnf(format string, args ...any) {
	if p.logger != nil {
		p.logger.Warn(format, args...)
	}
}
