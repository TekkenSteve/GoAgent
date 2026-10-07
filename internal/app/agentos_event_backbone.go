package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/config"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/runprojection"
	"github.com/TekkenSteve/GoAgent/internal/repo/outbox"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	eventlognats "github.com/TekkenSteve/GoAgent/pkg/eventlog/nats"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

const (
	// eventBackboneStopTimeout bounds how long Stop waits for the drain loops to
	// notice cancellation. A drain pass is a claim plus a bounded batch of
	// publishes, so exceeding this means the log is not answering, and blocking
	// process shutdown on it would be worse than letting the goroutine die with
	// the process.
	eventBackboneStopTimeout = 15 * time.Second

	// conversationOutboxDomain names the conversation outbox in drain logs.
	conversationOutboxDomain = "conversation"

	// runTimelineOutboxDomain names the run timeline outbox in drain logs.
	runTimelineOutboxDomain = "run"

	// runProjectionConsumer is the durable consumer name of the run timeline
	// projection. The name is the consumer's progress: reusing it resumes
	// where it stopped, so a restart continues instead of replaying.
	runProjectionConsumer = "agentos-run-projection"
)

// outboxDomain pairs a domain's outbox with the name its drainer logs under, so
// one domain's publish failure is attributable without reading its SQL.
type outboxDomain struct {
	name   string
	source outbox.PendingSource
}

// eventBackbone is the event backbone's process-local assembly: one producer
// shared by every domain's outbox, one drain loop per domain, and the run
// timeline projection consuming the log back into the durable timeline.
//
// The producer side is the only place in the process that publishes to the
// log, which is the point of an outbox: a domain records what must be
// published in the same transaction as the state it describes, and a single
// publisher turns those records into log records. Draining per caller instead
// would publish every event once per caller.
//
// The projection side is the ruling's read half: facts flow Postgres →
// outbox → log → projection, so the timeline is reconstructible from the log
// and a writer's missed append heals on delivery. Its appends are idempotent
// no-ops at steady state.
//
// Producers and the drainer are deliberately separable. Any process sharing
// this database — an embedded AgentOS host, a worker, a migration — may
// enqueue outbox rows by enabling its domain's outbox, while exactly one
// process per deployment runs the drainer and the projection.
type eventBackbone struct {
	publisher  *eventlognats.Publisher
	logger     logger.Interface
	done       chan struct{}
	cancel     context.CancelFunc
	stopReader func()
	once       sync.Once
}

// startEventBackbone builds and starts the backbone write path. An empty URL
// disables it and returns nil: the backbone stays opt-in, and domains that
// enqueue outbox rows keep working untouched.
//
// On failure it releases whatever it already built, because its caller reports
// the failure through a fatal path that does not run deferred cleanup.
func startEventBackbone(parent context.Context, l logger.Interface, cfg *config.Config, pg *postgres.Postgres) (*eventBackbone, error) {
	backboneCfg := eventBackboneConfig(cfg)

	if !backboneCfg.Enabled() {
		l.Info("app - startEventBackbone - event backbone disabled: AGENTFW_NATS_URL is empty")

		return nil, nil
	}

	publisher, err := eventlognats.NewPublisher(backboneCfg)
	if err != nil {
		return nil, fmt.Errorf("app - startEventBackbone - eventlog/nats.NewPublisher: %w", err)
	}

	drainers, err := newDomainDrainers(l, publisher, pg)
	if err != nil {
		closeErr := publisher.Close()
		if closeErr != nil {
			l.Error(fmt.Errorf("app - startEventBackbone - close publisher: %w", closeErr))
		}

		return nil, err
	}

	projector, reader, err := newRunTimelineProjector(parent, l, backboneCfg, pg)
	if err != nil {
		closeErr := publisher.Close()
		if closeErr != nil {
			l.Error(fmt.Errorf("app - startEventBackbone - close publisher: %w", closeErr))
		}

		return nil, err
	}

	ctx, cancel := context.WithCancel(parent)
	backbone := &eventBackbone{
		publisher: publisher,
		logger:    l,
		done:      make(chan struct{}),
		cancel:    cancel,
		stopReader: func() {
			if err := reader.Close(); err != nil {
				l.Error(fmt.Errorf("app - eventBackbone.Stop - close reader: %w", err))
			}
		},
	}

	runDomainDrainers(ctx, l, backbone, drainers)

	go func() {
		if err := projector.Run(ctx); err != nil {
			l.Error(fmt.Errorf("app - startEventBackbone - run projection: %w", err))
		}
	}()

	l.Info("app - startEventBackbone - event backbone started: %d outbox domains + run projection on %s",
		len(drainers), backboneCfg.URL)

	return backbone, nil
}

// newRunTimelineProjector builds the log-consuming run projection: a durable
// pull consumer over the run timeline domain, landing every fact it delivers
// before acknowledging it. The consumer reads from the oldest retained record
// when first created, so a fresh projection backfills rather than skips.
//
// The store deliberately has no outbox: every fact on the log already carries
// its committed Postgres row (the outbox and the event land in one
// transaction), so the projector's appends are replays — and after a table
// rebuild, re-enqueueing the whole replayed timeline would republish facts
// the log already has.
func newRunTimelineProjector(parent context.Context, l logger.Interface, backboneCfg eventlognats.Config, pg *postgres.Postgres) (*runprojection.LogProjector, *eventlognats.Reader, error) {
	reader, err := openBackboneReader(parent, l, &eventlognats.ReaderConfig{
		Config:   backboneCfg,
		Domain:   eventlog.DomainRunTimeline,
		Consumer: runProjectionConsumer,
		// The projection keeps the default retry policy on purpose: its only
		// failure is the store being unreachable, which parking would convert
		// into a fact missing from the durable timeline.
		Logger: l,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app - newRunTimelineProjector - eventlog/nats.NewReader: %w", err)
	}

	projector, err := runprojection.NewLogProjector(runprojection.LogConfig{
		Reader: reader,
		Store:  temporalrepo.NewAgentOSRunEventRepo(pg),
		Logger: l,
	})
	if err != nil {
		reader.Close()

		return nil, nil, fmt.Errorf("app - newRunTimelineProjector - runprojection.NewLogProjector: %w", err)
	}

	return projector, reader, nil
}

// Stop cancels the drain loops and the projection, waits for the drainers to
// finish, releases the projection's reader, then flushes and releases the
// producer. It is idempotent, and a no-op on a disabled backbone.
//
// Stop must run before the Postgres pool the loops read from is closed: a
// drain pass that loses its pool mid-claim is an error an operator has to read,
// where a clean stop is not.
func (b *eventBackbone) Stop() {
	if b == nil {
		return
	}

	b.once.Do(func() {
		b.cancel()

		select {
		case <-b.done:
		case <-time.After(eventBackboneStopTimeout):
		}

		if b.stopReader != nil {
			b.stopReader()
		}

		if err := b.publisher.Close(); err != nil {
			// The log may already be unreachable during shutdown, and the
			// process is going down either way: report it, do not block on it.
			b.logger.Error(fmt.Errorf("app - eventBackbone.Stop - close publisher: %w", err))
		}
	})
}

// eventBackboneConfig maps the application configuration onto the backbone
// client configuration. It is the one place the two shapes meet, so an operator
// setting has a single reader.
func eventBackboneConfig(cfg *config.Config) eventlognats.Config {
	return eventlognats.Config{
		URL:           cfg.AgentFW.NatsURL,
		SubjectPrefix: cfg.AgentFW.NatsSubjectPrefix,
		Shards:        cfg.AgentFW.NatsShards,
		ClientName:    cfg.App.Name,
	}
}

// backboneStreamWait bounds how long startup waits for a consumer's stream to
// exist before giving up.
const (
	backboneStreamWaitTimeout  = 60 * time.Second
	backboneStreamWaitInterval = time.Second
)

// openBackboneReader opens a consumer, waiting briefly for its stream to be
// provisioned. Streams are an operator action (or the compose one-shot job), so
// a cell — especially a replica that must have its mirror created — can start
// before its stream exists; waiting turns that ordering mistake into a slower
// start instead of a crash loop. Any other failure is returned at once: a wrong
// URL, a credential or a rejected configuration must not hide behind a timeout.
func openBackboneReader(parent context.Context, l logger.Interface, cfg *eventlognats.ReaderConfig) (*eventlognats.Reader, error) {
	return retryUntilStreamExists(parent, l, func() (*eventlognats.Reader, error) {
		return eventlognats.NewReader(parent, cfg)
	})
}

// retryUntilStreamExists is the wait behind openBackboneReader, with the opener
// injected so the policy is testable without a broker.
func retryUntilStreamExists(
	parent context.Context,
	l logger.Interface,
	open func() (*eventlognats.Reader, error),
) (*eventlognats.Reader, error) {
	deadline := time.Now().Add(backboneStreamWaitTimeout)

	for {
		reader, err := open()
		if err == nil {
			return reader, nil
		}

		if !errors.Is(err, eventlognats.ErrStreamNotFound) {
			return nil, err
		}

		if time.Now().After(deadline) {
			return nil, err
		}

		if l != nil {
			l.Info("app - retryUntilStreamExists - waiting for the stream: %v", err)
		}

		select {
		case <-parent.Done():
			return nil, parent.Err()
		case <-time.After(backboneStreamWaitInterval):
		}
	}
}

// startAppEventBackbone starts the backbone write path and returns the function
// that stops it, so Run reads as "start, defer stop" like every other component
// it owns. A failure to build it is fatal: running with a producer-side outbox
// and no drainer silently turns every event into a growing backlog. A broker
// that is merely unreachable is not a failure to build — connecting retries in
// the background, and the drainer backs off per row until the log answers.
func startAppEventBackbone(parent context.Context, l logger.Interface, cfg *config.Config, pg *postgres.Postgres) func() {
	backbone, err := startEventBackbone(parent, l, cfg, pg)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - start event backbone: %w", err))
	}

	return backbone.Stop
}

// backboneDrainer couples a domain with the drainer that publishes its outbox.
type backboneDrainer struct {
	domain  outboxDomain
	drainer *outbox.Drainer
}

// runDomainDrainers starts one loop per domain and closes the backbone's done
// channel once every loop has stopped, which is what Stop waits on.
func runDomainDrainers(ctx context.Context, l logger.Interface, backbone *eventBackbone, drainers []backboneDrainer) {
	var draining sync.WaitGroup

	for _, drained := range drainers {
		draining.Add(1)

		go func(domain outboxDomain, drainer *outbox.Drainer) {
			defer draining.Done()

			if err := drainer.Run(ctx); err != nil {
				l.Error(fmt.Errorf("app - event backbone - drain %s outbox: %w", domain.name, err))
			}
		}(drained.domain, drained.drainer)
	}

	go func() {
		draining.Wait()
		close(backbone.done)
	}()
}

// newDomainDrainers builds one drainer per domain outbox this process owns.
// Adding a domain here is what makes its outbox reachable; the source is the
// only domain-specific piece, because the drainer itself holds no SQL.
func newDomainDrainers(l logger.Interface, publisher eventlog.Publisher, pg *postgres.Postgres) ([]backboneDrainer, error) {
	domains := []outboxDomain{
		{
			name:   conversationOutboxDomain,
			source: temporalrepo.NewAgentOSConversationOutboxSource(pg),
		},
		{
			name:   runTimelineOutboxDomain,
			source: temporalrepo.NewAgentOSRunOutboxSource(pg),
		},
	}

	drainers := make([]backboneDrainer, 0, len(domains))

	for _, domain := range domains {
		drainer, err := outbox.NewDrainer(domain.source, publisher,
			outbox.WithLogger(l),
			outbox.WithDeadLetter(eventlog.DeadLetterDomain),
		)
		if err != nil {
			return nil, fmt.Errorf("app - newDomainDrainers - outbox.NewDrainer(%s): %w", domain.name, err)
		}

		drainers = append(drainers, backboneDrainer{domain: domain, drainer: drainer})
	}

	return drainers, nil
}
