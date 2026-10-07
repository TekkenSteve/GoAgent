package temporal

import (
	"fmt"

	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/centrifugo"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// StreamingDataPlane is the assembled live transport for the AG-UI timeline
// plus the fact recorder every writer shares: one Publisher (the Centrifugo
// client by default, or the in-process memstream bus when Centrifugo is not
// configured), one Subscriber for the app's own read roles, and the
// MilestoneRecorder that makes each run fact durable before its bus publish.
//
// The recorder is the write half of the fact-first ruling: facts land in
// Postgres (with their fact-log publication intent) at the writer, the bus
// only mirrors, and the event backbone's projection consumes the log back.
// It is the single live transport after the legacy Redis fan-out migration:
// activities publish through it, run subscriptions / plan SSE read it.
type StreamingDataPlane struct {
	Publisher  agentosstream.Publisher
	Subscriber agentosstream.Subscriber
	Facts      *streamadapter.MilestoneRecorder
}

// NewStreamingDataPlane assembles the data plane. Centrifugo is the default
// transport: with a configured base URL it builds the client pair over the
// server API / websocket. When no Centrifugo is configured it degrades to the
// in-process memstream bus (a warning is logged) — useful for a single-machine
// run, but not a production transport — where Publisher and Subscriber are the
// same instance so single-process subscribers see the activities' publishes.
//
// The recorder is always created: every milestone a writer emits is made
// durable before it reaches the bus, whether the bus is Centrifugo or
// memstream. Deployments that run the event backbone (AGENTFW_NATS_URL)
// enable the outbox option, so each recorded fact is also queued for the log.
func NewStreamingDataPlane(cfg StreamCentrifugoConfig, pg *postgres.Postgres, l *logger.Logger) (*StreamingDataPlane, error) {
	var (
		publisher  agentosstream.Publisher
		subscriber agentosstream.Subscriber
	)

	if cfg.BaseURL == "" {
		l.Warn("no Centrifugo base URL configured; degrading to the in-process memstream bus")

		bus := memstream.New()
		publisher = bus
		subscriber = bus
	} else {
		busCfg := centrifugo.Config{BaseURL: cfg.BaseURL, APIKey: cfg.APIKey}

		pub, err := centrifugo.NewPublisher(busCfg)
		if err != nil {
			return nil, fmt.Errorf("agentos data plane - stream publisher: %w", err)
		}

		sub, err := centrifugo.NewSubscriber(busCfg)
		if err != nil {
			return nil, fmt.Errorf("agentos data plane - stream subscriber: %w", err)
		}

		publisher = pub
		subscriber = sub
	}

	var storeOpts []temporalrepo.RunEventRepoOption
	if cfg.EventOutbox {
		storeOpts = append(storeOpts, temporalrepo.WithRunEventOutbox())
	}

	recorder, err := streamadapter.NewMilestoneRecorder(
		temporalrepo.NewAgentOSRunEventRepo(pg, storeOpts...), l,
	)
	if err != nil {
		return nil, fmt.Errorf("agentos data plane - milestone recorder: %w", err)
	}

	return &StreamingDataPlane{
		Publisher:  publisher,
		Subscriber: subscriber,
		Facts:      recorder,
	}, nil
}

// Close releases the data plane's per-owner resources. The bus transport is
// shared and has no per-owner close; the recorder holds only a store
// reference over the caller's pool, so there is nothing to stop here.
func (d *StreamingDataPlane) Close() error {
	return nil
}
