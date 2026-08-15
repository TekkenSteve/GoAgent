package temporal

import (
	"context"
	"fmt"

	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/runprojection"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/centrifugo"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// StreamingDataPlane is the assembled live transport for the AG-UI timeline:
// one Publisher plus one Subscriber (the Centrifugo client pair by default, or
// the in-process memstream bus when Centrifugo is not configured) and the
// durable run-event projector that consumes the subscriber and persists
// milestones to Postgres. It is the single live transport after the legacy
// Redis fan-out migration: activities publish, run subscriptions / plan SSE /
// the projection read.
type StreamingDataPlane struct {
	Publisher  agentosstream.Publisher
	Subscriber agentosstream.Subscriber
	Projector  *runprojection.RunEventProjector
}

// NewStreamingDataPlane assembles the data plane. Centrifugo is the default
// transport: with a configured base URL it builds the client pair over the
// server API / websocket. When no Centrifugo is configured it degrades to the
// in-process memstream bus (a warning is logged) — useful for a single-machine
// run, but not a production transport — where Publisher and Subscriber are the
// same instance so single-process subscribers see the activities' publishes.
// The projector is always created: it reduces the subscriber's stream to
// durable milestones and persists them to Postgres.
func NewStreamingDataPlane(parent context.Context, cfg StreamCentrifugoConfig, pg *postgres.Postgres, l *logger.Logger) (*StreamingDataPlane, error) {
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

	projector, err := runprojection.New(parent, runprojection.Config{
		Subscriber: subscriber,
		Store:      temporalrepo.NewAgentOSRunEventRepo(pg),
		Logger:     l,
	})
	if err != nil {
		return nil, fmt.Errorf("agentos data plane - run projection: %w", err)
	}

	return &StreamingDataPlane{
		Publisher:  publisher,
		Subscriber: subscriber,
		Projector:  projector,
	}, nil
}

// Close stops the run projection consumer. The bus transport is shared and has
// no per-owner close; the projector holds the consuming goroutine and the
// parent context, so closing it is the only per-owner teardown.
func (d *StreamingDataPlane) Close() error {
	if d == nil || d.Projector == nil {
		return nil
	}

	d.Projector.Close()

	return nil
}
