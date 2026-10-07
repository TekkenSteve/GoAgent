// Package temporalrouter turns facts on the event log into Temporal primitives.
//
// It is the middle of the cross-town loop: a peer publishes a fact, and this
// town reacts by starting or signaling a workflow. Nexus already carries
// cross-town *operations* with a durable promise; this is for the other
// direction — a fact somebody broadcast, which this town decides is its work.
//
// Three rules from the event-backbone design decide its shape:
//
//   - The envelope's entity id is the workflow ID, so the same fact always
//     lands on the same execution (rule 1). A route may override the
//     derivation, but the default is the entity.
//   - Delivery is duplicate-tolerant, not duplicate-impossible (rule 2).
//     Temporal is called before the batch is acknowledged, so a crash in between
//     redelivers. Every input therefore carries the fact's identity as an
//     idempotency token, and that identity is the envelope's event id: a log
//     coordinate would change when the log is replayed or mirrored to another
//     town, which is exactly when a duplicate must still be recognized.
//   - It must not stall its own poll loop (rule 3). The Temporal call between
//     Poll and Ack is one bounded RPC per record, and the batch is acknowledged
//     only once Temporal has taken every delivery in it.
//
// What it deliberately does not do: decide what a fact means. Routes are
// registered, so a new cross-town fact is a route, not a new consumer.
package temporalrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var (
	// ErrReaderRequired reports a router without a log to read.
	ErrReaderRequired = errors.New("temporal router: log reader is required")
	// ErrIngressRequired reports a router without a Temporal to call.
	ErrIngressRequired = errors.New("temporal router: ingress is required")
	// ErrRoutesRequired reports a router with nothing to deliver.
	ErrRoutesRequired = errors.New("temporal router: at least one route is required")
	// ErrRouteUndeliverable reports a route that names no workflow or no signal.
	ErrRouteUndeliverable = errors.New("temporal router: route needs a workflow type and a signal name")
	// ErrTaskQueueRequired reports an ingress that cannot say where to deliver.
	ErrTaskQueueRequired = errors.New("temporal router: task queue is required")
)

const (
	// pollRetryDelay bounds the spin when polling or delivering fails. Poll
	// blocks while the log is empty, but an error returns immediately, and
	// retrying that in a tight loop would turn a Temporal hiccup into a hot loop.
	pollRetryDelay = time.Second
)

// Fact is one decoded ingress record.
type Fact struct {
	// Domain is the logical log domain the fact was published on.
	Domain string
	// Sequence is the log's own position for the record. It is for diagnostics
	// only: it is not an identity, because replaying or mirroring the log
	// changes it.
	Sequence uint64
	// EventID is the producer-signed identity, and the idempotency token.
	EventID string
	// Key is the entity id from the envelope, which the taxonomy defines as
	// the workflow ID.
	Key string
	// Type is the fact type from the envelope.
	Type string
	// Payload is the fact body, passed to the workflow untouched.
	Payload json.RawMessage
}

// Token is the fact's stable identity: the producer's event id. A redelivery of
// the same fact carries the same token, and so does a copy of it in another
// town, which is what makes deduplication survive replay and mirroring.
func (f *Fact) Token() string {
	return f.EventID
}

// Input is what a workflow or signal receives: the fact's identity, and the
// payload as published. Identity is part of the contract rather than something a
// route can drop, because it is the only thing that makes a redelivery safe.
type Input struct {
	Token   string          `json:"token"`
	Domain  string          `json:"domain"`
	Type    string          `json:"type"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Delivery is what a route asks Temporal to do with a fact.
type Delivery struct {
	WorkflowID   string
	WorkflowType string
	SignalName   string
	Input        Input
}

// Ingress performs one delivery. *TemporalIngress is the production form; tests
// substitute a fake, so the router's logic needs no Temporal server.
type Ingress interface {
	SignalWithStart(ctx context.Context, delivery *Delivery) error
}

// Route maps one fact type on one domain to one delivery.
type Route struct {
	// Domain is the logical log domain to match, or empty to match any domain.
	Domain string
	// Type is the fact type to match. Required.
	Type string
	// WorkflowType is the workflow started when no execution exists yet.
	WorkflowType string
	// SignalName is the signal delivered to that execution. Together with
	// WorkflowType it makes the delivery a signal-with-start, which is what
	// makes "start if absent, signal if present" one atomic call.
	SignalName string
	// WorkflowID derives the workflow ID from the fact. Nil uses the fact's
	// entity id, which the taxonomy already defines as the workflow anchor.
	WorkflowID func(*Fact) string
}

func (r Route) matches(fact *Fact) bool {
	if r.Domain != "" && r.Domain != fact.Domain {
		return false
	}

	return r.Type == fact.Type
}

func (r Route) delivery(fact *Fact) *Delivery {
	workflowID := fact.Key
	if r.WorkflowID != nil {
		workflowID = r.WorkflowID(fact)
	}

	return &Delivery{
		WorkflowID:   workflowID,
		WorkflowType: r.WorkflowType,
		SignalName:   r.SignalName,
		Input: Input{
			Token:   fact.Token(),
			Domain:  fact.Domain,
			Type:    fact.Type,
			Key:     fact.Key,
			Payload: fact.Payload,
		},
	}
}

// Result reports what one routed batch did. Skipped counts records no route
// claimed and records that could not be decoded; neither is a failure, because
// a town subscribes to a topic, not to a private copy of it, and the log keeps
// what this town did not use.
type Result struct {
	Records   int
	Delivered int
	Skipped   int
	Failed    int
}

// Router delivers routed facts into Temporal.
type Router struct {
	reader  eventlog.Reader
	ingress Ingress
	routes  []Route
	logger  logger.Interface
}

// NewRouter creates the router. Routes are validated here rather than at
// delivery time: a route that can never be delivered is a configuration mistake,
// and finding it on the first fact instead of at start-up wastes the fact.
func NewRouter(cfg Config) (*Router, error) {
	if cfg.Reader == nil {
		return nil, ErrReaderRequired
	}

	if cfg.Ingress == nil {
		return nil, ErrIngressRequired
	}

	if len(cfg.Routes) == 0 {
		return nil, ErrRoutesRequired
	}

	for _, route := range cfg.Routes {
		if route.Type == "" || route.WorkflowType == "" || route.SignalName == "" {
			return nil, fmt.Errorf("%w: domain=%q type=%q", ErrRouteUndeliverable, route.Domain, route.Type)
		}
	}

	return &Router{
		reader:  cfg.Reader,
		ingress: cfg.Ingress,
		routes:  cfg.Routes,
		logger:  cfg.Logger,
	}, nil
}

// Config wires a router.
type Config struct {
	// Reader is the pull consumer over the fact domain.
	Reader eventlog.Reader
	// Ingress delivers one routed fact into Temporal.
	Ingress Ingress
	// Routes are the fact types this town reacts to.
	Routes []Route
	// Logger is optional; nil drops routing diagnostics.
	Logger logger.Interface
}

// RouteBatch polls once, delivers every fact a route claims, and acknowledges
// the batch only when Temporal has taken all of them.
//
// A failing delivery leaves the whole batch unacknowledged, so the facts are
// redelivered. That is the intended shape: the alternative — acknowledging a
// fact Temporal never accepted — would drop cross-town work with nothing left to
// replay it from. Redelivery is safe because the workflow ID is derived from the
// entity key and the input carries the fact's token.
func (r *Router) RouteBatch(ctx context.Context) (Result, error) {
	batch, err := r.reader.Poll(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("temporal router - poll: %w", err)
	}

	records := batch.Records()
	result := Result{Records: len(records)}

	var failures []error

	for i := range records {
		fact, err := decodeFact(&records[i])
		if err != nil {
			result.Skipped++

			r.warnf("temporal router - skipping undecodable record domain=%s sequence=%d: %v",
				records[i].Domain, records[i].Sequence, err)

			continue
		}

		route, ok := r.match(&fact)
		if !ok {
			result.Skipped++

			continue
		}

		if err := r.ingress.SignalWithStart(ctx, route.delivery(&fact)); err != nil {
			failures = append(failures, fmt.Errorf("temporal router - deliver %s %s: %w", fact.Token(), fact.Type, err))

			result.Failed++

			continue
		}

		result.Delivered++
	}

	if len(failures) > 0 {
		return result, errors.Join(failures...)
	}

	if err := batch.Ack(ctx); err != nil {
		return result, fmt.Errorf("temporal router - ack: %w", err)
	}

	return result, nil
}

// Run routes until the context is canceled.
//
// It checks the context on every pass rather than relying on Poll to block: a
// reader may return an empty batch immediately, and a loop that only noticed
// cancellation through a failing poll would spin on a quiet log.
func (r *Router) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		result, err := r.RouteBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			r.warnf("temporal router - route batch: %v", err)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(pollRetryDelay):
			}
		} else if result.Delivered > 0 {
			r.infof("temporal router - records=%d delivered=%d skipped=%d", result.Records, result.Delivered, result.Skipped)
		}
	}
}

func (r *Router) match(fact *Fact) (Route, bool) {
	for _, route := range r.routes {
		if route.matches(fact) {
			return route, true
		}
	}

	return Route{}, false
}

// decodeFact reads one record's envelope — the same versioned eventlog.Envelope
// every producer writes. Identity, type and entity travel in the body rather
// than in headers, so a consumer can never act on a record whose headers it
// failed to read: a fact whose type is unknown is skipped by name, a fact
// without an identity is refused rather than deduplicated under a made-up
// token, and an envelope this build cannot read stops the read instead of
// being guessed at.
func decodeFact(record *eventlog.ConsumedRecord) (Fact, error) {
	envelope, err := eventlog.DecodeEnvelope(record.Value)
	if err != nil {
		return Fact{}, fmt.Errorf("decode envelope: %w", err)
	}

	return Fact{
		Domain:   record.Domain,
		Sequence: record.Sequence,
		EventID:  envelope.EventID,
		Key:      envelope.Entity.ID,
		Type:     envelope.EventType,
		Payload:  envelope.Payload,
	}, nil
}

func (r *Router) warnf(format string, args ...any) {
	if r.logger == nil {
		return
	}

	r.logger.Warn(format, args...)
}

func (r *Router) infof(format string, args ...any) {
	if r.logger == nil {
		return
	}

	r.logger.Info(format, args...)
}
