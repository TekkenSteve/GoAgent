// Package planstream streams AgentOS plan events from the plan store.
package planstream

import (
	"context"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

const (
	// planEventCustomName is the namespaced CUSTOM name carrying plan events on
	// the data plane.
	planEventCustomName = "agentos.plan.event"
	// planEventDataKey is the payload key holding the serialized PlanEvent.
	planEventDataKey = "data"
	// planSubscriberBuffer is the live delivery buffer before a slow consumer
	// drops, mirroring the bus fan-out policy.
	planSubscriberBuffer = 256
)

// PlanEventStream publishes and subscribes public AgentOS PlanEvents on the
// data-plane bus. Durable replay remains owned by the Postgres PlanEventStore;
// the bus carries only the live tail (LiveOnly), so a bus hiccup is never a
// data loss — SubscribePlan falls back to pure Postgres replay.
type PlanEventStream struct {
	pub stream.Publisher
	sub stream.Subscriber
}

var (
	_ agentosplan.PlanEventPublisher  = (*PlanEventStream)(nil)
	_ agentosplan.PlanEventSubscriber = (*PlanEventStream)(nil)
)

// New creates the bus-backed plan event stream. The publisher and subscriber
// are the assembled data plane (memstream in-process pair or Centrifugo).
func New(pub stream.Publisher, sub stream.Subscriber) *PlanEventStream {
	return &PlanEventStream{pub: pub, sub: sub}
}

// PublishPlanEvent publishes a sequenced PlanEvent on the plan's live channel.
// The durable copy (and sequence assignment) remains owned by the PlanEventStore;
// this is the live fan-out after append.
func (s *PlanEventStream) PublishPlanEvent(ctx context.Context, event *agentos.PlanEvent) error {
	if s == nil || s.pub == nil {
		return fmt.Errorf("%w: plan event publisher is not configured", agentoscore.ErrInvalidPlanEvent)
	}

	// MarshalPlanEvent validates the tenant scope and sequence, so an
	// un-scoped event is rejected here at the boundary, not by the bus.
	data, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		return err
	}

	wire := stream.NewCustom(event.ThreadID, event.RunID, planEventCustomName)
	wire.Set(planEventDataKey, string(data))

	return s.pub.Publish(ctx, streamadapter.HandleForPlan(event.AccountID, event.PlanID), wire)
}

// SubscribePlanEvents subscribes to the live PlanEvent tail. The bus
// subscription is live-only: Postgres owns replay, so nothing in the retained
// window is read here. Plan-event-sequence filtering mirrors the Redis
// subscriber it replaces, so the caller's AfterSequence cursor stays the plan
// event cursor, not the bus offset.
func (s *PlanEventStream) SubscribePlanEvents(ctx context.Context, scope *agentos.PlanStreamScope) (agentosplan.PlanEventSubscription, error) {
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return nil, err
	}

	if s == nil || s.sub == nil {
		return nil, fmt.Errorf("%w: plan event subscriber is not configured", agentoscore.ErrInvalidStreamScope)
	}

	sub, err := s.sub.Subscribe(ctx, streamadapter.HandleForPlan(scope.AccountID, scope.PlanID), stream.LiveOnly)
	if err != nil {
		return nil, err
	}

	out := make(chan agentos.PlanEvent, planSubscriberBuffer)

	go func() {
		defer close(out)

		for stored := range sub.C {
			event, err := planEventFromStored(&stored)
			if err != nil || !planEventMatchesScope(&event, scope) {
				continue
			}

			out <- event
		}
	}()

	return &planLiveSubscription{
		events: out,
		close: func() error {
			sub.Close()

			return nil
		},
	}, nil
}

// planEventFromStored decodes the PlanEvent carried inside a bus CUSTOM event.
func planEventFromStored(stored *stream.StoredEvent) (agentos.PlanEvent, error) {
	data, ok := stored.Event.Payload[planEventDataKey].(string)
	if !ok {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event bus entry has no data", agentoscore.ErrInvalidPlanEvent)
	}

	return agentos.UnmarshalPlanEvent([]byte(data))
}

// planLiveSubscription is the typed live tail for public RunPlan events.
type planLiveSubscription struct {
	events <-chan agentos.PlanEvent
	close  func() error
}

func (s *planLiveSubscription) Events() <-chan agentos.PlanEvent {
	return s.events
}

func (s *planLiveSubscription) Close() error {
	if s.close == nil {
		return nil
	}

	return s.close()
}

// planEventMatchesScope filters a live event against the caller's tenant scope
// and cursor. NodeID/RunID narrow the scope when set; a sequence at or below
// the cursor is already covered by the Postgres replay.
func planEventMatchesScope(event *agentos.PlanEvent, scope *agentos.PlanStreamScope) bool {
	if event.PlanID != scope.PlanID {
		return false
	}

	if event.AccountID != scope.AccountID {
		return false
	}

	if event.ProjectID != scope.ProjectID {
		return false
	}

	if scope.NodeID != "" && event.NodeID != scope.NodeID {
		return false
	}

	if scope.RunID != "" && event.RunID != scope.RunID {
		return false
	}

	if event.Sequence <= scope.AfterSequence {
		return false
	}

	return true
}
