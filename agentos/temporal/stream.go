package temporal

import (
	"context"
	"errors"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
)

type subscription struct {
	events <-chan agentoscore.Event
	close  func() error
}

func (s *subscription) Events() <-chan agentoscore.Event {
	return s.events
}

func (s *subscription) Close() error {
	if s.close == nil {
		return nil
	}

	return s.close()
}

// newSubscription consumes the data-plane subscription for one run channel and
// reduces it to the control plane's milestone event model via ProjectToCore —
// the same reduction the run projection applies, so the subscribe API and the
// durable timeline agree on what survives. The bus offset becomes the event's
// Sequence and its id (run:offset), mirroring the projection sink.
func newSubscription(handle *agentosstream.Handle, internalSub *agentosstream.Subscription) agentoscore.Subscription {
	out := make(chan agentoscore.Event)

	go func() {
		defer close(out)

		for stored := range internalSub.C {
			core, ok := agentosstream.ProjectToCore(handle, &stored)
			if !ok {
				continue
			}

			core.Sequence = stored.Sequence
			core.EventID = fmt.Sprintf("%s:%d", core.RunID, stored.Sequence)

			out <- *core
		}
	}()

	return &subscription{
		events: out,
		close: func() error {
			internalSub.Close()

			return nil
		},
	}
}

var errAgentOSSubscriberNotConfigured = errors.New("agentos temporal subscriber: stream subscriber is not configured")

type agentOSSubscriber struct {
	subscriber agentosstream.Subscriber
}

func newAgentOSSubscriber(subscriber agentosstream.Subscriber) *agentOSSubscriber {
	if subscriber == nil {
		return nil
	}

	return &agentOSSubscriber{subscriber: subscriber}
}

// SubscribeAgentOS subscribes to a run's data-plane channel. The run timeline
// is published under streamadapter.HandleForRun; the tenant falls back to
// "default" because the scope carries no account (matching the run projection).
func (s *agentOSSubscriber) SubscribeAgentOS(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	if s == nil || s.subscriber == nil {
		return nil, errAgentOSSubscriberNotConfigured
	}

	if scope.RunID == "" {
		return nil, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidStreamScope)
	}

	handle := streamadapter.HandleForRun("", scope.RunID)

	sub, err := s.subscriber.Subscribe(ctx, handle, scope.AfterSequence)
	if err != nil {
		return nil, err
	}

	return newSubscription(handle, sub), nil
}
