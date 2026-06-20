package temporal

import (
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func newPlanReplaySubscription(planEvents []agentos.PlanEvent) agentos.Subscription {
	out := make(chan agentos.Event, len(planEvents))
	for _, planEvent := range planEvents {
		out <- planEvent.ToEvent()
	}
	close(out)

	return &subscription{events: out}
}

func newPlanReplayThenLiveSubscription(planEvents []agentos.PlanEvent, live agentos.Subscription) agentos.Subscription {
	return newPlanReplayThenLiveSubscriptionAfter(planEvents, live, 0)
}

func newPlanReplayThenLiveSubscriptionAfter(planEvents []agentos.PlanEvent, live agentos.Subscription, liveAfterSequence int64) agentos.Subscription {
	if live == nil {
		return newPlanReplaySubscription(planEvents)
	}

	out := make(chan agentos.Event, len(planEvents))
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(out)
		for _, planEvent := range planEvents {
			select {
			case <-done:
				return
			case out <- planEvent.ToEvent():
			}
		}
		for {
			select {
			case <-done:
				return
			case event, ok := <-live.Events():
				if !ok {
					return
				}
				if event.Sequence <= liveAfterSequence {
					continue
				}
				select {
				case <-done:
					return
				case out <- event:
				}
			}
		}
	}()

	return &subscription{
		events: out,
		close: func() error {
			var err error
			once.Do(func() {
				close(done)
				err = live.Close()
			})

			return err
		},
	}
}

func lastPlanEventSequence(afterSequence int64, planEvents []agentos.PlanEvent) int64 {
	last := afterSequence
	for _, planEvent := range planEvents {
		if planEvent.Sequence > last {
			last = planEvent.Sequence
		}
	}

	return last
}
