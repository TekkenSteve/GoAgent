package temporal

import (
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func newPlanReplaySubscription(planEvents []agentos.PlanEvent) agentos.Subscription {
	out := make(chan agentos.Event, len(planEvents))
	for _, planEvent := range planEvents {
		out <- planEvent.ToEvent()
	}
	close(out)

	return &subscription{events: out}
}

func newPlanReplayThenLiveSubscription(scope agentos.PlanStreamScope, planEvents []agentos.PlanEvent, live agentosplan.PlanEventSubscription) agentos.Subscription {
	return newPlanReplayThenLiveSubscriptionAfter(scope, planEvents, live, 0)
}

func newPlanReplayThenLiveSubscriptionAfter(scope agentos.PlanStreamScope, planEvents []agentos.PlanEvent, live agentosplan.PlanEventSubscription, liveAfterSequence int64) agentos.Subscription {
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
				if event.Sequence <= liveAfterSequence || !planEventMatchesSubscriptionScope(event, scope) {
					continue
				}
				select {
				case <-done:
					return
				case out <- event.ToEvent():
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

func planEventMatchesSubscriptionScope(event agentos.PlanEvent, scope agentos.PlanStreamScope) bool {
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

	return true
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
