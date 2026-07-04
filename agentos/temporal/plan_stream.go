package temporal

import (
	"sync"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func newPlanReplaySubscription(planEvents []agentos.PlanEvent) agentoscore.Subscription {
	out := make(chan agentoscore.Event, len(planEvents))
	for i := range planEvents {
		out <- planEvents[i].ToEvent()
	}

	close(out)

	return &subscription{events: out}
}

func newPlanReplayThenLiveSubscription(scope *agentos.PlanStreamScope, planEvents []agentos.PlanEvent, live agentosplan.PlanEventSubscription) agentoscore.Subscription {
	return newPlanReplayThenLiveSubscriptionAfter(scope, planEvents, live, 0)
}

func newPlanReplayThenLiveSubscriptionAfter(scope *agentos.PlanStreamScope, planEvents []agentos.PlanEvent, live agentosplan.PlanEventSubscription, liveAfterSequence int64) agentoscore.Subscription {
	if live == nil {
		return newPlanReplaySubscription(planEvents)
	}

	out := make(chan agentoscore.Event, len(planEvents))
	done := make(chan struct{})

	var once sync.Once

	go func() {
		defer close(out)

		if !replayPlanEvents(out, done, planEvents) {
			return
		}

		forwardLivePlanEvents(out, done, scope, live, liveAfterSequence)
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

func replayPlanEvents(out chan<- agentoscore.Event, done <-chan struct{}, planEvents []agentos.PlanEvent) bool {
	for i := range planEvents {
		planEvent := &planEvents[i]

		select {
		case <-done:
			return false
		case out <- planEvent.ToEvent():
		}
	}

	return true
}

func forwardLivePlanEvents(
	out chan<- agentoscore.Event,
	done <-chan struct{},
	scope *agentos.PlanStreamScope,
	live agentosplan.PlanEventSubscription,
	liveAfterSequence int64,
) {
	for {
		select {
		case <-done:
			return
		case event, ok := <-live.Events():
			if !ok {
				return
			}

			if event.Sequence <= liveAfterSequence || !planEventMatchesSubscriptionScope(&event, scope) {
				continue
			}

			select {
			case <-done:
				return
			case out <- event.ToEvent():
			}
		}
	}
}

func planEventMatchesSubscriptionScope(event *agentos.PlanEvent, scope *agentos.PlanStreamScope) bool {
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

	for i := range planEvents {
		planEvent := &planEvents[i]

		if planEvent.Sequence > last {
			last = planEvent.Sequence
		}
	}

	return last
}
