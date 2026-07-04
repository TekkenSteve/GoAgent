package agentosprocess

import "github.com/TekkenSteve/GoAgent/agentos"

type replaySubscription struct {
	events <-chan agentos.Event
}

func newReplaySubscription(processEvents []agentos.ProcessEvent) agentos.Subscription {
	out := make(chan agentos.Event, len(processEvents))
	for i := range processEvents {
		out <- processEvents[i].ToEvent()
	}

	close(out)

	return &replaySubscription{events: out}
}

func (s *replaySubscription) Events() <-chan agentos.Event {
	return s.events
}

func (s *replaySubscription) Close() error {
	return nil
}
