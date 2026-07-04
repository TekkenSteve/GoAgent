package agentosprocess

import (
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

type replaySubscription struct {
	events <-chan agentoscore.Event
}

func newReplaySubscription(processEvents []agentos.Event) agentoscore.Subscription {
	out := make(chan agentoscore.Event, len(processEvents))
	for i := range processEvents {
		out <- processEvents[i].ToEvent()
	}

	close(out)

	return &replaySubscription{events: out}
}

func (s *replaySubscription) Events() <-chan agentoscore.Event {
	return s.events
}

func (s *replaySubscription) Close() error {
	return nil
}
