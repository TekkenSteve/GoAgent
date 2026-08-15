package stream

import (
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// ProjectToCore reduces a stored AG-UI event to the control plane's durable
// event model. It returns false for transient events (byte deltas, snapshots,
// start markers, CUSTOM) — those live only in the bus history window and
// never reach the durable projection. The returned event carries the source
// channel in Source for audit linkage, and the AG-UI payload verbatim in
// Payload.
func ProjectToCore(handle *Handle, stored *StoredEvent) (*agentoscore.Event, bool) {
	if handle == nil || stored == nil {
		return nil, false
	}

	typ, ok := milestoneCoreType(stored.Event.Type)
	if !ok {
		return nil, false
	}

	ev := &stored.Event
	projected := &agentoscore.Event{
		EventType: typ,
		RunID:     ev.RunID,
		ThreadID:  ev.ThreadID,
		Timestamp: stored.StoredAt,
		Source:    handle.Channel,
		Payload:   ev.Payload,
	}

	return projected, true
}

// milestoneCoreType maps a milestone AG-UI type to the control plane's
// durable event type. The control plane persists the same timeline the
// frontend saw, minus the bytes. It is an if-chain rather than a switch so
// the exhaustive guard on IsMilestone stays the single classification point
// for new event types; anything unlisted here is transparently transient.
func milestoneCoreType(typ EventType) (agentoscore.EventType, bool) {
	if typ == EventRunStarted {
		return agentoscore.EventRunStarted, true
	}

	if typ == EventRunFinished {
		return agentoscore.EventRunCompleted, true
	}

	if typ == EventRunError {
		return agentoscore.EventRunFailed, true
	}

	if typ == EventStepStarted {
		return agentoscore.EventAgentStepStarted, true
	}

	if typ == EventStepFinished {
		return agentoscore.EventAgentStepCompleted, true
	}

	if typ == EventTextMessageEnd {
		return agentoscore.EventAgentMessageCompleted, true
	}

	if typ == EventToolCallStart {
		return agentoscore.EventToolCallStarted, true
	}

	if typ == EventToolCallResult {
		return agentoscore.EventToolCallCompleted, true
	}

	if typ == EventToolCallError {
		return agentoscore.EventToolCallFailed, true
	}

	return "", false
}
