package persistent

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompareClaimedConversationEvents locks the batch order the claim must
// publish in: one thread's events contiguous and ascending. A thread's events
// share a partition, so this order is what makes per-thread ordering a property
// of the source rather than of the query plan that happened to claim them.
func TestCompareClaimedConversationEvents(t *testing.T) {
	t.Parallel()

	claimed := []*claimedConversationEvent{
		{event: agentos.ConversationEvent{EventID: "b:2", ThreadID: "thread-b", Sequence: 2}},
		{event: agentos.ConversationEvent{EventID: "a:3", ThreadID: "thread-a", Sequence: 3}},
		{event: agentos.ConversationEvent{EventID: "b:1", ThreadID: "thread-b", Sequence: 1}},
		{event: agentos.ConversationEvent{EventID: "a:1", ThreadID: "thread-a", Sequence: 1}},
		{event: agentos.ConversationEvent{EventID: "a:2", ThreadID: "thread-a", Sequence: 2}},
	}

	slices.SortFunc(claimed, compareClaimedConversationEvents)

	got := make([]string, 0, len(claimed))
	for _, record := range claimed {
		got = append(got, record.event.EventID)
	}

	assert.Equal(t, []string{"a:1", "a:2", "a:3", "b:1", "b:2"}, got)
}

// TestConversationOutboxRecords locks the record contract a consumer reads:
// the value is the versioned envelope, its entity is the thread (so a thread's
// events share a partition), its tenant is the thread's, the handle is the
// event's own ID, and the fact body inside the envelope is the domain event
// verbatim — which is where a consumer finds the per-thread sequence for
// idempotency.
func TestConversationOutboxRecords(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	event := agentos.ConversationEvent{
		SchemaVersion: agentos.ConversationSchemaVersion,
		EventID:       "event-1",
		ThreadID:      "thread-1",
		RunID:         "run-1",
		ProcessID:     "process-1",
		Sequence:      4,
		EventType:     agentos.ConversationEventTextMessageContent,
		OccurredAt:    occurredAt,
		Payload:       map[string]any{"delta": "hi", "message_id": "message-1"},
	}

	claimed := []*claimedConversationEvent{{event: event, accountID: "account-1", projectID: "project-1", attempts: 2}}

	pending, err := conversationOutboxRecords(claimed)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	record := pending[0]

	assert.Equal(t, event.EventID, record.ID, "the handle is the event's own ID")
	assert.Equal(t, agentosConversationEventsDomain, record.Domain)
	assert.Equal(t, event.ThreadID, record.Key, "the key is the thread, so a thread's events share a partition")
	assert.Equal(t, 2, record.Attempts)
	assert.Empty(t, record.Headers, "the envelope carries the contract, not headers duplicating it")

	envelope, err := eventlog.DecodeEnvelope(record.Payload)
	require.NoError(t, err, "the value is an envelope this build can read")

	assert.Equal(t, event.EventID, envelope.EventID, "the envelope carries the fact's identity")
	assert.Equal(t, string(event.EventType), envelope.EventType, "and its type")
	assert.Equal(t, occurredAt, envelope.OccurredAt, "and when it happened")
	assert.Equal(t, eventlog.EntityRef{Kind: eventlog.EntityKindThread, ID: event.ThreadID}, envelope.Entity,
		"the entity is the thread")
	assert.Equal(t, eventlog.TenantRef{AccountID: "account-1", ProjectID: "project-1"}, envelope.Tenant,
		"the tenant is the thread's, read from the thread row")
	assert.Equal(t, eventlog.EnvelopeSchemaVersion, envelope.Schema, "and the current schema")

	var decoded agentos.ConversationEvent
	require.NoError(t, json.Unmarshal(envelope.Payload, &decoded))

	assert.Equal(t, event, decoded, "the fact body is the domain event verbatim, sequence included")
}

// TestConversationOutboxBackoff locks the retry curve: it grows so a broker
// outage does not become a hot loop, and it stops growing so a recovered broker
// does not wait hours for the backlog.
func TestConversationOutboxBackoff(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name     string
		attempts int
		want     time.Duration
	}{
		{name: "no attempt yet", attempts: 0, want: time.Second},
		{name: "first attempt", attempts: 1, want: time.Second},
		{name: "second attempt", attempts: 2, want: 2 * time.Second},
		{name: "third attempt", attempts: 3, want: 4 * time.Second},
		{name: "ninth attempt", attempts: 9, want: 256 * time.Second},
		{name: "capped", attempts: 10, want: outboxRetryCap},
		{name: "capped well past the exponent bound", attempts: 1000, want: outboxRetryCap},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.want, outboxBackoff(testCase.attempts))
		})
	}
}
