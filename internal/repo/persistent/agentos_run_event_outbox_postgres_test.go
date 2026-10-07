package persistent

import (
	"encoding/json"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunOutboxRecords locks the record contract a consumer reads: the value is
// the versioned envelope, its entity is the run (so a run's facts share a
// partition), its tenant is the run's registration, the handle is the event's
// own ID, and the fact body inside the envelope is the domain event verbatim —
// which is where a consumer finds the per-run sequence for idempotency.
func TestRunOutboxRecords(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	event := agentoscore.Event{
		EventID:   "evt-run-1",
		EventType: agentoscore.EventRunStarted,
		RunID:     "run-1",
		ThreadID:  "thread-1",
		Sequence:  7,
		Timestamp: occurredAt,
		Source:    "agentos/native",
		Payload:   map[string]any{"message": "hello"},
	}

	claimed := []*claimedRunEvent{{event: event, accountID: "account-1", projectID: "project-1", attempts: 0}}

	pending, err := runOutboxRecords(claimed)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	record := pending[0]

	assert.Equal(t, event.EventID, record.ID, "the handle is the event's own ID")
	assert.Equal(t, agentosRunTimelineDomain, record.Domain)
	assert.Equal(t, event.RunID, record.Key, "the key is the run, so a run's facts share a partition")
	assert.Equal(t, 0, record.Attempts)
	assert.Empty(t, record.Headers, "the envelope carries the contract, not headers duplicating it")

	envelope, err := eventlog.DecodeEnvelope(record.Payload)
	require.NoError(t, err, "the value is an envelope this build can read")

	assert.Equal(t, event.EventID, envelope.EventID, "the envelope carries the fact's identity")
	assert.Equal(t, string(event.EventType), envelope.EventType, "and its type")
	assert.Equal(t, occurredAt, envelope.OccurredAt, "and when it happened")
	assert.Equal(t, eventlog.EntityRef{Kind: eventlog.EntityKindRun, ID: event.RunID}, envelope.Entity,
		"the entity is the run")
	assert.Equal(t, eventlog.TenantRef{AccountID: "account-1", ProjectID: "project-1"}, envelope.Tenant,
		"the tenant is the run's, read from its registration")
	assert.Equal(t, eventlog.EnvelopeSchemaVersion, envelope.Schema, "and the current schema")

	var decoded agentoscore.Event
	require.NoError(t, json.Unmarshal(envelope.Payload, &decoded))

	assert.Equal(t, event, decoded, "the fact body is the domain event verbatim, sequence included")
}

// TestCompareClaimedRunEvents locks the claim order: a run's facts sort by
// sequence, and the run leads the comparison because different runs never
// share a partition.
func TestCompareClaimedRunEvents(t *testing.T) {
	t.Parallel()

	earlier := &claimedRunEvent{event: agentoscore.Event{RunID: "run-1", Sequence: 1}}
	later := &claimedRunEvent{event: agentoscore.Event{RunID: "run-1", Sequence: 2}}
	otherRun := &claimedRunEvent{event: agentoscore.Event{RunID: "run-2", Sequence: 1}}

	assert.Negative(t, compareClaimedRunEvents(earlier, later))
	assert.Positive(t, compareClaimedRunEvents(later, earlier))
	assert.Zero(t, compareClaimedRunEvents(earlier, earlier))
	assert.Negative(t, compareClaimedRunEvents(earlier, otherRun))
}
