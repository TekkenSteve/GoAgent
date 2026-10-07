package eventlog

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	envelope := Envelope{
		EventID:    "evt-1",
		EventType:  "TEXT_MESSAGE_CONTENT",
		OccurredAt: time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC),
		Entity:     EntityRef{Kind: EntityKindThread, ID: "thread-1"},
		Tenant:     TenantRef{AccountID: "account-1", ProjectID: "project-1"},
		Payload:    json.RawMessage(`{"delta":"hi"}`),
	}

	value, err := envelope.Encode()
	require.NoError(t, err, "a complete envelope encodes")

	decoded, err := DecodeEnvelope(value)
	require.NoError(t, err, "and reads back")

	require.Equal(t, envelope, decoded, "the wire form carries the whole fact")
	require.Equal(t, EnvelopeSchemaVersion, decoded.Schema, "Encode stamps the current schema")
}

// TestEnvelopeDecodeIgnoresUnknownFields locks the tolerant reader: a producer
// may extend the envelope within a schema version, and an older reader keeps
// working instead of failing on a field it never knew.
func TestEnvelopeDecodeIgnoresUnknownFields(t *testing.T) {
	t.Parallel()

	value := `{
		"event_id": "evt-1",
		"event_type": "RUN_STARTED",
		"occurred_at": "2026-08-15T12:00:00Z",
		"entity": {"kind": "run", "id": "run-1"},
		"schema": 1,
		"producer_note": "anything a future writer may add"
	}`

	decoded, err := DecodeEnvelope([]byte(value))
	require.NoError(t, err, "an unknown field is not an error")

	require.Equal(t, "run-1", decoded.Entity.ID, "the known fields still arrive")
}

// TestEnvelopeDecodeRefusesUnreadableSchema locks the version policy: an
// envelope without a schema, or from a newer one, stops the read. Guessing at
// a newer contract would turn a producer's upgrade into this reader's
// corruption, so refusal is the only safe behavior.
func TestEnvelopeDecodeRefusesUnreadableSchema(t *testing.T) {
	t.Parallel()

	future, err := DecodeEnvelope([]byte(`{"event_id":"evt-1","event_type":"RUN_STARTED","entity":{"kind":"run","id":"run-1"},"schema":2}`))
	require.ErrorIs(t, err, ErrEnvelopeSchemaFuture)
	require.Empty(t, future.EventID, "a refused envelope decodes to nothing")

	absent, err := DecodeEnvelope([]byte(`{"event_id":"evt-1","event_type":"RUN_STARTED","entity":{"kind":"run","id":"run-1"}}`))
	require.ErrorIs(t, err, ErrEnvelopeSchemaAbsent)
	require.Empty(t, absent.EventID)
}

// TestEnvelopeRefusesMissingIdentity locks what makes a redelivery safe: a
// fact without an identity, a type, or an entity is refused, because a reader
// cannot deduplicate, route, or place it — and improvising any of the three
// would hide the producer's bug behind this reader's guess.
func TestEnvelopeRefusesMissingIdentity(t *testing.T) {
	t.Parallel()

	complete := Envelope{
		EventID:   "evt-1",
		EventType: "RUN_STARTED",
		Entity:    EntityRef{Kind: EntityKindRun, ID: "run-1"},
	}

	stripped := complete

	stripped.EventID = ""

	_, err := stripped.Encode()
	require.ErrorIs(t, err, ErrEnvelopeMissingEventID)

	stripped = complete
	stripped.EventType = ""

	_, err = stripped.Encode()
	require.ErrorIs(t, err, ErrEnvelopeMissingEventType)

	stripped = complete
	stripped.Entity = EntityRef{}

	_, err = stripped.Encode()
	require.ErrorIs(t, err, ErrEnvelopeMissingEntity)

	_, err = DecodeEnvelope([]byte(`{"schema":1,"event_type":"RUN_STARTED","entity":{"kind":"run","id":"run-1"}}`))
	require.ErrorIs(t, err, ErrEnvelopeMissingEventID)

	_, err = DecodeEnvelope([]byte(`{"schema":1,"event_id":"evt-1","entity":{"kind":"run","id":"run-1"}}`))
	require.ErrorIs(t, err, ErrEnvelopeMissingEventType)

	_, err = DecodeEnvelope([]byte(`{"schema":1,"event_id":"evt-1","event_type":"RUN_STARTED"}`))
	require.ErrorIs(t, err, ErrEnvelopeMissingEntity)
}

// TestEnvelopeDecodeReportsMalformedJson keeps the failure honest: broken
// bytes are a decode error, not a skipped fact, so the caller decides whether
// skipping is safe.
func TestEnvelopeDecodeReportsMalformedJson(t *testing.T) {
	t.Parallel()

	_, err := DecodeEnvelope([]byte("{not json"))
	require.Error(t, err)

	var syntax *json.SyntaxError
	require.True(t, errors.As(err, &syntax), "the json failure stays inspectable: %v", err)
}
