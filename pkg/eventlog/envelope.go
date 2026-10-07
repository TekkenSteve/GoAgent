package eventlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// EnvelopeSchemaVersion is the wire version of Envelope. A reader accepts any
// envelope up to this version and refuses anything newer: guessing at a newer
// contract's meaning would turn a producer's upgrade into this consumer's
// corruption, so an unreadable schema stops the read instead.
const EnvelopeSchemaVersion = 1

// Entity kinds of the backbone. An entity kind names what the entity *is* —
// the thing whose facts are ordered in one shard and anchored to one workflow —
// not the domain that happens to carry it.
const (
	EntityKindRun    = "run"
	EntityKindPlan   = "plan"
	EntityKindThread = "thread"
)

// Errors a producer or a reader of envelopes can act on. They are sentinels so
// a consumer can tell "this envelope is not readable" from "this record is not
// for me", and a pipeline can refuse rather than improvise.
var (
	// ErrEnvelopeSchemaFuture reports an envelope from a newer contract.
	ErrEnvelopeSchemaFuture = errors.New("eventlog: envelope schema is newer than this reader")
	// ErrEnvelopeSchemaAbsent reports an envelope that carries no schema.
	ErrEnvelopeSchemaAbsent = errors.New("eventlog: envelope carries no schema version")
	// ErrEnvelopeMissingEventID reports a fact without an identity: a
	// redelivery of it could never be recognized, so it is refused rather than
	// deduplicated under a token this reader invented.
	ErrEnvelopeMissingEventID = errors.New("eventlog: envelope carries no event id")
	// ErrEnvelopeMissingEventType reports a fact that does not say what it is.
	// A reader cannot route it, and guessing would turn a producer's bug into
	// this consumer's.
	ErrEnvelopeMissingEventType = errors.New("eventlog: envelope carries no event type")
	// ErrEnvelopeMissingEntity reports a fact that names no entity. The entity
	// is the workflow anchor and the ordering key, so a fact without one cannot
	// be placed anywhere.
	ErrEnvelopeMissingEntity = errors.New("eventlog: envelope carries no entity")
)

// EntityRef is what a fact is about: its kind and the entity's own id. The id
// is the record's Key — it selects the shard, closes the ordering loop, and
// anchors the workflow a cross-town reader starts or signals.
type EntityRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// TenantRef is whose fact it is. Tenants travel per fact rather than per
// deployment so a mirrored log never has to be re-interpreted to know who owns
// a record.
type TenantRef struct {
	AccountID string `json:"account_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

// Envelope is the versioned wire form of a fact on the log — the one contract
// every producer and every consumer shares, across domains and across towns.
//
// The envelope carries the identity and the routing facts about a record —
// what happened, when, to which entity, for which tenant — while Payload holds
// the fact's own body, the domain event verbatim. A consumer that only routes
// or filters never decodes the payload; a domain consumer decodes the payload
// with its own types and finds the per-entity sequence there for idempotency.
//
// Publishing a record whose value is not an envelope breaks every reader that
// is not its author, so Record.Value is always an encoded Envelope.
type Envelope struct {
	// EventID is the fact's identity and the idempotency token: producers
	// sign it, transports deduplicate on it, and consumers recognize a
	// redelivery by it. It is stable across replays and across towns.
	EventID string `json:"event_id"`

	// EventType names what happened, in the producing domain's vocabulary.
	EventType string `json:"event_type"`

	// OccurredAt is when the fact happened at its source.
	OccurredAt time.Time `json:"occurred_at"`

	// Entity is what the fact is about. Its ID is the record key.
	Entity EntityRef `json:"entity"`

	// Tenant is who the fact belongs to, when the domain knows it.
	Tenant TenantRef `json:"tenant"`

	// Payload is the fact's body: the domain event as the producer stores it.
	// Records too large for the log put a storage reference here instead; the
	// envelope itself never grows with the payload.
	Payload json.RawMessage `json:"payload,omitempty"`

	// TraceID carries the producer's trace, when there is one, so a fact can
	// be tied back to the run that produced it.
	TraceID string `json:"trace_id,omitempty"`

	// Schema is the wire version this envelope is written against.
	Schema int `json:"schema"`
}

// Encode validates the envelope and returns its wire form, stamping the
// current schema version: writers always write the version they were built
// with, so a version can never be mistyped into the log.
func (e *Envelope) Encode() ([]byte, error) {
	e.Schema = EnvelopeSchemaVersion

	switch {
	case e.EventID == "":
		return nil, ErrEnvelopeMissingEventID
	case e.EventType == "":
		return nil, ErrEnvelopeMissingEventType
	case e.Entity.ID == "" || e.Entity.Kind == "":
		return nil, ErrEnvelopeMissingEntity
	}

	value, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("eventlog: encode envelope: %w", err)
	}

	return value, nil
}

// DecodeEnvelope reads the wire form. It is a tolerant reader: fields it does
// not know are ignored, so a producer may extend the envelope within a schema
// version. A schema it cannot read — absent, or newer than this build — is
// refused rather than guessed at, and so is an envelope without identity.
func DecodeEnvelope(value []byte) (Envelope, error) {
	var envelope Envelope

	if err := json.Unmarshal(value, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("eventlog: decode envelope: %w", err)
	}

	switch {
	case envelope.Schema == 0:
		return Envelope{}, ErrEnvelopeSchemaAbsent
	case envelope.Schema > EnvelopeSchemaVersion:
		return Envelope{}, ErrEnvelopeSchemaFuture
	case envelope.EventID == "":
		return Envelope{}, ErrEnvelopeMissingEventID
	case envelope.EventType == "":
		return Envelope{}, ErrEnvelopeMissingEventType
	case envelope.Entity.ID == "" || envelope.Entity.Kind == "":
		return Envelope{}, ErrEnvelopeMissingEntity
	}

	return envelope, nil
}
