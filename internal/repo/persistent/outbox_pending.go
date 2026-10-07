package persistent

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/repo/outbox"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
)

// What every domain outbox in this package shares once its SQL lives in
// queries/*.sql: the retry time policy (lease, backoff curve) and the
// row-to-record shaping (fact view, versioned envelope). The statements and
// bindings are generated per domain; these are the backbone's own constants
// and encodings.

const (
	// outboxClaimLease bounds how long a claimed row stays hidden from the
	// other drainers. It only has to outlast one publish, so it is generous
	// next to a broker round trip.
	outboxClaimLease = 30 * time.Second

	// outboxRetryBase and outboxRetryCap bound the retry backoff: backing off
	// keeps a broker outage from becoming a hot loop, and the cap keeps a
	// recovered broker from waiting hours for the backlog.
	outboxRetryBase = time.Second
	outboxRetryCap  = 5 * time.Minute

	// outboxRetryMaxShift bounds the backoff exponent so the shift cannot
	// overflow a duration before the cap applies.
	outboxRetryMaxShift = 20
)

// outboxBackoff returns how long a fact that just failed its attempt-th
// attempt stays claimable. The exponent is bounded before the shift so a
// large attempt count cannot overflow the duration.
func outboxBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	backoff := outboxRetryBase << min(attempts-1, outboxRetryMaxShift)
	if backoff > outboxRetryCap || backoff <= 0 {
		return outboxRetryCap
	}

	return backoff
}

// clampInt32 narrows an int from drainer configuration into the binding's
// int32 without wrapping: a misconfigured value surfaces as saturation, not
// as a negative batch size or attempt count.
func clampInt32(value int) int32 {
	return int32(min(max(value, 0), math.MaxInt32))
}

// claimOutboxBatch runs a domain's generated claim and decodes each returned
// row through the domain's mapper. The claim statement is the domain's; the
// lease, the batch clamp and the error paths are the backbone's, which is why
// they live here once instead of in every source.
func claimOutboxBatch[Row, Record any](
	ctx context.Context,
	caller string,
	claim func(ctx context.Context) ([]Row, error),
	decode func(*Row) (Record, error),
) ([]Record, error) {
	rows, err := claim(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s - ClaimPending - claim query: %w", caller, err)
	}

	records := make([]Record, 0, len(rows))

	for i := range rows {
		record, err := decode(&rows[i])
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

// outboxFact is one claimed fact in the shape the shared encoder needs: every
// domain difference stops at this struct.
type outboxFact struct {
	id         string
	key        string
	entityKind string
	eventType  string
	occurredAt time.Time
	tenant     eventlog.TenantRef
	attempts   int
	body       any
}

// outboxRecords maps one domain's claimed rows to facts and encodes them.
// What varies per domain is exactly the mapping: which entity the fact is
// about, which of its fields the sequence is allocated within, when it
// happened, and its event type.
func outboxRecords[T any](claimed []T, domain string, fact func(T) outboxFact) ([]outbox.Pending, error) {
	facts := make([]outboxFact, 0, len(claimed))

	for i := range claimed {
		facts = append(facts, fact(claimed[i]))
	}

	return outboxPending(facts, domain)
}

// outboxPending wraps facts in the versioned envelope and encodes them for the
// log. The envelope carries what every consumer needs — identity, type,
// entity, tenant, schema — while each fact's body is the domain event
// verbatim: a consumer decodes it with the domain types and finds the
// per-entity sequence there, which is the (entity, sequence) idempotency key.
func outboxPending(facts []outboxFact, domain string) ([]outbox.Pending, error) {
	pending := make([]outbox.Pending, 0, len(facts))

	for i := range facts {
		body, err := json.Marshal(facts[i].body)
		if err != nil {
			return nil, fmt.Errorf("persistent - outbox - encode event %s: %w", facts[i].id, err)
		}

		envelope := eventlog.Envelope{
			EventID:    facts[i].id,
			EventType:  facts[i].eventType,
			OccurredAt: facts[i].occurredAt,
			Entity:     eventlog.EntityRef{Kind: facts[i].entityKind, ID: facts[i].key},
			Tenant:     facts[i].tenant,
			Payload:    body,
		}

		value, err := envelope.Encode()
		if err != nil {
			return nil, fmt.Errorf("persistent - outbox - encode envelope %s: %w", facts[i].id, err)
		}

		pending = append(pending, outbox.Pending{
			ID:       facts[i].id,
			Domain:   domain,
			Key:      facts[i].key,
			Payload:  value,
			Attempts: facts[i].attempts,
		})
	}

	return pending, nil
}
