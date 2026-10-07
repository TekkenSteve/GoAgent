package persistent

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/outbox"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
)

const (
	// agentosConversationEventsDomain is the logical log domain carrying durable
	// conversation events. Names name content, not mechanism, and the producer
	// applies the deployment prefix, so the broker sees
	// agentos.conversation.events.
	agentosConversationEventsDomain = eventlog.DomainConversationEvents
)

// claimedConversationEvent is one claimed outbox row paired with the event it
// references. Claims are handled as pointers: the event they carry is far
// larger than the handle, so a batch sorts and moves by pointer.
type claimedConversationEvent struct {
	event     agentos.ConversationEvent
	accountID string
	projectID string
	attempts  int
}

// AgentOSConversationOutboxSource is the conversation domain's outbox as the
// backbone drainer sees it. It owns the row-to-record mapping: which topic a
// conversation event belongs on, which key orders it, and how it is encoded.
// The claim and the row bookkeeping are generated from
// queries/conversation_outbox.sql — the claim carries a CTE, a SKIP LOCKED
// row lock and joined RETURNING that no query builder in this module's
// dependency set can express, which is why this domain runs on SQL-first
// generation.
type AgentOSConversationOutboxSource struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

var _ outbox.PendingSource = (*AgentOSConversationOutboxSource)(nil)

// NewAgentOSConversationOutboxSource creates the drainer source over the
// conversation event outbox.
func NewAgentOSConversationOutboxSource(pg *postgres.Postgres) *AgentOSConversationOutboxSource {
	return &AgentOSConversationOutboxSource{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// ClaimPending claims up to limit ready events and turns them into records to
// publish.
//
// The handle returned for MarkPublished and MarkFailed is the event's own
// event_id. The outbox is keyed by (thread_id, sequence), and the event's
// globally unique ID identifies a queued row without inventing an encoding for
// that composite key — an encoding every later reader would have to decode.
func (s *AgentOSConversationOutboxSource) ClaimPending(ctx context.Context, limit int) ([]outbox.Pending, error) {
	claimed, err := s.claimConversationEvents(ctx, limit)
	if err != nil {
		return nil, err
	}

	// Ordering is restored here, not trusted to the claim: one thread's events
	// must reach its partition in sequence order, and only a sort inside this
	// process can promise that.
	slices.SortFunc(claimed, compareClaimedConversationEvents)

	return conversationOutboxRecords(claimed)
}

// MarkPublished drops the outbox rows a successful publish covered. There is
// no published flag anywhere: a row exists exactly while its fact is still
// unpublished, so an outbox that is being drained stays bounded and an empty
// outbox is the whole delivery state.
func (s *AgentOSConversationOutboxSource) MarkPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	if err := s.queries.MarkConversationOutboxPublished(ctx, ids); err != nil {
		return fmt.Errorf("AgentOSConversationOutboxSource - MarkPublished - exec: %w", err)
	}

	return nil
}

// MarkFailed records why a publish failed and defers the row by an exponential
// backoff, so a broker outage costs growing pauses instead of a hot loop.
func (s *AgentOSConversationOutboxSource) MarkFailed(ctx context.Context, id, cause string, attempts int) error {
	if err := s.queries.MarkConversationOutboxFailed(ctx, sqlcgen.MarkConversationOutboxFailedParams{
		Attempts:       clampInt32(attempts),
		LastError:      cause,
		BackoffSeconds: outboxBackoff(attempts).Seconds(),
		EventID:        id,
	}); err != nil {
		return fmt.Errorf("AgentOSConversationOutboxSource - MarkFailed - exec: %w", err)
	}

	return nil
}

// claimConversationEvents claims a batch of ready rows through the generated
// claim, which leases them and reads the event each one references together
// with the owning thread's tenant. The lease hides in-flight rows from
// overlapping drainers; its lapse — a drainer that died mid-publish — makes
// the rows claimable again, which is the at-least-once half of the protocol.
func (s *AgentOSConversationOutboxSource) claimConversationEvents(ctx context.Context, limit int) ([]*claimedConversationEvent, error) {
	return claimOutboxBatch(ctx, "AgentOSConversationOutboxSource",
		func(ctx context.Context) ([]sqlcgen.ClaimConversationOutboxEventsRow, error) {
			return s.queries.ClaimConversationOutboxEvents(ctx, sqlcgen.ClaimConversationOutboxEventsParams{
				LeaseSeconds: outboxClaimLease.Seconds(),
				BatchLimit:   clampInt32(limit),
			})
		},
		conversationEventFromClaimRow,
	)
}

// conversationEventFromClaimRow turns one claimed row into the fact view the
// encoder needs: the domain event reconstructed from its stored columns — the
// payload comes from agentos_conversation_events, which is what makes the
// outbox a claim check rather than a second copy of the event — and the
// thread's tenant.
func conversationEventFromClaimRow(row *sqlcgen.ClaimConversationOutboxEventsRow) (*claimedConversationEvent, error) {
	record := &claimedConversationEvent{
		accountID: row.AccountID,
		projectID: row.ProjectID,
		attempts:  int(row.Attempts),
	}

	record.event = agentos.ConversationEvent{
		EventID:        row.EventID,
		ThreadID:       row.ThreadID,
		RunID:          row.RunID,
		ProcessID:      row.ProcessID,
		Sequence:       row.Sequence,
		SourceEventID:  row.SourceEventID,
		SourceSequence: row.SourceSequence,
		EventType:      agentoscore.EventType(row.EventType),
		OccurredAt:     row.OccurredAt,
		SchemaVersion:  agentos.ConversationSchemaVersion,
	}

	if err := json.Unmarshal(row.Payload, &record.event.Payload); err != nil {
		// A stored payload that no longer decodes is a fact whose writer and
		// reader disagree on the schema; failing the claim loudly beats
		// publishing a fact whose body is not what its envelope promises.
		return nil, fmt.Errorf("AgentOSConversationOutboxSource - ClaimPending - decode payload: %w", err)
	}

	return record, nil
}

// compareClaimedConversationEvents orders a claimed batch by thread and then by
// sequence, which is the order the claimed thread's events must be published
// in. Different threads never share a partition, so their relative order is
// unconstrained and sorting on the thread first is enough.
func compareClaimedConversationEvents(left, right *claimedConversationEvent) int {
	if order := cmp.Compare(left.event.ThreadID, right.event.ThreadID); order != 0 {
		return order
	}

	return cmp.Compare(left.event.Sequence, right.event.Sequence)
}

// conversationOutboxRecords wraps claimed events in the versioned envelope
// and encodes them for the log. The key is the thread, not the run: sequence
// is allocated per thread, so threading the key is what puts a thread's events
// in one partition and therefore in order.
func conversationOutboxRecords(claimed []*claimedConversationEvent) ([]outbox.Pending, error) {
	return outboxRecords(claimed, agentosConversationEventsDomain, conversationOutboxFact)
}

// conversationOutboxFact is one claimed conversation event as a fact: the
// entity is the thread whose sequence orders it.
func conversationOutboxFact(claimed *claimedConversationEvent) outboxFact {
	return outboxFact{
		id:         claimed.event.EventID,
		key:        claimed.event.ThreadID,
		entityKind: eventlog.EntityKindThread,
		eventType:  string(claimed.event.EventType),
		occurredAt: claimed.event.OccurredAt,
		tenant:     eventlog.TenantRef{AccountID: claimed.accountID, ProjectID: claimed.projectID},
		attempts:   claimed.attempts,
		body:       claimed.event,
	}
}
