package persistent

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/outbox"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
)

const (
	// agentosRunTimelineDomain is the logical log domain carrying the run
	// milestone timeline. Names name content, not mechanism, and the producer
	// applies the deployment prefix, so the broker sees agentos.run.timeline.
	agentosRunTimelineDomain = eventlog.DomainRunTimeline
)

// claimedRunEvent is one claimed outbox row paired with the event it
// references. Claims are handled as pointers: the event they carry is far
// larger than the handle, so a batch sorts and moves by pointer.
type claimedRunEvent struct {
	event     agentoscore.Event
	accountID string
	projectID string
	attempts  int
}

// AgentOSRunOutboxSource is the run timeline's outbox as the backbone drainer
// sees it. It owns the row-to-record mapping: which domain a run fact belongs
// on, which key orders it, and how it is encoded. The claim and the row
// bookkeeping are generated from queries/run_outbox.sql — the claim carries a
// CTE, a SKIP LOCKED row lock and joined RETURNING that no query builder in
// this module's dependency set can express, which is why this domain runs on
// SQL-first generation.
type AgentOSRunOutboxSource struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

var _ outbox.PendingSource = (*AgentOSRunOutboxSource)(nil)

// NewAgentOSRunOutboxSource creates the drainer source over the run event
// outbox.
func NewAgentOSRunOutboxSource(pg *postgres.Postgres) *AgentOSRunOutboxSource {
	return &AgentOSRunOutboxSource{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// ClaimPending claims up to limit ready facts and turns them into records to
// publish.
//
// The handle returned for MarkPublished and MarkFailed is the event's own
// event_id. The outbox is keyed by (run_id, sequence), and the event's globally
// unique ID identifies a queued row without inventing an encoding for that
// composite key — an encoding every later reader would have to decode.
func (s *AgentOSRunOutboxSource) ClaimPending(ctx context.Context, limit int) ([]outbox.Pending, error) {
	claimed, err := s.claimRunEvents(ctx, limit)
	if err != nil {
		return nil, err
	}

	// Ordering is restored here, not trusted to the claim: one run's facts
	// must reach its partition in sequence order, and only a sort inside this
	// process can promise that.
	slices.SortFunc(claimed, compareClaimedRunEvents)

	return runOutboxRecords(claimed)
}

// MarkPublished drops the outbox rows a successful publish covered. There is
// no published flag anywhere: a row exists exactly while its fact is still
// unpublished, so an outbox that is being drained stays bounded and an empty
// outbox is the whole delivery state.
func (s *AgentOSRunOutboxSource) MarkPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	if err := s.queries.MarkRunOutboxPublished(ctx, ids); err != nil {
		return fmt.Errorf("AgentOSRunOutboxSource - MarkPublished - exec: %w", err)
	}

	return nil
}

// MarkFailed records why a publish failed and defers the row by an exponential
// backoff, so a broker outage costs growing pauses instead of a hot loop.
func (s *AgentOSRunOutboxSource) MarkFailed(ctx context.Context, id, cause string, attempts int) error {
	if err := s.queries.MarkRunOutboxFailed(ctx, sqlcgen.MarkRunOutboxFailedParams{
		Attempts:       clampInt32(attempts),
		LastError:      cause,
		BackoffSeconds: outboxBackoff(attempts).Seconds(),
		EventID:        id,
	}); err != nil {
		return fmt.Errorf("AgentOSRunOutboxSource - MarkFailed - exec: %w", err)
	}

	return nil
}

// claimRunEvents claims a batch of ready rows through the generated claim,
// which leases them and reads the fact each one references together with the
// run's registration for the tenant. The lease hides in-flight rows from
// overlapping drainers; its lapse — a drainer that died mid-publish — makes
// the rows claimable again, which is the at-least-once half of the protocol.
func (s *AgentOSRunOutboxSource) claimRunEvents(ctx context.Context, limit int) ([]*claimedRunEvent, error) {
	return claimOutboxBatch(ctx, "AgentOSRunOutboxSource",
		func(ctx context.Context) ([]sqlcgen.ClaimRunOutboxFactsRow, error) {
			return s.queries.ClaimRunOutboxFacts(ctx, sqlcgen.ClaimRunOutboxFactsParams{
				LeaseSeconds: outboxClaimLease.Seconds(),
				BatchLimit:   clampInt32(limit),
			})
		},
		runEventFromClaimRow,
	)
}

// runEventFromClaimRow turns one claimed row into the fact view the encoder
// needs: the domain event reconstructed from its stored columns, the tenant
// the registration join carried in (empty when the run is unregistered — a
// fact is never stranded), and the attempt count for the retry bookkeeping.
func runEventFromClaimRow(row *sqlcgen.ClaimRunOutboxFactsRow) (*claimedRunEvent, error) {
	record := &claimedRunEvent{
		accountID: row.AccountID,
		projectID: row.ProjectID,
		attempts:  int(row.Attempts),
	}

	record.event = agentoscore.Event{
		EventID:   row.EventID,
		EventType: agentoscore.EventType(row.EventType),
		RunID:     row.RunID,
		ThreadID:  row.ThreadID,
		ProcessID: row.ProcessID,
		Source:    row.Source,
		Sequence:  row.Sequence,
		Timestamp: row.OccurredAt,
	}

	if err := json.Unmarshal(row.Payload, &record.event.Payload); err != nil {
		return nil, fmt.Errorf("AgentOSRunOutboxSource - ClaimPending - decode payload: %w", err)
	}

	return record, nil
}

// compareClaimedRunEvents orders a claimed batch by run and then by sequence,
// which is the order a run's facts must be published in. Different runs never
// share a partition, so their relative order is unconstrained and sorting on
// the run first is enough.
func compareClaimedRunEvents(left, right *claimedRunEvent) int {
	if order := cmp.Compare(left.event.RunID, right.event.RunID); order != 0 {
		return order
	}

	return cmp.Compare(left.event.Sequence, right.event.Sequence)
}

// runOutboxRecords wraps claimed facts in the versioned envelope and encodes
// them for the log. The key is the run: sequence is allocated per run, so
// keying by the run is what puts a run's facts in one partition and therefore
// in order.
func runOutboxRecords(claimed []*claimedRunEvent) ([]outbox.Pending, error) {
	return outboxRecords(claimed, agentosRunTimelineDomain, runOutboxFact)
}

// runOutboxFact is one claimed run milestone as a fact: the entity is the run
// whose sequence orders it.
func runOutboxFact(claimed *claimedRunEvent) outboxFact {
	return outboxFact{
		id:         claimed.event.EventID,
		key:        claimed.event.RunID,
		entityKind: eventlog.EntityKindRun,
		eventType:  string(claimed.event.EventType),
		occurredAt: claimed.event.Timestamp,
		tenant:     eventlog.TenantRef{AccountID: claimed.accountID, ProjectID: claimed.projectID},
		attempts:   claimed.attempts,
		body:       claimed.event,
	}
}
