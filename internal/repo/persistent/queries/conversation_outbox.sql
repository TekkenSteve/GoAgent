-- Conversation outbox: the durable queue behind the conversation.events
-- domain of the fact log.

-- ClaimConversationOutboxEvents claims a batch and reads the event each
-- claimed row references, plus the thread that owns it. Four decisions are
-- load-bearing here:
--
--   - FOR UPDATE SKIP LOCKED keeps two drainers — two replicas, or the two
--     halves of a rolling deploy — off the same rows while they claim.
--   - Claiming pushes available_at forward by a lease. Without it the row lock
--     dies with the statement, so the next drain pass would re-claim and
--     re-publish rows that are still in flight. A crash mid-publish is still
--     safe: the lease expires and the row is redelivered, which is the
--     at-least-once contract this outbox promises.
--   - Ready rows are ordered by thread and sequence, so a thread's events are
--     claimed in the order they were written.
--   - The thread comes along for the tenant: a fact's envelope says whose it
--     is, and the thread — not the event — is where the outbox's schema keeps
--     that.
--
-- The batch is re-sorted after claiming: RETURNING does not promise to
-- preserve the CTE's ORDER BY, so the order is the caller's guarantee to make
-- rather than the planner's to keep.
-- name: ClaimConversationOutboxEvents :many
WITH ready AS (
    SELECT o.thread_id, o.sequence
    FROM agentos_conversation_event_outbox o
    WHERE o.available_at <= NOW()
    ORDER BY o.available_at, o.thread_id, o.sequence
    LIMIT sqlc.arg(batch_limit)
    FOR UPDATE OF o SKIP LOCKED
)
UPDATE agentos_conversation_event_outbox AS o
SET available_at = NOW() + (sqlc.arg(lease_seconds)::float8 * INTERVAL '1 second')
FROM ready, agentos_conversation_events AS e, agentos_threads AS t
WHERE o.thread_id = ready.thread_id AND o.sequence = ready.sequence
  AND e.thread_id = ready.thread_id AND e.sequence = ready.sequence
  AND t.thread_id = ready.thread_id
RETURNING o.attempts, e.event_id, e.thread_id, e.run_id, e.process_id, e.sequence,
          e.source_event_id, e.source_sequence, e.event_type, e.occurred_at, e.payload,
          t.account_id, t.project_id;

-- MarkConversationOutboxPublished drops the rows whose publish succeeded. The
-- outbox carries no published flag: a row exists exactly while its event is
-- still unpublished, so an outbox that is being drained stays bounded and an
-- empty outbox is the whole delivery state.
-- name: MarkConversationOutboxPublished :exec
DELETE FROM agentos_conversation_event_outbox AS o
USING agentos_conversation_events AS e
WHERE o.thread_id = e.thread_id AND o.sequence = e.sequence
  AND e.event_id = ANY(sqlc.arg(event_ids)::text[]);

-- MarkConversationOutboxFailed records a failed attempt and defers the row by
-- the supplied backoff. Readiness is compared against the database clock, so
-- the deferral is computed from that same clock rather than from a process
-- clock that can drift away from it.
-- name: MarkConversationOutboxFailed :exec
UPDATE agentos_conversation_event_outbox AS o
SET attempts = sqlc.arg(attempts),
    last_error = sqlc.arg(last_error),
    available_at = NOW() + (sqlc.arg(backoff_seconds)::float8 * INTERVAL '1 second')
FROM agentos_conversation_events AS e
WHERE o.thread_id = e.thread_id AND o.sequence = e.sequence
  AND e.event_id = sqlc.arg(event_id);
