-- Run timeline queries: the durable half of the data plane's projection
-- consumption and the run.timeline outbox behind the fact log.

-- InsertRunEvent persists one projected milestone. The bus offset (sequence)
-- IS the idempotency token: the same (run_id, sequence) always carries the
-- same event, so a replay conflict is success rather than an error. :execrows
-- distinguishes a stored row from a replayed one.
-- name: InsertRunEvent :execrows
INSERT INTO agentos_run_events (
    run_id,
    sequence,
    event_id,
    event_type,
    thread_id,
    process_id,
    source,
    occurred_at,
    payload
) VALUES (
    sqlc.arg(run_id),
    sqlc.arg(sequence),
    sqlc.arg(event_id),
    sqlc.arg(event_type),
    sqlc.arg(thread_id),
    sqlc.arg(process_id),
    sqlc.arg(source),
    sqlc.arg(occurred_at),
    sqlc.arg(payload)
)
ON CONFLICT (run_id, sequence) DO NOTHING;

-- EnqueueRunEventOutbox records a stored event's publication intent. The
-- outbox row is a claim check — the event's identity and nothing else — and
-- the conflict clause keeps a replayed append from queueing a fact that is
-- already queued or already published.
-- name: EnqueueRunEventOutbox :exec
INSERT INTO agentos_run_event_outbox (run_id, sequence)
VALUES (sqlc.arg(run_id), sqlc.arg(sequence))
ON CONFLICT (run_id, sequence) DO NOTHING;

-- ClaimRunOutboxFacts claims a batch and reads the fact each claimed row
-- references, plus the run's registration for the tenant. Four decisions are
-- load-bearing here:
--
--   - FOR UPDATE SKIP LOCKED keeps two drainers off the same rows while they
--     claim.
--   - Claiming pushes available_at forward by a lease, so an in-flight row is
--     not re-claimed by the next pass; a crash mid-publish is covered by the
--     lease lapsing — at-least-once, never at-most-once.
--   - Ready rows are ordered by run and sequence, so a run's facts are claimed
--     in the order they were written.
--   - The run's registration comes along for the tenant. The join is a LEFT
--     join on purpose: run_backend_index is the router's registry, not a
--     foreign key of the event, and a fact must not be stranded because the
--     registry was rebuilt — an unregistered run publishes with an empty
--     tenant rather than not publishing.
--
-- The batch is re-sorted after claiming: RETURNING does not promise to
-- preserve the CTE's ORDER BY, so the order is the caller's guarantee to make
-- rather than the planner's to keep.
-- name: ClaimRunOutboxFacts :many
WITH ready AS (
    SELECT o.run_id, o.sequence
    FROM agentos_run_event_outbox o
    WHERE o.available_at <= NOW()
    ORDER BY o.available_at, o.run_id, o.sequence
    LIMIT sqlc.arg(batch_limit)
    FOR UPDATE OF o SKIP LOCKED
)
UPDATE agentos_run_event_outbox AS o
SET available_at = NOW() + (sqlc.arg(lease_seconds)::float8 * INTERVAL '1 second')
FROM ready, agentos_run_events AS e
LEFT JOIN run_backend_index AS b ON b.run_id = e.run_id
WHERE o.run_id = ready.run_id AND o.sequence = ready.sequence
  AND e.run_id = ready.run_id AND e.sequence = ready.sequence
RETURNING o.attempts, e.event_id, e.event_type, e.run_id, e.thread_id, e.process_id,
          e.source, e.sequence, e.occurred_at, e.payload,
          COALESCE(b.account_id, '') AS account_id,
          COALESCE(b.project_id, '') AS project_id;

-- MarkRunOutboxPublished drops the rows whose publish succeeded. There is no
-- published flag: a row exists exactly while its fact is still unpublished.
-- name: MarkRunOutboxPublished :exec
DELETE FROM agentos_run_event_outbox AS o
USING agentos_run_events AS e
WHERE o.run_id = e.run_id AND o.sequence = e.sequence
  AND e.event_id = ANY(sqlc.arg(event_ids)::text[]);

-- MarkRunOutboxFailed records a failed attempt and defers the row by the
-- supplied backoff, computed against the database clock the readiness
-- comparison uses.
-- name: MarkRunOutboxFailed :exec
UPDATE agentos_run_event_outbox AS o
SET attempts = sqlc.arg(attempts),
    last_error = sqlc.arg(last_error),
    available_at = NOW() + (sqlc.arg(backoff_seconds)::float8 * INTERVAL '1 second')
FROM agentos_run_events AS e
WHERE o.run_id = e.run_id AND o.sequence = e.sequence
  AND e.event_id = sqlc.arg(event_id);

-- LastRunEventSequence reads the highest persisted dense ordinal for a run —
-- the bootstrapping cursor a writer's recorder uses to number its first
-- milestone after a process restart, so the durable timeline stays gapless per
-- run. COALESCE makes a new run report 0 (numbering starts at 1).
-- name: LastRunEventSequence :one
SELECT COALESCE(MAX(sequence), 0)::bigint AS last_sequence
FROM agentos_run_events
WHERE run_id = sqlc.arg(run_id);

-- ListRunEvents reads a run's projected milestones in sequence order,
-- resuming after a cursor. This is the run's authoritative history: the same
-- durable timeline the projector wrote, minus the transient byte deltas that
-- only ever live in the bus history window.
-- name: ListRunEvents :many
SELECT event_id, event_type, thread_id, process_id, source, occurred_at, payload, sequence
FROM agentos_run_events
WHERE run_id = sqlc.arg(run_id) AND sequence > sqlc.arg(after_sequence)
ORDER BY sequence
LIMIT sqlc.arg(row_limit);
