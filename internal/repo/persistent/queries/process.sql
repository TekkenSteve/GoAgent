-- Generic AgentOS process projections: one row per process holding its spec
-- and status documents, plus the append-only event log and the
-- idempotency-keyed status updates.

-- GetProcessByRef loads a process by tenant-scoped reference.
-- name: GetProcessByRef :one
SELECT spec_json, status_json
FROM processes
WHERE process_id = $1 AND account_id = $2 AND project_id = $3;

-- GetProcessByIdempotencyKey resolves a process through its tenant-scoped
-- idempotency key, so a replayed create finds the original.
-- name: GetProcessByIdempotencyKey :one
SELECT spec_json, status_json
FROM processes
WHERE account_id = $1 AND project_id = $2 AND idempotency_key = $3;

-- ListProcesses pages a tenant's processes newest-updated first. Optional
-- filters are empty-string-means-absent; a NULL row_limit is LIMIT ALL.
-- name: ListProcesses :many
SELECT status_json
FROM processes
WHERE account_id = $1
  AND project_id = $2
  AND (sqlc.arg(resource_kind)::text = '' OR (resource_kind = sqlc.arg(resource_kind)::text AND resource_id = sqlc.arg(resource_id)))
  AND (sqlc.arg(kind_filter)::text = '' OR kind = sqlc.arg(kind_filter)::text)
  AND (sqlc.arg(lifecycle_state)::text = '' OR lifecycle_state = sqlc.arg(lifecycle_state)::text)
ORDER BY updated_at DESC, process_id ASC
LIMIT sqlc.narg(row_limit)::bigint;

-- InsertProcess stores a new process projection.
-- name: InsertProcess :exec
INSERT INTO processes (
    process_id,
    kind,
    account_id,
    project_id,
    resource_kind,
    resource_id,
    idempotency_key,
    lifecycle_state,
    reason,
    spec_json,
    status_json,
    requested_at,
    updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);

-- UpdateProcessProjection writes a status update's projection. The row count
-- tells the caller whether the process still exists.
-- name: UpdateProcessProjection :execrows
UPDATE processes
SET lifecycle_state = $4,
    reason = $5,
    status_json = $6,
    updated_at = $7
WHERE process_id = $1 AND account_id = $2 AND project_id = $3;

-- LockProcessEventScope takes the process row lock that serializes event
-- appends for one process, returning the scope and current sequence.
-- name: LockProcessEventScope :one
SELECT process_id, account_id, project_id, resource_kind, resource_id, event_sequence
FROM processes
WHERE process_id = $1 AND account_id = $2 AND project_id = $3
FOR UPDATE;

-- AdvanceProcessEventSequence publishes the next sequence number the locked
-- scope handed out.
-- name: AdvanceProcessEventSequence :exec
UPDATE processes
SET event_sequence = $4
WHERE process_id = $1 AND account_id = $2 AND project_id = $3;

-- InsertProcessEvent appends one process event.
-- name: InsertProcessEvent :exec
INSERT INTO process_events (
    event_id,
    process_id,
    account_id,
    project_id,
    resource_kind,
    resource_id,
    event_type,
    sequence,
    idempotency_key,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);

-- GetProcessEventByIdempotencyKey resolves an event through its
-- tenant-scoped idempotency key.
-- name: GetProcessEventByIdempotencyKey :one
SELECT event_json
FROM process_events
WHERE process_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- ListProcessEvents pages a process's events by sequence ascending, starting
-- after the given sequence. A NULL row_limit is LIMIT ALL.
-- name: ListProcessEvents :many
SELECT event_json
FROM process_events
WHERE process_id = $1
  AND account_id = $2
  AND project_id = $3
  AND sequence > $4
ORDER BY sequence ASC
LIMIT sqlc.narg(row_limit)::bigint;

-- GetProcessStatusByIdempotencyKey resolves a status update through its
-- tenant-scoped idempotency key.
-- name: GetProcessStatusByIdempotencyKey :one
SELECT status_json
FROM process_status_updates
WHERE process_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- InsertProcessStatusUpdate records that a status update was applied.
-- name: InsertProcessStatusUpdate :exec
INSERT INTO process_status_updates (
    process_id,
    account_id,
    project_id,
    idempotency_key,
    status_json
) VALUES ($1,$2,$3,$4,$5);
