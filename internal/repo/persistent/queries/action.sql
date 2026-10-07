-- Governed actions: spec and status projections with idempotent status
-- updates, the same shape as worksets (see queries/workset.sql).

-- InsertGovernedAction persists a new action projection and returns the
-- stored status document.
-- name: InsertGovernedAction :one
INSERT INTO governed_actions (
    action_id, account_id, project_id, process_id,
    resource_kind, resource_id, kind, lifecycle_state, idempotency_key,
    spec_json, status_json, requested_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
RETURNING status_json;

-- GetGovernedActionByID loads an action's spec and status documents by id.
-- name: GetGovernedActionByID :one
SELECT spec_json, status_json
FROM governed_actions
WHERE action_id = $1;

-- GetGovernedActionByIdempotencyKey resolves an action through its
-- tenant-scoped registration key, so a replayed create finds the original.
-- name: GetGovernedActionByIdempotencyKey :one
SELECT spec_json, status_json
FROM governed_actions
WHERE account_id = $1 AND project_id = $2 AND idempotency_key = $3;

-- GetGovernedActionStatusUpdate loads the status a prior idempotent update
-- stored.
-- name: GetGovernedActionStatusUpdate :one
SELECT status_json
FROM governed_action_status_updates
WHERE action_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- ClaimGovernedActionStatusUpdate inserts an update's claim; ON CONFLICT DO
-- NOTHING plus RETURNING means no row comes back when the key already
-- exists, and the caller then reads the stored status instead.
-- name: ClaimGovernedActionStatusUpdate :one
INSERT INTO governed_action_status_updates (action_id, account_id, project_id, idempotency_key, status_json)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (account_id, project_id, action_id, idempotency_key) DO NOTHING
RETURNING status_json;

-- UpdateGovernedActionProjection writes the claimed status into the
-- projection. Zero affected rows means the action vanished mid-update — a
-- tenant-scope error to the caller, never a silent success.
-- name: UpdateGovernedActionProjection :execrows
UPDATE governed_actions
SET lifecycle_state = $1, status_json = $2, updated_at = $3
WHERE action_id = $4 AND account_id = $5 AND project_id = $6;

-- ListGovernedActions pages action statuses newest-first inside a tenant,
-- with the same empty-string-means-absent filters as ListWorksets.
-- name: ListGovernedActions :many
SELECT status_json
FROM governed_actions
WHERE account_id = $1
  AND project_id = $2
  AND (sqlc.arg(process_id)::text = '' OR process_id = sqlc.arg(process_id)::text)
  AND (sqlc.arg(resource_kind)::text = '' OR (resource_kind = sqlc.arg(resource_kind)::text AND resource_id = sqlc.arg(resource_id)))
  AND (sqlc.arg(kind)::text = '' OR kind = sqlc.arg(kind)::text)
  AND (sqlc.arg(lifecycle_state)::text = '' OR lifecycle_state = sqlc.arg(lifecycle_state)::text)
ORDER BY updated_at DESC, action_id ASC
LIMIT sqlc.narg(row_limit)::bigint;
