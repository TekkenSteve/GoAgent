-- Batch worksets: spec and status projections with idempotent status
-- updates and chunk results. The spec/status pair is the projection; the
-- workset_status_updates and workset_chunk_results tables are the
-- claim-before-apply ledgers that make updates replayable.

-- InsertWorkset persists a new workset projection and returns the stored
-- status document.
-- name: InsertWorkset :one
INSERT INTO worksets (
    workset_id, account_id, project_id, process_id,
    resource_kind, resource_id, kind, lifecycle_state, idempotency_key,
    spec_json, status_json, requested_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
RETURNING status_json;

-- GetWorksetByID loads a workset's spec and status documents by id.
-- name: GetWorksetByID :one
SELECT spec_json, status_json
FROM worksets
WHERE workset_id = $1;

-- GetWorksetByIdempotencyKey resolves a workset through its tenant-scoped
-- registration key, so a replayed create finds the original.
-- name: GetWorksetByIdempotencyKey :one
SELECT spec_json, status_json
FROM worksets
WHERE account_id = $1 AND project_id = $2 AND idempotency_key = $3;

-- GetWorksetStatusUpdate loads the status a prior idempotent update stored.
-- name: GetWorksetStatusUpdate :one
SELECT status_json
FROM workset_status_updates
WHERE workset_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- GetWorksetChunkStatus loads the status a prior chunk result stored.
-- name: GetWorksetChunkStatus :one
SELECT status_json
FROM workset_chunk_results
WHERE workset_id = $1 AND account_id = $2 AND project_id = $3
  AND chunk_id = $4 AND idempotency_key = $5;

-- ClaimWorksetStatusUpdate inserts an update's claim; ON CONFLICT DO NOTHING
-- plus RETURNING means no row comes back when the key already exists, and
-- the caller then reads the stored status instead.
-- name: ClaimWorksetStatusUpdate :one
INSERT INTO workset_status_updates (workset_id, account_id, project_id, idempotency_key, status_json)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (account_id, project_id, workset_id, idempotency_key) DO NOTHING
RETURNING status_json;

-- ClaimWorksetChunkResult inserts a chunk result's claim, same protocol as
-- the status update claim.
-- name: ClaimWorksetChunkResult :one
INSERT INTO workset_chunk_results (workset_id, account_id, project_id, chunk_id, idempotency_key, result_json, status_json)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (account_id, project_id, workset_id, chunk_id, idempotency_key) DO NOTHING
RETURNING status_json;

-- UpdateWorksetProjection writes the claimed status into the projection.
-- Zero affected rows means the workset vanished mid-update — a tenant-scope
-- error to the caller, never a silent success.
-- name: UpdateWorksetProjection :execrows
UPDATE worksets
SET lifecycle_state = $1, status_json = $2, updated_at = $3
WHERE workset_id = $4 AND account_id = $5 AND project_id = $6;

-- ListWorksets pages workset statuses newest-first inside a tenant. Optional
-- filters are empty-string-means-absent: an empty narg disables the
-- predicate, preserving the caller's filter semantics. A NULL row_limit is
-- LIMIT ALL.
-- name: ListWorksets :many
SELECT status_json
FROM worksets
WHERE account_id = $1
  AND project_id = $2
  AND (sqlc.arg(process_id)::text = '' OR process_id = sqlc.arg(process_id)::text)
  AND (sqlc.arg(resource_kind)::text = '' OR (resource_kind = sqlc.arg(resource_kind)::text AND resource_id = sqlc.arg(resource_id)))
  AND (sqlc.arg(kind)::text = '' OR kind = sqlc.arg(kind)::text)
  AND (sqlc.arg(lifecycle_state)::text = '' OR lifecycle_state = sqlc.arg(lifecycle_state)::text)
ORDER BY updated_at DESC, workset_id ASC
LIMIT sqlc.narg(row_limit)::bigint;
