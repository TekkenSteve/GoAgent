-- Plan artifacts: metadata rows in Postgres, payload bytes in a blob store;
-- the URI column is the pointer between them.

-- InsertArtifact publishes metadata. ON CONFLICT DO NOTHING plus RETURNING
-- means no row comes back on any unique collision — the caller resolves it
-- through the idempotency path, which distinguishes a replay from a
-- conflicting artifact ID.
-- name: InsertArtifact :one
INSERT INTO artifacts (
    artifact_id,
    plan_id,
    account_id,
    project_id,
    node_id,
    run_id,
    name,
    kind,
    media_type,
    uri,
    size_bytes,
    digest,
    metadata_json,
    idempotency_key,
    created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT DO NOTHING
RETURNING artifact_id,
          plan_id,
          COALESCE(node_id, '') AS node_id,
          COALESCE(run_id, '') AS run_id,
          name,
          kind,
          media_type,
          uri,
          size_bytes,
          digest,
          metadata_json,
          created_at;

-- The node/run ownership join lives in queries/plan.sql as
-- GetPlanNodeRunOwnership; the artifact publish path and the audit path
-- check the same durable ownership with that one statement.

-- GetArtifactByIdempotencyKey resolves a publish through its tenant-scoped
-- idempotency key.
-- name: GetArtifactByIdempotencyKey :one
SELECT artifact_id,
       plan_id,
       COALESCE(node_id, '') AS node_id,
       COALESCE(run_id, '') AS run_id,
       name,
       kind,
       media_type,
       uri,
       size_bytes,
       digest,
       metadata_json,
       created_at
FROM artifacts
WHERE plan_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- GetArtifactByID loads one artifact's metadata by ID.
-- name: GetArtifactByID :one
SELECT artifact_id,
       plan_id,
       COALESCE(node_id, '') AS node_id,
       COALESCE(run_id, '') AS run_id,
       name,
       kind,
       media_type,
       uri,
       size_bytes,
       digest,
       metadata_json,
       created_at
FROM artifacts
WHERE artifact_id = $1;

-- GetArtifactByScope loads one artifact's metadata by plan scope. Optional
-- filters are empty-string-means-absent.
-- name: GetArtifactByScope :one
SELECT artifact_id,
       plan_id,
       COALESCE(node_id, '') AS node_id,
       COALESCE(run_id, '') AS run_id,
       name,
       kind,
       media_type,
       uri,
       size_bytes,
       digest,
       metadata_json,
       created_at
FROM artifacts
WHERE plan_id = $1
  AND account_id = $2
  AND project_id = $3
  AND (sqlc.arg(artifact_id)::text = '' OR artifact_id = sqlc.arg(artifact_id)::text)
  AND (sqlc.arg(node_id)::text = '' OR node_id = sqlc.arg(node_id)::text)
  AND (sqlc.arg(run_id)::text = '' OR run_id = sqlc.arg(run_id)::text);

-- ListArtifacts pages a plan scope's artifacts oldest-first. Optional filters
-- are empty-string-means-absent; a NULL row_limit is LIMIT ALL.
-- name: ListArtifacts :many
SELECT artifact_id,
       plan_id,
       COALESCE(node_id, '') AS node_id,
       COALESCE(run_id, '') AS run_id,
       name,
       kind,
       media_type,
       uri,
       size_bytes,
       digest,
       metadata_json,
       created_at
FROM artifacts
WHERE plan_id = $1
  AND account_id = $2
  AND project_id = $3
  AND (sqlc.arg(artifact_id)::text = '' OR artifact_id = sqlc.arg(artifact_id)::text)
  AND (sqlc.arg(node_id)::text = '' OR node_id = sqlc.arg(node_id)::text)
  AND (sqlc.arg(run_id)::text = '' OR run_id = sqlc.arg(run_id)::text)
ORDER BY created_at ASC
LIMIT sqlc.narg(row_limit)::bigint;
