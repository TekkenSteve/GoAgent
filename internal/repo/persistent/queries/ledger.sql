-- Append-only generic AgentOS ledger: one JSON document per entry with a
-- tenant-scoped monotonic sequence.

-- AppendLedgerEntry inserts an entry with its tenant-scoped sequence
-- allocated under a per-tenant advisory transaction lock, so concurrent
-- appends serialize on the sequence without a global lock. The document is
-- stamped server-side with the sequence, created_at and (when absent)
-- occurred_at, so what RETURNING hands back is the stored truth.
-- name: AppendLedgerEntry :one
WITH tenant_lock AS (
    SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(account_id) || ':' || sqlc.arg(project_id), 0))
),
next_sequence AS (
    SELECT COALESCE(MAX(ledger_entries.sequence), 0) + 1 AS sequence
    FROM tenant_lock
    LEFT JOIN ledger_entries
        ON ledger_entries.account_id = sqlc.arg(account_id)
       AND ledger_entries.project_id = sqlc.arg(project_id)
)
INSERT INTO ledger_entries (
    entry_id,
    account_id,
    project_id,
    process_id,
    resource_kind,
    resource_id,
    kind,
    sequence,
    idempotency_key,
    entry_json,
    occurred_at
) SELECT sqlc.arg(entry_id), sqlc.arg(account_id), sqlc.arg(project_id), sqlc.arg(process_id),
         sqlc.arg(resource_kind), sqlc.arg(resource_id), sqlc.arg(kind), next_sequence.sequence, sqlc.arg(idempotency_key),
         jsonb_set(
             jsonb_set(
                 jsonb_set(sqlc.arg(entry_json)::jsonb, '{sequence}', to_jsonb(next_sequence.sequence), true),
                 '{created_at}', to_jsonb(COALESCE(sqlc.narg(occurred_at)::timestamptz, NOW())), true
             ),
             '{occurred_at}', to_jsonb(COALESCE(sqlc.narg(occurred_at)::timestamptz, NOW())), true
         ),
         sqlc.narg(occurred_at)::timestamptz
FROM next_sequence
RETURNING entry_json;

-- GetLedgerEntryByIdempotencyKey resolves an entry through its tenant-scoped
-- idempotency key, so a replayed append finds the original.
-- name: GetLedgerEntryByIdempotencyKey :one
SELECT entry_json
FROM ledger_entries
WHERE account_id = $1 AND project_id = $2 AND idempotency_key = $3;

-- GetLedgerEntryByID loads an entry by its ID.
-- name: GetLedgerEntryByID :one
SELECT entry_json
FROM ledger_entries
WHERE entry_id = $1;

-- ListLedgerEntries pages a tenant's entries by sequence ascending, starting
-- after the given sequence. Optional filters are empty-string-means-absent; a
-- NULL row_limit is LIMIT ALL.
-- name: ListLedgerEntries :many
SELECT entry_json
FROM ledger_entries
WHERE account_id = $1
  AND project_id = $2
  AND sequence > $3
  AND (sqlc.arg(process_id)::text = '' OR process_id = sqlc.arg(process_id)::text)
  AND (sqlc.arg(resource_kind)::text = '' OR (resource_kind = sqlc.arg(resource_kind)::text AND resource_id = sqlc.arg(resource_id)))
  AND (sqlc.arg(kind)::text = '' OR kind = sqlc.arg(kind)::text)
ORDER BY sequence ASC
LIMIT sqlc.narg(row_limit)::bigint;
