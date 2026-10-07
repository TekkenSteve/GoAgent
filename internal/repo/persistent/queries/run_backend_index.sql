-- Run backend ownership: which backend kind and name owns each run, keyed by
-- run id and deduplicated per tenant by idempotency key. The run/event outbox
-- claim joins this table for the tenant, so resolution here is on the
-- publish path too.

-- GetRunBackendIndex loads one ownership row. plan_id and node_id are stored
-- nullable for standalone runs but read as empty strings — the record is a
-- flat ownership fact, and "no plan" is the same fact as "empty plan".
-- name: GetRunBackendIndex :one
SELECT run_id,
       COALESCE(plan_id, '') AS plan_id,
       COALESCE(node_id, '') AS node_id,
       thread_id,
       account_id,
       project_id,
       backend_kind,
       backend_name,
       idempotency_key,
       lifecycle_state,
       created_at,
       updated_at
FROM run_backend_index
WHERE run_id = $1;

-- GetRunBackendIndexByIdempotencyKey resolves an ownership row through the
-- tenant-scoped idempotency key, so a replayed bind finds its original run.
-- name: GetRunBackendIndexByIdempotencyKey :one
SELECT run_id,
       COALESCE(plan_id, '') AS plan_id,
       COALESCE(node_id, '') AS node_id,
       thread_id,
       account_id,
       project_id,
       backend_kind,
       backend_name,
       idempotency_key,
       lifecycle_state,
       created_at,
       updated_at
FROM run_backend_index
WHERE account_id = $1 AND project_id = $2 AND idempotency_key = $3;

-- InsertRunBackendIndex records ownership. DO NOTHING keeps the first bind
-- for a run id; the caller resolves the collision through the idempotency
-- path (zero affected rows means a conflicting bind, not a silent overwrite).
-- name: InsertRunBackendIndex :execrows
INSERT INTO run_backend_index (
    run_id, plan_id, node_id, thread_id, account_id, project_id,
    backend_kind, backend_name, idempotency_key, lifecycle_state
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (run_id) DO NOTHING;

-- GetPlanNodeOwner reads the plan's tenant together with the node's durable
-- state in one join: a node row absent or carrying a different run id is an
-- ownership violation the caller rejects. The LEFT JOIN keeps a plan with no
-- such node readable, which is itself the "node not durable" case.
-- name: GetPlanNodeOwner :one
SELECT p.account_id, p.project_id, n.node_id, n.run_id
FROM plans p
LEFT JOIN plan_nodes n
    ON n.plan_id = p.plan_id
   AND n.node_id = $2
WHERE p.plan_id = $1;

-- UpdateRunBackendLifecycle advances a run's lifecycle state on a bind whose
-- run already exists with the same declaration.
-- name: UpdateRunBackendLifecycle :exec
UPDATE run_backend_index
SET lifecycle_state = $2,
    updated_at = NOW()
WHERE run_id = $1;
