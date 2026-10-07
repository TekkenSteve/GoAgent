-- RunPlan aggregate persistence: the plans row holds the spec and status
-- documents, plan_nodes holds one status document per node, and plan_events
-- is the append-only, idempotency-keyed event stream. Sequence allocation
-- and the create/append idempotency protocols live in the repository; these
-- statements are the SQL they drive.

-- ListPlanRefs lists plan references matching the scope filters, oldest
-- update first. Optional text filters are empty-string-means-absent; an
-- empty lifecycle state array disables that predicate; a NULL updated_after
-- or row_limit is "no bound".
-- name: ListPlanRefs :many
SELECT plan_id, account_id, project_id
FROM plans
WHERE (sqlc.arg(account_id)::text = '' OR account_id = sqlc.arg(account_id)::text)
  AND (sqlc.arg(project_id)::text = '' OR project_id = sqlc.arg(project_id)::text)
  AND (COALESCE(cardinality(sqlc.arg(lifecycle_states)::text[]), 0) = 0
       OR lifecycle_state = ANY(sqlc.arg(lifecycle_states)::text[]))
  AND (sqlc.narg(updated_after)::timestamptz IS NULL OR updated_at > sqlc.narg(updated_after)::timestamptz)
ORDER BY updated_at ASC, plan_id ASC
LIMIT sqlc.narg(row_limit)::bigint;

-- GetPlanStateByID loads a plan's spec and status documents by id.
-- name: GetPlanStateByID :one
SELECT spec_json, status_json
FROM plans
WHERE plan_id = $1;

-- GetPlanStateByRef loads a plan's spec and status documents by
-- tenant-scoped reference.
-- name: GetPlanStateByRef :one
SELECT spec_json, status_json
FROM plans
WHERE plan_id = $1 AND account_id = $2 AND project_id = $3;

-- GetPlanByIdempotencyKey resolves a plan through its tenant-scoped
-- idempotency key, so a replayed create finds the original.
-- name: GetPlanByIdempotencyKey :one
SELECT spec_json, status_json
FROM plans
WHERE account_id = $1 AND project_id = $2 AND idempotency_key = $3;

-- GetPlanSpecForUpdate takes the plan row lock a state save orders itself
-- against and returns the stored spec for identity validation.
-- name: GetPlanSpecForUpdate :one
SELECT spec_json
FROM plans
WHERE plan_id = $1
FOR UPDATE;

-- UpsertPlan writes a full plan state snapshot. A NULL requested_at is the
-- zero domain time; identity columns (plan_id, idempotency_key, created_at)
-- are never rewritten by the conflict branch.
-- name: UpsertPlan :exec
INSERT INTO plans (
    plan_id,
    thread_id,
    account_id,
    project_id,
    idempotency_key,
    lifecycle_state,
    reason,
    spec_json,
    status_json,
    requested_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,sqlc.narg(requested_at)::timestamptz)
ON CONFLICT (plan_id) DO UPDATE SET
    thread_id = EXCLUDED.thread_id,
    account_id = EXCLUDED.account_id,
    project_id = EXCLUDED.project_id,
    lifecycle_state = EXCLUDED.lifecycle_state,
    reason = EXCLUDED.reason,
    spec_json = EXCLUDED.spec_json,
    status_json = EXCLUDED.status_json,
    requested_at = EXCLUDED.requested_at,
    updated_at = NOW();

-- UpsertPlanNode writes one node status projection under its plan.
-- name: UpsertPlanNode :exec
INSERT INTO plan_nodes (
    plan_id,
    node_id,
    run_id,
    backend_kind,
    backend_name,
    capability,
    lifecycle_state,
    attempts,
    reason,
    status_json
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (plan_id, node_id) DO UPDATE SET
    run_id = EXCLUDED.run_id,
    backend_kind = EXCLUDED.backend_kind,
    backend_name = EXCLUDED.backend_name,
    capability = EXCLUDED.capability,
    lifecycle_state = EXCLUDED.lifecycle_state,
    attempts = EXCLUDED.attempts,
    reason = EXCLUDED.reason,
    status_json = EXCLUDED.status_json,
    updated_at = NOW();

-- DeletePlanNodesByPlanID removes every node of a plan; the caller uses it
-- when a snapshot carries no nodes.
-- name: DeletePlanNodesByPlanID :exec
DELETE FROM plan_nodes
WHERE plan_id = $1;

-- DeletePlanNodesNotIn removes the nodes a snapshot dropped, keeping the
-- ones it still carries.
-- name: DeletePlanNodesNotIn :exec
DELETE FROM plan_nodes
WHERE plan_id = $1 AND NOT (node_id = ANY(sqlc.arg(node_ids)::text[]));

-- ListPlanNodesByPlanID loads one plan's node status documents ordered by
-- node id.
-- name: ListPlanNodesByPlanID :many
SELECT status_json
FROM plan_nodes
WHERE plan_id = $1
ORDER BY node_id ASC;

-- GetPlanTenantScope resolves the tenant a plan belongs to; a missing plan
-- is the caller's not-found case.
-- name: GetPlanTenantScope :one
SELECT account_id, project_id
FROM plans
WHERE plan_id = $1;

-- LockPlanRow takes the plan row lock that serializes a transition save and
-- returns the tenant scope.
-- name: LockPlanRow :one
SELECT account_id, project_id
FROM plans
WHERE plan_id = $1
FOR UPDATE;

-- LockPlanEventSequence takes the plan row lock that serializes event
-- appends and returns the current sequence plus the tenant scope.
-- name: LockPlanEventSequence :one
SELECT event_sequence, account_id, project_id
FROM plans
WHERE plan_id = $1
FOR UPDATE;

-- AdvancePlanEventSequence publishes the sequence number the locked append
-- handed out.
-- name: AdvancePlanEventSequence :exec
UPDATE plans
SET event_sequence = $2
WHERE plan_id = $1;

-- InsertPlanEvent appends one durable plan event and returns the stored
-- event document. A NULL transition_snapshot_json is an append outside a
-- transition.
-- name: InsertPlanEvent :one
INSERT INTO plan_events (
    event_id,
    plan_id,
    account_id,
    project_id,
    node_id,
    run_id,
    event_type,
    sequence,
    idempotency_key,
    transition_snapshot_digest,
    transition_snapshot_json,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
RETURNING event_json;

-- GetPlanEventByIdempotencyKey resolves an event through its tenant-scoped
-- idempotency key, so a replayed append finds the original.
-- name: GetPlanEventByIdempotencyKey :one
SELECT event_json, transition_snapshot_digest
FROM plan_events
WHERE account_id = $1 AND project_id = $2 AND plan_id = $3 AND idempotency_key = $4;

-- ListPlanEvents pages a plan's events by sequence ascending, starting after
-- the given sequence. The optional node and run filters are
-- empty-string-means-absent; a NULL row_limit is LIMIT ALL.
-- name: ListPlanEvents :many
SELECT event_json
FROM plan_events
WHERE plan_id = $1
  AND account_id = $2
  AND project_id = $3
  AND sequence > $4
  AND (sqlc.arg(node_id)::text = '' OR node_id = sqlc.arg(node_id)::text)
  AND (sqlc.arg(run_id)::text = '' OR run_id = sqlc.arg(run_id)::text)
ORDER BY sequence ASC
LIMIT sqlc.narg(row_limit)::bigint;

-- Plan audit logs: one append-only record per control-plane action, identified
-- inside a tenant by its plan and idempotency key. run_id and node_id are
-- nullable — a record either names a durable node/run pair or names neither —
-- and read back as empty strings.

-- GetAuditRecord loads one audit record by its tenant-scoped idempotency key.
-- name: GetAuditRecord :one
SELECT audit_id,
       plan_id,
       account_id,
       project_id,
       COALESCE(run_id, '') AS run_id,
       COALESCE(node_id, '') AS node_id,
       actor_id,
       action,
       idempotency_key,
       payload_json,
       created_at
FROM audit_logs
WHERE plan_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- InsertAuditRecord appends one audit record. A unique collision is the
-- caller's replay case, resolved through the idempotency key against the
-- stored record.
-- name: InsertAuditRecord :exec
INSERT INTO audit_logs (
    audit_id,
    plan_id,
    account_id,
    project_id,
    run_id,
    node_id,
    actor_id,
    action,
    idempotency_key,
    payload_json,
    created_at,
    prev_hash,
    row_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);

-- LatestAuditRowHash returns the tenant's most recent chain hash, which is the
-- predecessor of the next row. Empty when the tenant has no chained row yet.
-- name: LatestAuditRowHash :one
SELECT COALESCE((
    SELECT row_hash FROM audit_logs
    WHERE account_id = $1 AND project_id = $2 AND row_hash <> ''
    ORDER BY created_at DESC, audit_id DESC
    LIMIT 1
), '')::text AS prev_hash;

-- ListAuditChain walks a tenant's audit rows in chain order, which is the only
-- order in which the chain can be verified.
-- name: ListAuditChain :many
SELECT audit_id,
       plan_id,
       account_id,
       project_id,
       COALESCE(run_id, '')::text    AS run_id,
       COALESCE(node_id, '')::text   AS node_id,
       actor_id,
       action,
       idempotency_key,
       payload_json,
       created_at,
       prev_hash,
       row_hash
FROM audit_logs
WHERE account_id = $1 AND project_id = $2
ORDER BY created_at, audit_id;

-- ListAuditRecords lists a plan's audit records oldest-first. The node, run
-- and action filters are empty-string-means-absent; a NULL node/run filter
-- compares against the COALESCEd empty string so absent stays absent; a NULL
-- row_limit is LIMIT ALL.
-- name: ListAuditRecords :many
SELECT audit_id,
       plan_id,
       account_id,
       project_id,
       COALESCE(run_id, '') AS run_id,
       COALESCE(node_id, '') AS node_id,
       actor_id,
       action,
       idempotency_key,
       payload_json,
       created_at
FROM audit_logs
WHERE plan_id = $1
  AND account_id = $2
  AND project_id = $3
  AND (sqlc.arg(node_id)::text = '' OR COALESCE(node_id, '') = sqlc.arg(node_id)::text)
  AND (sqlc.arg(run_id)::text = '' OR COALESCE(run_id, '') = sqlc.arg(run_id)::text)
  AND (sqlc.arg(action)::text = '' OR action = sqlc.arg(action)::text)
ORDER BY created_at ASC, audit_id ASC
LIMIT sqlc.narg(row_limit)::bigint;

-- Plan commands: the durable control-plane outbox. A command enters pending,
-- and delivered is terminal.

-- InsertPlanCommand appends one outbox command. A unique collision is the
-- caller's replay case, resolved through the idempotency key against the
-- stored command.
-- name: InsertPlanCommand :exec
INSERT INTO plan_commands (
    command_id,
    plan_id,
    account_id,
    project_id,
    actor_id,
    action,
    idempotency_key,
    payload_json,
    status,
    failure_reason,
    created_at,
    updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);

-- GetPlanCommand loads one outbox command by its tenant-scoped idempotency
-- key.
-- name: GetPlanCommand :one
SELECT command_id,
       plan_id,
       actor_id,
       action,
       idempotency_key,
       payload_json,
       status,
       failure_reason,
       created_at,
       updated_at,
       account_id,
       project_id
FROM plan_commands
WHERE plan_id = $1 AND account_id = $2 AND project_id = $3 AND idempotency_key = $4;

-- ListRecoverablePlanCommands lists commands in the recoverable states
-- oldest-update-first. An empty status array disables that predicate; the
-- plan/account/project/action filters are empty-string-means-absent; a NULL
-- row_limit is LIMIT ALL.
-- name: ListRecoverablePlanCommands :many
SELECT command_id,
       plan_id,
       actor_id,
       action,
       idempotency_key,
       payload_json,
       status,
       failure_reason,
       created_at,
       updated_at,
       account_id,
       project_id
FROM plan_commands
WHERE (COALESCE(cardinality(sqlc.arg(statuses)::text[]), 0) = 0
       OR status = ANY(sqlc.arg(statuses)::text[]))
  AND (sqlc.arg(plan_id)::text = '' OR plan_id = sqlc.arg(plan_id)::text)
  AND (sqlc.arg(account_id)::text = '' OR account_id = sqlc.arg(account_id)::text)
  AND (sqlc.arg(project_id)::text = '' OR project_id = sqlc.arg(project_id)::text)
  AND (sqlc.arg(action)::text = '' OR action = sqlc.arg(action)::text)
ORDER BY updated_at ASC, command_id ASC
LIMIT sqlc.narg(row_limit)::bigint;

-- UpdatePlanCommandStatus writes a command's lifecycle state and returns the
-- stored row; the tenant-scoped idempotency key identifies it.
-- name: UpdatePlanCommandStatus :one
UPDATE plan_commands
SET status = $1,
    failure_reason = $2,
    updated_at = $3
WHERE plan_id = $4
  AND account_id = $5
  AND project_id = $6
  AND idempotency_key = $7
RETURNING command_id,
          plan_id,
          actor_id,
          action,
          idempotency_key,
          payload_json,
          status,
          failure_reason,
          created_at,
          updated_at,
          account_id,
          project_id;

-- Plan metrics: per-exporter checkpoints a sequence can only move forward,
-- and append-only samples keyed by their full identity.

-- GetPlanMetricCheckpoint loads the checkpoint one exporter stored for a plan.
-- name: GetPlanMetricCheckpoint :one
SELECT exporter_id,
       plan_id,
       account_id,
       project_id,
       sequence,
       projection_json,
       updated_at
FROM plan_metric_checkpoints
WHERE exporter_id = $1 AND plan_id = $2 AND account_id = $3 AND project_id = $4;

-- SavePlanMetricCheckpoint writes a checkpoint, updating only when the stored
-- sequence has not moved past the incoming one. Zero affected rows is the
-- caller's backward-sequence case.
-- name: SavePlanMetricCheckpoint :execrows
INSERT INTO plan_metric_checkpoints (
    exporter_id,
    plan_id,
    account_id,
    project_id,
    sequence,
    projection_json,
    updated_at
) VALUES ($1,$2,$3,$4,$5,$6,NOW())
ON CONFLICT (exporter_id, plan_id) DO UPDATE SET
    account_id = EXCLUDED.account_id,
    project_id = EXCLUDED.project_id,
    sequence = EXCLUDED.sequence,
    projection_json = EXCLUDED.projection_json,
    updated_at = NOW()
WHERE plan_metric_checkpoints.sequence <= EXCLUDED.sequence;

-- RecordPlanMetric appends one metric sample. DO NOTHING keeps the first
-- sample for an identity; zero affected rows means the identity already
-- exists and the caller resolves the collision through the key.
-- name: RecordPlanMetric :execrows
INSERT INTO plan_metric_samples (
    metric_name,
    plan_id,
    account_id,
    project_id,
    node_id,
    run_id,
    event_id,
    sequence,
    value,
    unit,
    labels_json,
    sample_timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT DO NOTHING;

-- GetPlanMetricSampleByKey resolves one metric sample through its full
-- identity, so a DO NOTHING collision can find what was stored.
-- name: GetPlanMetricSampleByKey :one
SELECT metric_name,
       plan_id,
       account_id,
       project_id,
       node_id,
       run_id,
       event_id,
       sequence,
       value,
       unit,
       labels_json,
       sample_timestamp
FROM plan_metric_samples
WHERE metric_name = $1
  AND plan_id = $2
  AND node_id = $3
  AND run_id = $4
  AND event_id = $5
  AND sequence = $6;

-- ValidateAuditOwnership reads a plan node's durable run together with the
-- run's registration in one join: the LEFT JOIN keeps the read working when
-- the run has no ownership row, which is itself the "run not registered" case.
-- name: GetPlanNodeRunOwnership :one
SELECT n.run_id AS node_run_id, r.run_id AS owned_run_id
FROM plan_nodes n
LEFT JOIN run_backend_index r
    ON r.plan_id = n.plan_id
   AND r.node_id = n.node_id
   AND r.run_id = $3
   AND r.account_id = $4
   AND r.project_id = $5
WHERE n.plan_id = $1
  AND n.node_id = $2;
