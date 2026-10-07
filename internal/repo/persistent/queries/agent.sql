-- Agent definitions and their version snapshots.

-- InsertAgent creates an agent definition. The ID is minted by the database,
-- and a new agent is never the account default — promotion happens through a
-- later update.
-- name: InsertAgent :one
INSERT INTO agents (
    account_id,
    name,
    description,
    system_prompt,
    model_ref,
    config,
    current_version,
    is_default
) VALUES ($1,$2,$3,$4,$5,$6,$7,FALSE)
RETURNING agent_id, account_id, name, description, system_prompt, model_ref, config, current_version, is_default, created_at, updated_at;

-- GetAgent loads one agent by ID.
-- name: GetAgent :one
SELECT agent_id, account_id, name, description, system_prompt, model_ref, config, current_version, is_default, created_at, updated_at
FROM agents
WHERE agent_id = $1;

-- UpdateAgent applies a partial update: every NULL parameter keeps the stored
-- value, so a caller only sends what changed.
-- name: UpdateAgent :one
UPDATE agents
SET name = COALESCE(sqlc.narg(name)::varchar, name),
    description = COALESCE(sqlc.narg(description)::text, description),
    system_prompt = COALESCE(sqlc.narg(system_prompt)::text, system_prompt),
    model_ref = COALESCE(sqlc.narg(model_ref)::varchar, model_ref),
    config = COALESCE(sqlc.narg(config)::jsonb, config),
    is_default = COALESCE(sqlc.narg(is_default)::boolean, is_default),
    current_version = COALESCE(sqlc.narg(current_version)::text, current_version)
WHERE agent_id = sqlc.arg(agent_id)
RETURNING agent_id, account_id, name, description, system_prompt, model_ref, config, current_version, is_default, created_at, updated_at;

-- DeleteAgent removes an agent and, by foreign key, its versions.
-- name: DeleteAgent :exec
DELETE FROM agents
WHERE agent_id = $1;

-- ListAgentsByAccount returns an account's agents newest first.
-- name: ListAgentsByAccount :many
SELECT agent_id, account_id, name, description, system_prompt, model_ref, config, current_version, is_default, created_at, updated_at
FROM agents
WHERE account_id = $1
ORDER BY created_at DESC;

-- InsertAgentVersion stores one version snapshot.
-- name: InsertAgentVersion :exec
INSERT INTO agent_versions (
    version_id,
    agent_id,
    version_name,
    system_prompt,
    model_ref,
    config,
    tool_bindings,
    change_description
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);

-- GetAgentVersion loads one version snapshot by ID.
-- name: GetAgentVersion :one
SELECT version_id, agent_id, version_name, system_prompt, model_ref, config, tool_bindings, change_description, created_at
FROM agent_versions
WHERE version_id = $1;

-- ListAgentVersions returns an agent's snapshots newest first.
-- name: ListAgentVersions :many
SELECT version_id, agent_id, version_name, system_prompt, model_ref, config, tool_bindings, change_description, created_at
FROM agent_versions
WHERE agent_id = $1
ORDER BY created_at DESC;
