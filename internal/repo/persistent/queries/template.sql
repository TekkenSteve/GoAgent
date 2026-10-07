-- Workflow templates: named team specs an account can instantiate.

-- InsertWorkflowTemplate creates a template and returns the stored row.
-- name: InsertWorkflowTemplate :one
INSERT INTO workflow_templates (
    account_id,
    name,
    description,
    team_spec,
    system_prompt,
    default_model,
    tags,
    is_enabled
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
RETURNING id, account_id, name, description, team_spec, system_prompt, default_model, tags, is_enabled, created_at, updated_at;

-- GetWorkflowTemplate loads one template by ID.
-- GetWorkflowTemplate reads a template by id, scoped to its owning account. The
-- account is part of the lookup rather than a check afterwards: a foreign id
-- answers "not found", which is the same answer a nonexistent id gets, so the
-- read cannot even confirm that someone else's template exists.
-- name: GetWorkflowTemplate :one
SELECT id, account_id, name, description, team_spec, system_prompt, default_model, tags, is_enabled, created_at, updated_at
FROM workflow_templates
WHERE id = $1 AND account_id = $2;

-- UpdateWorkflowTemplate applies a partial update: every NULL parameter
-- keeps the stored value, so omitted fields stay untouched. updated_at is
-- stamped server-side.
-- name: UpdateWorkflowTemplate :one
UPDATE workflow_templates
SET name = COALESCE(sqlc.narg(name)::varchar, name),
    description = COALESCE(sqlc.narg(description)::text, description),
    team_spec = COALESCE(sqlc.narg(team_spec)::jsonb, team_spec),
    system_prompt = COALESCE(sqlc.narg(system_prompt)::text, system_prompt),
    default_model = COALESCE(sqlc.narg(default_model)::varchar, default_model),
    tags = COALESCE(sqlc.narg(tags)::text[], tags),
    is_enabled = COALESCE(sqlc.narg(is_enabled)::boolean, is_enabled),
    updated_at = NOW()
WHERE id = sqlc.arg(id) AND account_id = sqlc.arg(account_id)
RETURNING id, account_id, name, description, team_spec, system_prompt, default_model, tags, is_enabled, created_at, updated_at;

-- DeleteWorkflowTemplate removes a template the account owns. A foreign id
-- removes nothing, which the caller reports as not found.
-- name: DeleteWorkflowTemplate :execrows
DELETE FROM workflow_templates
WHERE id = $1 AND account_id = $2;

-- ListWorkflowTemplatesByAccount returns an account's templates newest first.
-- name: ListWorkflowTemplatesByAccount :many
SELECT id, account_id, name, description, team_spec, system_prompt, default_model, tags, is_enabled, created_at, updated_at
FROM workflow_templates
WHERE account_id = $1
ORDER BY created_at DESC;
