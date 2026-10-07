-- Backend capability declarations. The declaration itself is the JSON
-- document; the typed columns exist for conflict detection and lookups.

-- UpsertCapability registers a declaration. The conditional DO UPDATE means a
-- conflicting declaration (same backend and name, different idempotency key)
-- updates nothing and returns no row — ErrNoRows to the caller, which turns
-- it into an invalid-plan error naming the conflict.
-- name: UpsertCapability :one
INSERT INTO agentos_capabilities (
    backend_kind, backend_name, capability_name, description,
    capability_json, idempotency_key
)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (backend_kind, backend_name, capability_name) DO UPDATE SET
    description = EXCLUDED.description,
    capability_json = EXCLUDED.capability_json,
    idempotency_key = EXCLUDED.idempotency_key,
    updated_at = NOW()
WHERE agentos_capabilities.idempotency_key = EXCLUDED.idempotency_key
RETURNING capability_json;

-- GetCapabilityByName loads the declaration registered for a backend and
-- capability name.
-- name: GetCapabilityByName :one
SELECT capability_json
FROM agentos_capabilities
WHERE backend_kind = $1 AND backend_name = $2 AND capability_name = $3;

-- GetCapabilityByIdempotencyKey loads a declaration by its registration key,
-- so a replayed registration finds the original.
-- name: GetCapabilityByIdempotencyKey :one
SELECT capability_json
FROM agentos_capabilities
WHERE idempotency_key = $1;
