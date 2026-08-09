package persistent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	artifactblob "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const postgresUniqueViolation = "23505"

// AgentOSArtifactRepo persists artifact metadata in Postgres and payload bytes
// in a blob store.
type AgentOSArtifactRepo struct {
	*postgres.Postgres
	blob artifactblob.BlobStore
}

// NewAgentOSArtifactRepo creates a production AgentOS artifact store.
func NewAgentOSArtifactRepo(pg *postgres.Postgres, blob artifactblob.BlobStore) *AgentOSArtifactRepo {
	return &AgentOSArtifactRepo{Postgres: pg, blob: blob}
}

// Put publishes an artifact under the given reference, storing payload bytes in the blob store and metadata in Postgres.
func (r *AgentOSArtifactRepo) Put(ctx context.Context, artifact *agentoscore.ArtifactRef, payload any, idempotencyKey string) (agentoscore.ArtifactRef, error) {
	ref := *artifact

	encodedPayload, encodeErr := encodeArtifactPayload(payload, &ref)
	if encodeErr != nil {
		return agentoscore.ArtifactRef{}, encodeErr
	}

	scope, existing, exists, err := r.prepareArtifactPut(ctx, &ref, payload, idempotencyKey)
	if err != nil {
		return agentoscore.ArtifactRef{}, err
	}

	if exists {
		return existing, nil
	}

	stored, err := r.insertArtifact(ctx, &ref, scope, idempotencyKey, encodedPayload)
	if err != nil {
		return agentoscore.ArtifactRef{}, err
	}

	if err := agentosplan.ValidateArtifactPublishIdempotency(&stored, &ref); err != nil {
		return agentoscore.ArtifactRef{}, err
	}

	return stored, nil
}

func (r *AgentOSArtifactRepo) prepareArtifactPut(ctx context.Context, ref *agentoscore.ArtifactRef, payload any, idempotencyKey string) (planTenantScope, agentoscore.ArtifactRef, bool, error) {
	if err := validateArtifactCreateInput(ref, idempotencyKey); err != nil {
		return planTenantScope{}, agentoscore.ArtifactRef{}, false, err
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, ref.PlanID)
	if err != nil {
		return planTenantScope{}, agentoscore.ArtifactRef{}, false, err
	}

	if !exists {
		return planTenantScope{}, agentoscore.ArtifactRef{}, false, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, ref.PlanID)
	}

	if err := r.validateArtifactOwnership(ctx, ref, scope); err != nil {
		return planTenantScope{}, agentoscore.ArtifactRef{}, false, err
	}

	setArtifactRefDefaults(ref, idempotencyKey)

	existing, exists, err := r.existingArtifactPublish(ctx, ref, scope, idempotencyKey)
	if err != nil || exists {
		return scope, existing, exists, err
	}

	if err := validateNewArtifactPublishPayload(ref, payload); err != nil {
		return planTenantScope{}, agentoscore.ArtifactRef{}, false, err
	}

	if err := r.validateArtifactIDAvailable(ctx, ref); err != nil {
		return planTenantScope{}, agentoscore.ArtifactRef{}, false, err
	}

	return scope, agentoscore.ArtifactRef{}, false, nil
}

func validateNewArtifactPublishPayload(ref *agentoscore.ArtifactRef, payload any) error {
	if payload != nil {
		return nil
	}

	return agentosplan.ValidateNewRefOnlyArtifactPublish(ref)
}

func (r *AgentOSArtifactRepo) validateArtifactIDAvailable(ctx context.Context, ref *agentoscore.ArtifactRef) error {
	existing, exists, err := r.artifactByID(ctx, ref.ArtifactID)
	if err != nil {
		return err
	}

	if exists {
		return artifactIDConflictError(&existing, ref.ArtifactID)
	}

	return nil
}

func (r *AgentOSArtifactRepo) insertArtifact(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope, idempotencyKey string, encodedPayload []byte) (agentoscore.ArtifactRef, error) {
	if err := r.storePayloadForArtifact(ctx, ref, encodedPayload); err != nil {
		return agentoscore.ArtifactRef{}, err
	}

	metadataJSON, err := json.Marshal(ref.Metadata)
	if err != nil {
		return agentoscore.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - Put - marshal metadata: %w", err)
	}

	row := r.insertArtifactRow(ctx, ref, scope, idempotencyKey, metadataJSON)

	stored, err := scanArtifactRef(row)
	if err != nil {
		return r.resolveArtifactPutConflict(ctx, ref, scope, idempotencyKey, err)
	}

	return stored, nil
}

func validateArtifactCreateInput(ref *agentoscore.ArtifactRef, idempotencyKey string) error {
	if idempotencyKey == "" {
		return fmt.Errorf("%w: artifact idempotency key is required", agentoscore.ErrInvalidArtifact)
	}

	if ref.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidArtifact)
	}

	if ref.Name == "" {
		return fmt.Errorf("%w: artifact name is required", agentoscore.ErrInvalidArtifact)
	}

	if ref.Kind == "" {
		return fmt.Errorf("%w: artifact kind is required", agentoscore.ErrInvalidArtifact)
	}

	return nil
}

func setArtifactRefDefaults(ref *agentoscore.ArtifactRef, idempotencyKey string) {
	if ref.ArtifactID == "" {
		ref.ArtifactID = agentosplan.ArtifactIDFromRef(ref.PlanID, idempotencyKey)
	}

	if ref.CreatedAt.IsZero() {
		ref.CreatedAt = time.Now().UTC()
	}
}

func encodeArtifactPayload(payload any, ref *agentoscore.ArtifactRef) ([]byte, error) {
	if payload == nil {
		return nil, nil
	}

	encoded, mediaType, err := agentosplan.EncodeArtifactPayload(payload, ref.MediaType)
	if err != nil {
		return nil, err
	}

	ref.MediaType = mediaType
	ref.SizeBytes = int64(len(encoded))
	ref.Digest = agentosplan.DigestArtifactPayload(encoded)

	return encoded, nil
}

func (r *AgentOSArtifactRepo) storePayloadForArtifact(ctx context.Context, ref *agentoscore.ArtifactRef, encodedPayload []byte) error {
	if encodedPayload == nil {
		return nil
	}

	if r.blob == nil {
		return fmt.Errorf("%w: blob store is required for artifact payload", agentoscore.ErrInvalidArtifact)
	}

	object, err := r.blob.Put(ctx, artifactBlobKey(ref.ArtifactID, ref.Digest), encodedPayload)
	if err != nil {
		return err
	}

	ref.URI = object.URI
	ref.SizeBytes = object.SizeBytes
	ref.Digest = object.Digest

	return nil
}

func (r *AgentOSArtifactRepo) insertArtifactRow(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope, idempotencyKey string, metadataJSON []byte) pgx.Row {
	return r.Pool.QueryRow(
		ctx, `
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
RETURNING `+strings.Join(artifactColumns(), ", "),
		ref.ArtifactID,
		ref.PlanID,
		scope.AccountID,
		scope.ProjectID,
		nullableString(ref.NodeID),
		nullableString(ref.RunID),
		ref.Name,
		string(ref.Kind),
		ref.MediaType,
		ref.URI,
		ref.SizeBytes,
		ref.Digest,
		metadataJSON,
		idempotencyKey,
		ref.CreatedAt,
	)
}

func (r *AgentOSArtifactRepo) resolveArtifactPutConflict(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope, idempotencyKey string, err error) (agentoscore.ArtifactRef, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return r.resolveArtifactNoRowsConflict(ctx, ref, scope, idempotencyKey)
	}

	if isPostgresUniqueViolation(err) {
		return r.resolveArtifactUniqueConflict(ctx, ref, scope, idempotencyKey, err)
	}

	return agentoscore.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - Put - insert: %w", err)
}

func (r *AgentOSArtifactRepo) resolveArtifactNoRowsConflict(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope, idempotencyKey string) (agentoscore.ArtifactRef, error) {
	existing, exists, err := r.existingArtifactPublish(ctx, ref, scope, idempotencyKey)
	if err != nil || exists {
		return existing, err
	}

	return agentoscore.ArtifactRef{}, artifactIDConflictError(&agentoscore.ArtifactRef{}, ref.ArtifactID)
}

func (r *AgentOSArtifactRepo) resolveArtifactUniqueConflict(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope, idempotencyKey string, err error) (agentoscore.ArtifactRef, error) {
	existing, exists, lookupErr := r.existingArtifactPublish(ctx, ref, scope, idempotencyKey)
	if lookupErr != nil || exists {
		return existing, lookupErr
	}

	return agentoscore.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - Put - insert: %w", err)
}

func (r *AgentOSArtifactRepo) existingArtifactPublish(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope, idempotencyKey string) (agentoscore.ArtifactRef, bool, error) {
	existing, exists, err := r.artifactByIdempotencyKey(ctx, ref.PlanID, scope, idempotencyKey)
	if err != nil || !exists {
		return agentoscore.ArtifactRef{}, false, err
	}

	if err := agentosplan.ValidateArtifactPublishIdempotency(&existing, ref); err != nil {
		return agentoscore.ArtifactRef{}, false, err
	}

	return existing, true, nil
}

// Get loads an artifact by scope, returning its reference and decoded payload.
func (r *AgentOSArtifactRepo) Get(ctx context.Context, scope *agentos.PlanArtifactScope) (agentoscore.ArtifactRef, any, error) {
	if err := agentosplan.ValidatePlanArtifactScope(scope); err != nil {
		return agentoscore.ArtifactRef{}, nil, err
	}

	if scope.ArtifactID == "" {
		return agentoscore.ArtifactRef{}, nil, fmt.Errorf("%w: artifact id is required", agentoscore.ErrInvalidArtifact)
	}

	ref, exists, err := r.getRef(ctx, artifactScopeWhere(scope))
	if err != nil || !exists {
		if !exists {
			return agentoscore.ArtifactRef{}, nil, fmt.Errorf("%w: %s", agentoscore.ErrArtifactNotFound, scope.ArtifactID)
		}

		return agentoscore.ArtifactRef{}, nil, err
	}

	if ref.URI == "" {
		return ref, nil, nil
	}

	if r.blob == nil {
		return agentoscore.ArtifactRef{}, nil, fmt.Errorf("%w: blob store is required for artifact payload", agentoscore.ErrInvalidArtifact)
	}

	data, err := r.blob.Get(ctx, ref.URI)
	if err != nil {
		return agentoscore.ArtifactRef{}, nil, err
	}

	payload, err := decodeStoredArtifactPayload(&ref, data)
	if err != nil {
		return agentoscore.ArtifactRef{}, nil, err
	}

	return ref, payload, nil
}

func decodeStoredArtifactPayload(ref *agentoscore.ArtifactRef, data []byte) (any, error) {
	if ref.SizeBytes != int64(len(data)) {
		return nil, fmt.Errorf("%w: artifact %q blob size %d does not match metadata size %d", agentoscore.ErrInvalidArtifact, ref.ArtifactID, len(data), ref.SizeBytes)
	}

	digest := agentosplan.DigestArtifactPayload(data)
	if ref.Digest != digest {
		return nil, fmt.Errorf("%w: artifact %q blob digest %q does not match metadata digest %q", agentoscore.ErrInvalidArtifact, ref.ArtifactID, digest, ref.Digest)
	}

	return agentosplan.DecodeArtifactPayload(data, ref.MediaType)
}

// List returns artifact references matching the given plan artifact scope.
func (r *AgentOSArtifactRepo) List(ctx context.Context, scope *agentos.PlanArtifactScope) ([]agentoscore.ArtifactRef, error) {
	if err := agentosplan.ValidatePlanArtifactScope(scope); err != nil {
		return nil, err
	}

	builder := r.Builder.
		Select(artifactColumns()...).
		From("artifacts").
		OrderBy("created_at ASC").
		Where(artifactScopeWhere(scope))
	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSArtifactRepo - List - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSArtifactRepo - List - query: %w", err)
	}
	defer rows.Close()

	var refs []agentoscore.ArtifactRef

	for rows.Next() {
		ref, err := scanArtifactRef(rows)
		if err != nil {
			return nil, err
		}

		refs = append(refs, ref)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSArtifactRepo - List - rows: %w", err)
	}

	return refs, nil
}

func (r *AgentOSArtifactRepo) validateArtifactOwnership(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope) error {
	if ref.NodeID == "" && ref.RunID == "" {
		return nil
	}

	if ref.NodeID == "" || ref.RunID == "" {
		return fmt.Errorf("%w: artifact node id and run id must be provided together", agentoscore.ErrInvalidArtifact)
	}

	var (
		nodeRunID  string
		ownedRunID sql.NullString
	)

	err := r.Pool.QueryRow(ctx, `
SELECT n.run_id, r.run_id
FROM plan_nodes n
LEFT JOIN run_backend_index r
    ON r.plan_id = n.plan_id
   AND r.node_id = n.node_id
   AND r.run_id = $3
   AND r.account_id = $4
   AND r.project_id = $5
WHERE n.plan_id = $1
  AND n.node_id = $2`, ref.PlanID, ref.NodeID, ref.RunID, scope.AccountID, scope.ProjectID).Scan(&nodeRunID, &ownedRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: artifact node %q is not durable", agentoscore.ErrInvalidArtifact, ref.NodeID)
		}

		return fmt.Errorf("AgentOSArtifactRepo - validateArtifactOwnership - query: %w", err)
	}

	if nodeRunID != ref.RunID {
		return fmt.Errorf("%w: artifact node %q has durable run id %q, got %q", agentoscore.ErrInvalidArtifact, ref.NodeID, nodeRunID, ref.RunID)
	}

	if !ownedRunID.Valid || ownedRunID.String == "" {
		return fmt.Errorf("%w: %s", agentoscore.ErrRunRouteNotFound, ref.RunID)
	}

	return nil
}

func (r *AgentOSArtifactRepo) artifactByIdempotencyKey(ctx context.Context, planID string, scope planTenantScope, key string) (agentoscore.ArtifactRef, bool, error) {
	return r.getRef(ctx, sq.Eq{
		"plan_id":         planID,
		"account_id":      scope.AccountID,
		"project_id":      scope.ProjectID,
		"idempotency_key": key,
	})
}

func (r *AgentOSArtifactRepo) artifactByID(ctx context.Context, artifactID string) (agentoscore.ArtifactRef, bool, error) {
	if artifactID == "" {
		return agentoscore.ArtifactRef{}, false, fmt.Errorf("%w: artifact id is required", agentoscore.ErrInvalidArtifact)
	}

	return r.getRef(ctx, sq.Eq{"artifact_id": artifactID})
}

func artifactIDConflictError(existing *agentoscore.ArtifactRef, requestedID string) error {
	artifactID := requestedID
	if artifactID == "" {
		artifactID = existing.ArtifactID
	}

	return fmt.Errorf("%w: artifact id %q already exists with a different idempotency key", agentoscore.ErrInvalidArtifact, artifactID)
}

func artifactScopeWhere(scope *agentos.PlanArtifactScope) sq.Eq {
	where := sq.Eq{
		"plan_id":    scope.PlanID,
		"account_id": scope.AccountID,
		"project_id": scope.ProjectID,
	}
	if scope.ArtifactID != "" {
		where["artifact_id"] = scope.ArtifactID
	}

	if scope.NodeID != "" {
		where["node_id"] = scope.NodeID
	}

	if scope.RunID != "" {
		where["run_id"] = scope.RunID
	}

	return where
}

func (r *AgentOSArtifactRepo) getRef(ctx context.Context, where sq.Eq) (agentoscore.ArtifactRef, bool, error) {
	query, args, err := r.Builder.
		Select(artifactColumns()...).
		From("artifacts").
		Where(where).
		ToSql()
	if err != nil {
		return agentoscore.ArtifactRef{}, false, fmt.Errorf("getRef builder: %w", err)
	}

	row := r.Pool.QueryRow(ctx, query, args...)

	ref, err := scanArtifactRef(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentoscore.ArtifactRef{}, false, nil
		}

		return agentoscore.ArtifactRef{}, false, fmt.Errorf("AgentOSArtifactRepo - getRef - scan: %w", err)
	}

	return ref, true, nil
}

func artifactColumns() []string {
	return []string{"artifact_id", "plan_id", "COALESCE(node_id, '') AS node_id", "COALESCE(run_id, '') AS run_id", "name", "kind", "media_type", "uri", "size_bytes", "digest", "metadata_json", "created_at"}
}

func artifactBlobKey(artifactID, digest string) string {
	if digest == "" {
		return artifactID
	}

	return artifactID + "/" + strings.TrimPrefix(digest, "sha256:")
}

func isPostgresUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == postgresUniqueViolation
}

type artifactScanner interface {
	Scan(dest ...any) error
}

func scanArtifactRef(scanner artifactScanner) (agentoscore.ArtifactRef, error) {
	var (
		ref          agentoscore.ArtifactRef
		kind         string
		metadataJSON []byte
	)

	if err := scanner.Scan(
		&ref.ArtifactID,
		&ref.PlanID,
		&ref.NodeID,
		&ref.RunID,
		&ref.Name,
		&kind,
		&ref.MediaType,
		&ref.URI,
		&ref.SizeBytes,
		&ref.Digest,
		&metadataJSON,
		&ref.CreatedAt,
	); err != nil {
		return agentoscore.ArtifactRef{}, err
	}

	ref.Kind = agentoscore.ArtifactKind(kind)
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &ref.Metadata); err != nil {
			return agentoscore.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - scanArtifactRef - decode metadata: %w", err)
		}
	}

	return ref, nil
}
