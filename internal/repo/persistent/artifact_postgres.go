package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	artifactblob "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const postgresUniqueViolation = "23505"

// AgentOSArtifactRepo persists artifact metadata in Postgres and payload bytes
// in a blob store. Statements and bindings come from queries/artifact.sql;
// this file owns the publish-idempotency protocol around them.
type AgentOSArtifactRepo struct {
	*postgres.Postgres

	blob    artifactblob.BlobStore
	queries *sqlcgen.Queries
}

// NewAgentOSArtifactRepo creates a production AgentOS artifact store.
func NewAgentOSArtifactRepo(pg *postgres.Postgres, blob artifactblob.BlobStore) *AgentOSArtifactRepo {
	return &AgentOSArtifactRepo{Postgres: pg, blob: blob, queries: sqlcgen.New(pg.Pool)}
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

	scope, exists, err := planTenantScopeByPlanID(ctx, r.queries, ref.PlanID)
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

	row, err := r.queries.InsertArtifact(ctx, sqlcgen.InsertArtifactParams{
		ArtifactID:     ref.ArtifactID,
		PlanID:         ref.PlanID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		NodeID:         nullableText(ref.NodeID),
		RunID:          nullableText(ref.RunID),
		Name:           ref.Name,
		Kind:           string(ref.Kind),
		MediaType:      ref.MediaType,
		Uri:            ref.URI,
		SizeBytes:      ref.SizeBytes,
		Digest:         ref.Digest,
		MetadataJson:   metadataJSON,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      ref.CreatedAt,
	})
	if err != nil {
		return r.resolveArtifactPutConflict(ctx, ref, scope, idempotencyKey, err)
	}

	stored, err := artifactRefFromInsertRow(&row)
	if err != nil {
		return agentoscore.ArtifactRef{}, err
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

	row, err := r.queries.GetArtifactByScope(ctx, sqlcgen.GetArtifactByScopeParams{
		PlanID:     scope.PlanID,
		AccountID:  scope.AccountID,
		ProjectID:  scope.ProjectID,
		ArtifactID: scope.ArtifactID,
		NodeID:     scope.NodeID,
		RunID:      scope.RunID,
	})
	if missingRow(err) {
		return agentoscore.ArtifactRef{}, nil, fmt.Errorf("%w: %s", agentoscore.ErrArtifactNotFound, scope.ArtifactID)
	}

	if err != nil {
		return agentoscore.ArtifactRef{}, nil, fmt.Errorf("AgentOSArtifactRepo - Get - query: %w", err)
	}

	ref, err := artifactRefFromScopeRow(&row)
	if err != nil {
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

	params := sqlcgen.ListArtifactsParams{
		PlanID:     scope.PlanID,
		AccountID:  scope.AccountID,
		ProjectID:  scope.ProjectID,
		ArtifactID: scope.ArtifactID,
		NodeID:     scope.NodeID,
		RunID:      scope.RunID,
		RowLimit:   optionalInt8(scope.Limit),
	}

	rows, err := r.queries.ListArtifacts(ctx, params)

	return listRecords("AgentOSArtifactRepo - List", rows, err, artifactRefFromListRow)
}

func (r *AgentOSArtifactRepo) validateArtifactOwnership(ctx context.Context, ref *agentoscore.ArtifactRef, scope planTenantScope) error {
	if ref.NodeID == "" && ref.RunID == "" {
		return nil
	}

	if ref.NodeID == "" || ref.RunID == "" {
		return fmt.Errorf("%w: artifact node id and run id must be provided together", agentoscore.ErrInvalidArtifact)
	}

	owner, err := r.queries.GetPlanNodeRunOwnership(ctx, sqlcgen.GetPlanNodeRunOwnershipParams{
		PlanID:    ref.PlanID,
		NodeID:    ref.NodeID,
		RunID:     ref.RunID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
	if missingRow(err) {
		return fmt.Errorf("%w: artifact node %q is not durable", agentoscore.ErrInvalidArtifact, ref.NodeID)
	}

	if err != nil {
		return fmt.Errorf("AgentOSArtifactRepo - validateArtifactOwnership - query: %w", err)
	}

	if owner.NodeRunID != ref.RunID {
		return fmt.Errorf("%w: artifact node %q has durable run id %q, got %q", agentoscore.ErrInvalidArtifact, ref.NodeID, owner.NodeRunID, ref.RunID)
	}

	if !owner.OwnedRunID.Valid || owner.OwnedRunID.String == "" {
		return fmt.Errorf("%w: %s", agentoscore.ErrRunRouteNotFound, ref.RunID)
	}

	return nil
}

func (r *AgentOSArtifactRepo) artifactByIdempotencyKey(ctx context.Context, planID string, scope planTenantScope, key string) (agentoscore.ArtifactRef, bool, error) {
	row, err := r.queries.GetArtifactByIdempotencyKey(ctx, sqlcgen.GetArtifactByIdempotencyKeyParams{
		PlanID:         planID,
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		IdempotencyKey: key,
	})
	if missingRow(err) {
		return agentoscore.ArtifactRef{}, false, nil
	}

	if err != nil {
		return agentoscore.ArtifactRef{}, false, fmt.Errorf("AgentOSArtifactRepo - artifactByIdempotencyKey - query: %w", err)
	}

	ref, err := artifactRefFromIdempotencyRow(&row)
	if err != nil {
		return agentoscore.ArtifactRef{}, false, err
	}

	return ref, true, nil
}

func (r *AgentOSArtifactRepo) artifactByID(ctx context.Context, artifactID string) (agentoscore.ArtifactRef, bool, error) {
	if artifactID == "" {
		return agentoscore.ArtifactRef{}, false, fmt.Errorf("%w: artifact id is required", agentoscore.ErrInvalidArtifact)
	}

	row, err := r.queries.GetArtifactByID(ctx, artifactID)
	if missingRow(err) {
		return agentoscore.ArtifactRef{}, false, nil
	}

	if err != nil {
		return agentoscore.ArtifactRef{}, false, fmt.Errorf("AgentOSArtifactRepo - artifactByID - query: %w", err)
	}

	ref, err := artifactRefFromIDRow(&row)
	if err != nil {
		return agentoscore.ArtifactRef{}, false, err
	}

	return ref, true, nil
}

func artifactIDConflictError(existing *agentoscore.ArtifactRef, requestedID string) error {
	artifactID := requestedID
	if artifactID == "" {
		artifactID = existing.ArtifactID
	}

	return fmt.Errorf("%w: artifact id %q already exists with a different idempotency key", agentoscore.ErrInvalidArtifact, artifactID)
}

// artifactRefFields is the one projection every artifacts query returns, so
// every row mapper shapes it through this single constructor.
type artifactRefFields struct {
	ArtifactID string
	PlanID     string
	NodeID     string
	RunID      string
	Name       string
	Kind       string
	MediaType  string
	URI        string
	SizeBytes  int64
	Digest     string
	Metadata   []byte
	CreatedAt  time.Time
}

func artifactRefFromFields(name string, fields *artifactRefFields) (agentoscore.ArtifactRef, error) {
	ref := agentoscore.ArtifactRef{
		ArtifactID: fields.ArtifactID,
		PlanID:     fields.PlanID,
		NodeID:     fields.NodeID,
		RunID:      fields.RunID,
		Name:       fields.Name,
		Kind:       agentoscore.ArtifactKind(fields.Kind),
		MediaType:  fields.MediaType,
		URI:        fields.URI,
		SizeBytes:  fields.SizeBytes,
		Digest:     fields.Digest,
		CreatedAt:  fields.CreatedAt,
	}

	if len(fields.Metadata) > 0 {
		if err := json.Unmarshal(fields.Metadata, &ref.Metadata); err != nil {
			return agentoscore.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - %s - decode metadata: %w", name, err)
		}
	}

	return ref, nil
}

func artifactRefFromInsertRow(row *sqlcgen.InsertArtifactRow) (agentoscore.ArtifactRef, error) {
	fields := artifactRefFields{
		ArtifactID: row.ArtifactID, PlanID: row.PlanID, NodeID: row.NodeID, RunID: row.RunID,
		Name: row.Name, Kind: row.Kind, MediaType: row.MediaType, URI: row.Uri,
		SizeBytes: row.SizeBytes, Digest: row.Digest, Metadata: row.MetadataJson, CreatedAt: row.CreatedAt,
	}

	return artifactRefFromFields("Put", &fields)
}

func artifactRefFromIdempotencyRow(row *sqlcgen.GetArtifactByIdempotencyKeyRow) (agentoscore.ArtifactRef, error) {
	fields := artifactRefFields{
		ArtifactID: row.ArtifactID, PlanID: row.PlanID, NodeID: row.NodeID, RunID: row.RunID,
		Name: row.Name, Kind: row.Kind, MediaType: row.MediaType, URI: row.Uri,
		SizeBytes: row.SizeBytes, Digest: row.Digest, Metadata: row.MetadataJson, CreatedAt: row.CreatedAt,
	}

	return artifactRefFromFields("artifactByIdempotencyKey", &fields)
}

func artifactRefFromIDRow(row *sqlcgen.GetArtifactByIDRow) (agentoscore.ArtifactRef, error) {
	fields := artifactRefFields{
		ArtifactID: row.ArtifactID, PlanID: row.PlanID, NodeID: row.NodeID, RunID: row.RunID,
		Name: row.Name, Kind: row.Kind, MediaType: row.MediaType, URI: row.Uri,
		SizeBytes: row.SizeBytes, Digest: row.Digest, Metadata: row.MetadataJson, CreatedAt: row.CreatedAt,
	}

	return artifactRefFromFields("artifactByID", &fields)
}

func artifactRefFromScopeRow(row *sqlcgen.GetArtifactByScopeRow) (agentoscore.ArtifactRef, error) {
	fields := artifactRefFields{
		ArtifactID: row.ArtifactID, PlanID: row.PlanID, NodeID: row.NodeID, RunID: row.RunID,
		Name: row.Name, Kind: row.Kind, MediaType: row.MediaType, URI: row.Uri,
		SizeBytes: row.SizeBytes, Digest: row.Digest, Metadata: row.MetadataJson, CreatedAt: row.CreatedAt,
	}

	return artifactRefFromFields("Get", &fields)
}

func artifactRefFromListRow(row *sqlcgen.ListArtifactsRow) (agentoscore.ArtifactRef, error) {
	fields := artifactRefFields{
		ArtifactID: row.ArtifactID, PlanID: row.PlanID, NodeID: row.NodeID, RunID: row.RunID,
		Name: row.Name, Kind: row.Kind, MediaType: row.MediaType, URI: row.Uri,
		SizeBytes: row.SizeBytes, Digest: row.Digest, Metadata: row.MetadataJson, CreatedAt: row.CreatedAt,
	}

	return artifactRefFromFields("List", &fields)
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
