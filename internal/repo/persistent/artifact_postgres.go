package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/agentos"
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

func (r *AgentOSArtifactRepo) Put(ctx context.Context, ref agentos.ArtifactRef, payload any, idempotencyKey string) (agentos.ArtifactRef, error) {
	if idempotencyKey == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact idempotency key is required", agentos.ErrInvalidArtifact)
	}
	if ref.Name == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact name is required", agentos.ErrInvalidArtifact)
	}
	if ref.Kind == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact kind is required", agentos.ErrInvalidArtifact)
	}
	if ref.ArtifactID == "" {
		ref.ArtifactID = agentosplan.ArtifactIDFromIdempotencyKey(idempotencyKey)
	}
	if ref.CreatedAt.IsZero() {
		ref.CreatedAt = time.Now().UTC()
	}
	var encodedPayload []byte
	if payload != nil {
		encoded, mediaType, err := agentosplan.EncodeArtifactPayload(payload, ref.MediaType)
		if err != nil {
			return agentos.ArtifactRef{}, err
		}
		encodedPayload = encoded
		ref.MediaType = mediaType
		ref.SizeBytes = int64(len(encodedPayload))
		ref.Digest = agentosplan.DigestArtifactPayload(encodedPayload)
	}

	existing, exists, err := r.artifactByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return agentos.ArtifactRef{}, err
	}
	if exists {
		if err := agentosplan.ValidateArtifactPublishIdempotency(existing, ref); err != nil {
			return agentos.ArtifactRef{}, err
		}

		return existing, nil
	}

	if payload != nil {
		if r.blob == nil {
			return agentos.ArtifactRef{}, fmt.Errorf("%w: blob store is required for artifact payload", agentos.ErrInvalidArtifact)
		}
		object, err := r.blob.Put(ctx, artifactBlobKey(ref.ArtifactID, ref.Digest), encodedPayload)
		if err != nil {
			return agentos.ArtifactRef{}, err
		}
		ref.URI = object.URI
		ref.SizeBytes = object.SizeBytes
		ref.Digest = object.Digest
	}
	metadataJSON, err := json.Marshal(ref.Metadata)
	if err != nil {
		return agentos.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - Put - marshal metadata: %w", err)
	}

	row := r.Pool.QueryRow(ctx, `
INSERT INTO artifacts (
    artifact_id,
    plan_id,
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
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (artifact_id) DO UPDATE SET
    artifact_id = artifacts.artifact_id
WHERE artifacts.idempotency_key = EXCLUDED.idempotency_key
RETURNING `+strings.Join(artifactColumns(), ", "),
		ref.ArtifactID,
		ref.PlanID,
		ref.NodeID,
		ref.RunID,
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
	stored, err := scanArtifactRef(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.ArtifactRef{}, fmt.Errorf("%w: artifact id %q already exists with a different idempotency key", agentos.ErrInvalidArtifact, ref.ArtifactID)
		}
		if isPostgresUniqueViolation(err) {
			existing, exists, lookupErr := r.artifactByIdempotencyKey(ctx, idempotencyKey)
			if lookupErr != nil {
				return agentos.ArtifactRef{}, lookupErr
			}
			if exists {
				if err := agentosplan.ValidateArtifactPublishIdempotency(existing, ref); err != nil {
					return agentos.ArtifactRef{}, err
				}

				return existing, nil
			}
		}

		return agentos.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - Put - insert: %w", err)
	}
	if err := agentosplan.ValidateArtifactPublishIdempotency(stored, ref); err != nil {
		return agentos.ArtifactRef{}, err
	}

	return stored, nil
}

func (r *AgentOSArtifactRepo) Get(ctx context.Context, artifactID string) (agentos.ArtifactRef, any, error) {
	ref, exists, err := r.getRef(ctx, sq.Eq{"artifact_id": artifactID})
	if err != nil || !exists {
		if !exists {
			return agentos.ArtifactRef{}, nil, fmt.Errorf("%w: %s", agentos.ErrArtifactNotFound, artifactID)
		}

		return agentos.ArtifactRef{}, nil, err
	}
	if ref.URI == "" || r.blob == nil {
		return ref, nil, nil
	}
	data, err := r.blob.Get(ctx, ref.URI)
	if err != nil {
		return agentos.ArtifactRef{}, nil, err
	}
	payload, err := agentosplan.DecodeArtifactPayload(data, ref.MediaType)
	if err != nil {
		return agentos.ArtifactRef{}, nil, err
	}

	return ref, payload, nil
}

func (r *AgentOSArtifactRepo) List(ctx context.Context, planID string) ([]agentos.ArtifactRef, error) {
	sql, args, err := r.Builder.
		Select(artifactColumns()...).
		From("artifacts").
		Where(sq.Eq{"plan_id": planID}).
		OrderBy("created_at ASC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSArtifactRepo - List - builder: %w", err)
	}
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSArtifactRepo - List - query: %w", err)
	}
	defer rows.Close()

	var refs []agentos.ArtifactRef
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

func (r *AgentOSArtifactRepo) artifactByIdempotencyKey(ctx context.Context, key string) (agentos.ArtifactRef, bool, error) {
	return r.getRef(ctx, sq.Eq{"idempotency_key": key})
}

func (r *AgentOSArtifactRepo) getRef(ctx context.Context, where sq.Eq) (agentos.ArtifactRef, bool, error) {
	sql, args, err := r.Builder.
		Select(artifactColumns()...).
		From("artifacts").
		Where(where).
		ToSql()
	if err != nil {
		return agentos.ArtifactRef{}, false, fmt.Errorf("AgentOSArtifactRepo - getRef - builder: %w", err)
	}
	row := r.Pool.QueryRow(ctx, sql, args...)
	ref, err := scanArtifactRef(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.ArtifactRef{}, false, nil
		}

		return agentos.ArtifactRef{}, false, fmt.Errorf("AgentOSArtifactRepo - getRef - scan: %w", err)
	}

	return ref, true, nil
}

func artifactColumns() []string {
	return []string{"artifact_id", "plan_id", "node_id", "run_id", "name", "kind", "media_type", "uri", "size_bytes", "digest", "metadata_json", "created_at"}
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

func scanArtifactRef(scanner artifactScanner) (agentos.ArtifactRef, error) {
	var ref agentos.ArtifactRef
	var kind string
	var metadataJSON []byte
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
		return agentos.ArtifactRef{}, err
	}
	ref.Kind = agentos.ArtifactKind(kind)
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &ref.Metadata); err != nil {
			return agentos.ArtifactRef{}, fmt.Errorf("AgentOSArtifactRepo - scanArtifactRef - decode metadata: %w", err)
		}
	}

	return ref, nil
}
