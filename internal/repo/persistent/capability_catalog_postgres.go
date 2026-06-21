package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

// AgentOSCapabilityCatalogRepo persists backend capability declarations.
type AgentOSCapabilityCatalogRepo struct {
	*postgres.Postgres
}

// NewAgentOSCapabilityCatalogRepo creates a Postgres-backed capability catalog.
func NewAgentOSCapabilityCatalogRepo(pg *postgres.Postgres) *AgentOSCapabilityCatalogRepo {
	return &AgentOSCapabilityCatalogRepo{Postgres: pg}
}

func (r *AgentOSCapabilityCatalogRepo) RegisterCapability(ctx context.Context, capability agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error) {
	if err := agentosplan.ValidateCapability(capability); err != nil {
		return agentos.Capability{}, false, err
	}
	expectedKey, err := agentosplan.CapabilityRegistrationIdempotencyKey(capability)
	if err != nil {
		return agentos.Capability{}, false, err
	}
	if idempotencyKey != expectedKey {
		return agentos.Capability{}, false, fmt.Errorf("%w: capability registration idempotency key must match declaration", agentos.ErrInvalidRunPlan)
	}
	existing, exists, err := r.capabilityByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return agentos.Capability{}, false, err
	}
	if exists {
		if err := agentosplan.ValidateCapabilityRegistrationIdempotency(existing, capability); err != nil {
			return agentos.Capability{}, false, err
		}

		return existing, false, nil
	}

	capabilityJSON, err := json.Marshal(capability)
	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - marshal: %w", err)
	}
	existing, exists, err = r.GetCapability(ctx, capability.Backend, capability.Name)
	if err != nil {
		return agentos.Capability{}, false, err
	}
	if exists {
		if err := agentosplan.ValidateCapabilityRegistrationIdempotency(existing, capability); err != nil {
			return agentos.Capability{}, false, err
		}
	}
	created := !exists

	row := r.Pool.QueryRow(ctx, `
INSERT INTO agentos_capabilities (
    backend_kind,
    backend_name,
    capability_name,
    description,
    capability_json,
    idempotency_key
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (backend_kind, backend_name, capability_name) DO UPDATE SET
    description = EXCLUDED.description,
    capability_json = EXCLUDED.capability_json,
    idempotency_key = EXCLUDED.idempotency_key,
    updated_at = NOW()
WHERE agentos_capabilities.idempotency_key = EXCLUDED.idempotency_key
RETURNING capability_json`,
		string(capability.Backend.Kind),
		capability.Backend.Name,
		capability.Name,
		capability.Description,
		capabilityJSON,
		idempotencyKey,
	)
	if err := row.Scan(&capabilityJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.Capability{}, false, fmt.Errorf("%w: capability %s/%s/%s already exists with a different declaration", agentos.ErrInvalidRunPlan, capability.Backend.Kind, capability.Backend.Name, capability.Name)
		}
		if isPostgresUniqueViolation(err) {
			existing, exists, lookupErr := r.capabilityByIdempotencyKey(ctx, idempotencyKey)
			if lookupErr != nil {
				return agentos.Capability{}, false, lookupErr
			}
			if exists {
				if err := agentosplan.ValidateCapabilityRegistrationIdempotency(existing, capability); err != nil {
					return agentos.Capability{}, false, err
				}

				return existing, false, nil
			}
		}

		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - upsert: %w", err)
	}

	registered, err := unmarshalCapability(capabilityJSON)
	if err != nil {
		return agentos.Capability{}, false, err
	}

	return registered, created, nil
}

func (r *AgentOSCapabilityCatalogRepo) GetCapability(ctx context.Context, backend agentos.BackendRef, name string) (agentos.Capability, bool, error) {
	sql, args, err := r.Builder.
		Select("capability_json").
		From("agentos_capabilities").
		Where(sq.Eq{
			"backend_kind":    string(backend.Kind),
			"backend_name":    backend.Name,
			"capability_name": name,
		}).
		ToSql()
	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - GetCapability - builder: %w", err)
	}

	return r.scanCapability(ctx, sql, args...)
}

func (r *AgentOSCapabilityCatalogRepo) capabilityByIdempotencyKey(ctx context.Context, idempotencyKey string) (agentos.Capability, bool, error) {
	sql, args, err := r.Builder.
		Select("capability_json").
		From("agentos_capabilities").
		Where(sq.Eq{"idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - capabilityByIdempotencyKey - builder: %w", err)
	}

	return r.scanCapability(ctx, sql, args...)
}

func (r *AgentOSCapabilityCatalogRepo) scanCapability(ctx context.Context, sql string, args ...any) (agentos.Capability, bool, error) {
	var capabilityJSON []byte
	err := r.Pool.QueryRow(ctx, sql, args...).Scan(&capabilityJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.Capability{}, false, nil
		}

		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - scanCapability - query: %w", err)
	}
	capability, err := unmarshalCapability(capabilityJSON)
	if err != nil {
		return agentos.Capability{}, false, err
	}

	return capability, true, nil
}

func unmarshalCapability(data []byte) (agentos.Capability, error) {
	var capability agentos.Capability
	if err := json.Unmarshal(data, &capability); err != nil {
		return agentos.Capability{}, fmt.Errorf("AgentOSCapabilityCatalogRepo - unmarshalCapability - decode: %w", err)
	}
	if err := agentosplan.ValidateCapability(capability); err != nil {
		return agentos.Capability{}, err
	}

	return capability, nil
}

var (
	_ agentosplan.CapabilityCatalog  = (*AgentOSCapabilityCatalogRepo)(nil)
	_ agentosplan.CapabilityRegistry = (*AgentOSCapabilityCatalogRepo)(nil)
)
