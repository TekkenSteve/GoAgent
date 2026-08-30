package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
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

// RegisterCapability upserts a backend capability declaration, returning the stored capability and whether it was newly registered.
func (r *AgentOSCapabilityCatalogRepo) RegisterCapability(ctx context.Context, capability *agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error) {
	existing, exists, err := r.existingCapabilityByIdempotencyKey(ctx, capability, idempotencyKey)
	if err != nil {
		return agentos.Capability{}, false, err
	}

	if exists {
		return existing, false, nil
	}

	capabilityJSON, err := json.Marshal(capability)
	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - marshal: %w", err)
	}

	_, exists, err = r.existingCapabilityByName(ctx, capability)
	if err != nil {
		return agentos.Capability{}, false, err
	}

	capabilityJSON, err = r.insertCapabilityRow(ctx, capability, capabilityJSON, idempotencyKey)
	if err != nil {
		return r.handleCapabilityScanErr(ctx, err, capability, idempotencyKey)
	}

	registered, err := unmarshalCapability(capabilityJSON)
	if err != nil {
		return agentos.Capability{}, false, err
	}

	return registered, !exists, nil
}

func (r *AgentOSCapabilityCatalogRepo) existingCapabilityByIdempotencyKey(ctx context.Context, capability *agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error) {
	if err := validateCapabilityRegistrationRequest(capability, idempotencyKey); err != nil {
		return agentos.Capability{}, false, err
	}

	existing, exists, err := r.capabilityByIdempotencyKey(ctx, idempotencyKey)
	if err != nil || !exists {
		return agentos.Capability{}, false, err
	}

	if err := agentosplan.ValidateCapabilityRegistrationIdempotency(&existing, capability); err != nil {
		return agentos.Capability{}, false, err
	}

	return existing, true, nil
}

func validateCapabilityRegistrationRequest(capability *agentos.Capability, idempotencyKey string) error {
	if err := agentosplan.ValidateCapability(capability); err != nil {
		return err
	}

	expectedKey, err := agentosplan.CapabilityRegistrationIdempotencyKey(capability)
	if err != nil {
		return err
	}

	if idempotencyKey != expectedKey {
		return fmt.Errorf("%w: capability registration idempotency key must match declaration", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}

func (r *AgentOSCapabilityCatalogRepo) existingCapabilityByName(ctx context.Context, capability *agentos.Capability) (agentos.Capability, bool, error) {
	existing, exists, err := r.GetCapability(ctx, capability.Backend, capability.Name)
	if err != nil || !exists {
		return agentos.Capability{}, false, err
	}

	if err := agentosplan.ValidateCapabilityRegistrationIdempotency(&existing, capability); err != nil {
		return agentos.Capability{}, false, err
	}

	return existing, true, nil
}

func (r *AgentOSCapabilityCatalogRepo) insertCapabilityRow(ctx context.Context, capability *agentos.Capability, capabilityJSON []byte, idempotencyKey string) ([]byte, error) {
	row := r.Pool.QueryRow(
		ctx, `
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
		return nil, err
	}

	return capabilityJSON, nil
}

func (r *AgentOSCapabilityCatalogRepo) handleCapabilityScanErr(ctx context.Context, scanErr error, capability *agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error) {
	if errors.Is(scanErr, pgx.ErrNoRows) {
		return agentos.Capability{}, false, fmt.Errorf("%w: capability %s/%s/%s already exists with a different declaration", agentoscore.ErrInvalidRunPlan, capability.Backend.Kind, capability.Backend.Name, capability.Name)
	}

	if isPostgresUniqueViolation(scanErr) {
		return r.resolveCapabilityUniqueViolation(ctx, capability, idempotencyKey, scanErr)
	}

	return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - upsert: %w", scanErr)
}

func (r *AgentOSCapabilityCatalogRepo) resolveCapabilityUniqueViolation(ctx context.Context, capability *agentos.Capability, idempotencyKey string, scanErr error) (agentos.Capability, bool, error) {
	existing, exists, err := r.existingCapabilityByIdempotencyKey(ctx, capability, idempotencyKey)
	if err != nil || exists {
		return existing, false, err
	}

	return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - upsert: %w", scanErr)
}

// GetCapability loads the capability registered for the given backend and name.
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
		Where(sq.Eq{_colIDempotencyKey: idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - capabilityByIdempotencyKey - builder: %w", err)
	}

	return r.scanCapability(ctx, sql, args...)
}

func (r *AgentOSCapabilityCatalogRepo) scanCapability(ctx context.Context, sql string, args ...any) (agentos.Capability, bool, error) {
	capabilityJSON, found, err := scanJSONQueryRow(ctx, r.Pool, sql, args...)
	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - scanCapability - query: %w", err)
	}

	if !found {
		return agentos.Capability{}, false, nil
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

	if err := agentosplan.ValidateCapability(&capability); err != nil {
		return agentos.Capability{}, err
	}

	return capability, nil
}

var (
	_ agentosplan.CapabilityCatalog  = (*AgentOSCapabilityCatalogRepo)(nil)
	_ agentosplan.CapabilityRegistry = (*AgentOSCapabilityCatalogRepo)(nil)
)
