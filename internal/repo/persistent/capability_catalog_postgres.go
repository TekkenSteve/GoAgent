package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

// AgentOSCapabilityCatalogRepo persists backend capability declarations.
// Statements and bindings come from queries/capability_catalog.sql; this file
// owns the registration idempotency protocol around them.
type AgentOSCapabilityCatalogRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewAgentOSCapabilityCatalogRepo creates a Postgres-backed capability catalog.
func NewAgentOSCapabilityCatalogRepo(pg *postgres.Postgres) *AgentOSCapabilityCatalogRepo {
	return &AgentOSCapabilityCatalogRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
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

	row, err := r.queries.UpsertCapability(ctx, sqlcgen.UpsertCapabilityParams{
		BackendKind:    string(capability.Backend.Kind),
		BackendName:    capability.Backend.Name,
		CapabilityName: capability.Name,
		Description:    capability.Description,
		CapabilityJson: capabilityJSON,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return r.handleCapabilityUpsertErr(ctx, err, capability, idempotencyKey)
	}

	registered, err := unmarshalCapability(row)
	if err != nil {
		return agentos.Capability{}, false, err
	}

	return registered, !exists, nil
}

func (r *AgentOSCapabilityCatalogRepo) existingCapabilityByIdempotencyKey(ctx context.Context, capability *agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error) {
	if err := validateCapabilityRegistrationRequest(capability, idempotencyKey); err != nil {
		return agentos.Capability{}, false, err
	}

	row, err := r.queries.GetCapabilityByIdempotencyKey(ctx, idempotencyKey)
	if missingRow(err) {
		return agentos.Capability{}, false, nil
	}

	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - existingCapabilityByIdempotencyKey - query: %w", err)
	}

	existing, err := unmarshalCapability(row)
	if err != nil {
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

func (r *AgentOSCapabilityCatalogRepo) handleCapabilityUpsertErr(ctx context.Context, upsertErr error, capability *agentos.Capability, idempotencyKey string) (agentos.Capability, bool, error) {
	// The conditional upsert reports a conflicting declaration by updating
	// nothing: no RETURNING row is the conflict, named for the caller.
	if errors.Is(upsertErr, pgx.ErrNoRows) {
		return agentos.Capability{}, false, fmt.Errorf("%w: capability %s/%s/%s already exists with a different declaration", agentoscore.ErrInvalidRunPlan, capability.Backend.Kind, capability.Backend.Name, capability.Name)
	}

	if isPostgresUniqueViolation(upsertErr) {
		return r.resolveCapabilityUniqueViolation(ctx, capability, idempotencyKey, upsertErr)
	}

	return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - upsert: %w", upsertErr)
}

func (r *AgentOSCapabilityCatalogRepo) resolveCapabilityUniqueViolation(ctx context.Context, capability *agentos.Capability, idempotencyKey string, upsertErr error) (agentos.Capability, bool, error) {
	existing, exists, err := r.existingCapabilityByIdempotencyKey(ctx, capability, idempotencyKey)
	if err != nil || exists {
		return existing, false, err
	}

	return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - RegisterCapability - upsert: %w", upsertErr)
}

// GetCapability loads the capability registered for the given backend and name.
func (r *AgentOSCapabilityCatalogRepo) GetCapability(ctx context.Context, backend agentos.BackendRef, name string) (agentos.Capability, bool, error) {
	row, err := r.queries.GetCapabilityByName(ctx, sqlcgen.GetCapabilityByNameParams{
		BackendKind:    string(backend.Kind),
		BackendName:    backend.Name,
		CapabilityName: name,
	})
	if missingRow(err) {
		return agentos.Capability{}, false, nil
	}

	if err != nil {
		return agentos.Capability{}, false, fmt.Errorf("AgentOSCapabilityCatalogRepo - GetCapability - query: %w", err)
	}

	capability, err := unmarshalCapability(row)
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
