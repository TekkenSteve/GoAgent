package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// PlanDeltaInput is deterministic context for workflow-owned dynamic expansion.
type PlanDeltaInput struct {
	Spec           agentos.RunPlanSpec
	Status         agentos.RunPlanStatus
	Node           agentos.PlanNodeSpec
	RunStatus      agentos.RunStatus
	Artifacts      []agentoscore.ArtifactRef
	ExpansionCount int32
}

// PlanDeltaProvider derives bounded expansion proposals for PlanWorkflow. The
// workflow still owns validation and application of the returned delta.
type PlanDeltaProvider interface {
	NextPlanDelta(ctx context.Context, input *PlanDeltaInput) (PlanDelta, bool, error)
}

// ArtifactPlanDeltaProvider materializes plan expansion proposals from explicit
// plan_delta artifacts published by completed child runs.
type ArtifactPlanDeltaProvider struct {
	Store ArtifactStore
}

func NewArtifactPlanDeltaProvider(store ArtifactStore) ArtifactPlanDeltaProvider {
	return ArtifactPlanDeltaProvider{Store: store}
}

func (p ArtifactPlanDeltaProvider) NextPlanDelta(ctx context.Context, input *PlanDeltaInput) (delta PlanDelta, expanded bool, err error) {
	refs := planDeltaArtifacts(input.Artifacts)
	if len(refs) == 0 {
		return PlanDelta{}, false, nil
	}

	if p.Store == nil {
		return PlanDelta{}, false, fmt.Errorf("%w: artifact store is required for plan expansion", agentoscore.ErrInvalidArtifact)
	}

	var combined PlanDelta

	for i := range refs {
		ref := refs[i]

		if ref.ArtifactID == "" {
			return PlanDelta{}, false, fmt.Errorf("%w: plan delta artifact %q has no artifact id", agentoscore.ErrInvalidArtifact, ref.Name)
		}

		scope := agentos.PlanArtifactScope{
			PlanID:     input.Spec.PlanID,
			AccountID:  input.Spec.AccountID,
			ProjectID:  input.Spec.ProjectID,
			ArtifactID: ref.ArtifactID,
		}

		_, payload, err := p.Store.Get(ctx, &scope)
		if err != nil {
			return PlanDelta{}, false, err
		}

		if payload == nil {
			return PlanDelta{}, false, fmt.Errorf("%w: plan delta artifact %q has no payload", agentoscore.ErrArtifactNotFound, ref.Name)
		}

		delta, err := DecodePlanDeltaPayload(payload)
		if err != nil {
			return PlanDelta{}, false, fmt.Errorf("%w: plan delta artifact %q: %w", agentoscore.ErrInvalidRunPlan, ref.Name, err)
		}

		combined.Nodes = append(combined.Nodes, delta.Nodes...)
		combined.Edges = append(combined.Edges, delta.Edges...)
	}

	if len(combined.Nodes) == 0 && len(combined.Edges) == 0 {
		return PlanDelta{}, false, fmt.Errorf("%w: plan delta artifacts produced an empty delta", agentoscore.ErrInvalidRunPlan)
	}

	return combined, true, nil
}

func DecodePlanDeltaPayload(payload any) (PlanDelta, error) {
	if delta, ok := payload.(PlanDelta); ok {
		return delta, nil
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return PlanDelta{}, fmt.Errorf("encode payload: %w", err)
	}

	delta, err := DecodeWireJSON[PlanDelta](data)
	if err != nil {
		return PlanDelta{}, fmt.Errorf("decode payload: %w", err)
	}

	return delta, nil
}

func planDeltaArtifacts(refs []agentoscore.ArtifactRef) []agentoscore.ArtifactRef {
	result := make([]agentoscore.ArtifactRef, 0)

	for i := range refs {
		ref := refs[i]

		if ref.Kind == agentoscore.ArtifactKindPlanDelta {
			result = append(result, ref)
		}
	}

	return result
}
