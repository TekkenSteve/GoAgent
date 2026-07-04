package temporal

import (
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

const (
	RESEARCH    = "research"
	runResearch = "run-research"
)

func TestNormalizeRunArtifactsAddsPlanNodeAndRunRefs(t *testing.T) {
	t.Parallel()

	node := agentos.PlanNodeSpec{NodeID: RESEARCH}
	status := agentos.RunStatus{
		RunID: "run-research",
		Artifacts: []agentoscore.ArtifactRef{
			{
				ArtifactID: "artifact-1",
				Name:       "summary",
				Kind:       agentoscore.ArtifactKindObject,
			},
		},
	}

	refs, err := normalizeRunArtifacts("plan-1", &node, &status)
	if err != nil {
		t.Fatalf("normalizeRunArtifacts: %v", err)
	}

	if len(refs) != 1 {
		t.Fatalf("refs = %#v", refs)
	}

	if refs[0].PlanID != Plan1 || refs[0].NodeID != RESEARCH || refs[0].RunID != runResearch {
		t.Fatalf("ref = %#v", refs[0])
	}
}

func TestNormalizeRunArtifactsRejectsMismatchedOwnership(t *testing.T) {
	t.Parallel()

	node := agentos.PlanNodeSpec{NodeID: RESEARCH}
	status := agentos.RunStatus{RunID: "run-research"}

	for _, test := range []struct {
		name string
		ref  agentoscore.ArtifactRef
	}{
		{
			name: "plan",
			ref: agentoscore.ArtifactRef{
				ArtifactID: "artifact-1",
				PlanID:     "other-plan",
				Name:       "summary",
				Kind:       agentoscore.ArtifactKindObject,
			},
		},
		{
			name: "node",
			ref: agentoscore.ArtifactRef{
				ArtifactID: "artifact-1",
				NodeID:     "other-node",
				Name:       "summary",
				Kind:       agentoscore.ArtifactKindObject,
			},
		},
		{
			name: "run",
			ref: agentoscore.ArtifactRef{
				ArtifactID: "artifact-1",
				RunID:      "other-run",
				Name:       "summary",
				Kind:       agentoscore.ArtifactKindObject,
			},
		},
		{
			name: "uri-only",
			ref: agentoscore.ArtifactRef{
				URI:  "s3://artifacts/plan-1/artifact-1",
				Name: "summary",
				Kind: agentoscore.ArtifactKindObject,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := normalizeRunArtifacts("plan-1", &node, &agentos.RunStatus{
				RunID:     status.RunID,
				Artifacts: []agentoscore.ArtifactRef{test.ref},
			})
			if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
				t.Fatalf("normalizeRunArtifacts error = %v, want ErrInvalidArtifact", err)
			}
		})
	}
}

func TestValidateRequiredArtifactsRejectsMissingOutput(t *testing.T) {
	t.Parallel()

	err := validateRequiredArtifacts(
		[]agentos.ArtifactSpec{{Name: "summary", Kind: agentoscore.ArtifactKindObject, Required: true}},
		nil,
	)
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}
