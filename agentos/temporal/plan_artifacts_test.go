package temporal

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
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
		Artifacts: []agentos.ArtifactRef{
			{
				ArtifactID: "artifact-1",
				Name:       "summary",
				Kind:       agentos.ArtifactKindObject,
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
		ref  agentos.ArtifactRef
	}{
		{
			name: "plan",
			ref: agentos.ArtifactRef{
				ArtifactID: "artifact-1",
				PlanID:     "other-plan",
				Name:       "summary",
				Kind:       agentos.ArtifactKindObject,
			},
		},
		{
			name: "node",
			ref: agentos.ArtifactRef{
				ArtifactID: "artifact-1",
				NodeID:     "other-node",
				Name:       "summary",
				Kind:       agentos.ArtifactKindObject,
			},
		},
		{
			name: "run",
			ref: agentos.ArtifactRef{
				ArtifactID: "artifact-1",
				RunID:      "other-run",
				Name:       "summary",
				Kind:       agentos.ArtifactKindObject,
			},
		},
		{
			name: "uri-only",
			ref: agentos.ArtifactRef{
				URI:  "s3://artifacts/plan-1/artifact-1",
				Name: "summary",
				Kind: agentos.ArtifactKindObject,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := normalizeRunArtifacts("plan-1", &node, &agentos.RunStatus{
				RunID:     status.RunID,
				Artifacts: []agentos.ArtifactRef{test.ref},
			})
			if !errors.Is(err, agentos.ErrInvalidArtifact) {
				t.Fatalf("normalizeRunArtifacts error = %v, want ErrInvalidArtifact", err)
			}
		})
	}
}

func TestValidateRequiredArtifactsRejectsMissingOutput(t *testing.T) {
	t.Parallel()

	err := validateRequiredArtifacts(
		[]agentos.ArtifactSpec{{Name: "summary", Kind: agentos.ArtifactKindObject, Required: true}},
		nil,
	)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}
