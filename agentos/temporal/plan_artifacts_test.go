package temporal

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestNormalizeRunArtifactsAddsPlanNodeAndRunRefs(t *testing.T) {
	node := agentos.PlanNodeSpec{NodeID: "research"}
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

	refs, err := normalizeRunArtifacts("plan-1", node, status)
	if err != nil {
		t.Fatalf("normalizeRunArtifacts: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %#v", refs)
	}
	if refs[0].PlanID != "plan-1" || refs[0].NodeID != "research" || refs[0].RunID != "run-research" {
		t.Fatalf("ref = %#v", refs[0])
	}
}

func TestValidateRequiredArtifactsRejectsMissingOutput(t *testing.T) {
	err := validateRequiredArtifacts(
		[]agentos.ArtifactSpec{{Name: "summary", Kind: agentos.ArtifactKindObject, Required: true}},
		nil,
	)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}
