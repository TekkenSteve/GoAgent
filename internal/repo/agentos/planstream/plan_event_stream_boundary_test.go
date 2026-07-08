package planstream

import (
	"encoding/json"
	"os/exec"
	"testing"
)

func TestPlanEventStreamPackageDoesNotDependOnNativeStreamTypes(t *testing.T) {
	t.Parallel()

	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("lookup go binary: %v", err)
	}

	cmd := exec.CommandContext(t.Context(), goBinary, "list", "-json", ".")

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list planstream package: %v", err)
	}

	var pkg struct {
		Imports []string
		Deps    []string
	}
	if err := json.Unmarshal(out, &pkg); err != nil {
		t.Fatalf("decode go list output: %v", err)
	}

	for _, imported := range append(pkg.Imports, pkg.Deps...) {
		switch imported {
		case "github.com/TekkenSteve/GoAgent/internal/entity",
			"github.com/TekkenSteve/GoAgent/internal/agentfw/stream":
			t.Fatalf("plan event stream package must use public agentos.PlanEvent, not %s", imported)
		}
	}
}
