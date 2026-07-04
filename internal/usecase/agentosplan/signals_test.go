package agentosplan

import (
	"errors"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestValidatePlanSignalRejectsNilSignal(t *testing.T) {
	t.Parallel()

	err := ValidatePlanSignal(nil)
	if !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("error = %v, want ErrInvalidSignal", err)
	}
}

func TestPlanSignalHelpersHandleNilSignal(t *testing.T) {
	t.Parallel()

	if _, err := PlanSignalNodeID(nil); !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("PlanSignalNodeID nil error = %v, want ErrInvalidSignal", err)
	}

	if reason := PlanSignalReason(nil); reason != "" {
		t.Fatalf("PlanSignalReason nil = %q, want empty", reason)
	}
}

func TestValidatePlanSignalRequiresRetryNodeID(t *testing.T) {
	t.Parallel()

	err := ValidatePlanSignal(&agentoscore.Signal{
		Type:           agentoscore.SignalPlanNodeRetry,
		IdempotencyKey: "retry-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("error = %v, want ErrInvalidSignal", err)
	}
}

func TestValidatePlanSignalAcceptsPlanSignals(t *testing.T) {
	t.Parallel()

	signals := []agentoscore.Signal{
		{
			Type:           agentoscore.SignalPlanNodeRetry,
			IdempotencyKey: "retry-1",
			ActorID:        "operator-1",
			Payload:        map[string]any{SignalPayloadNodeID: "node-1"},
		},
		{Type: agentoscore.SignalPlanApprove, IdempotencyKey: "approve-1", ActorID: "operator-1"},
		{Type: agentoscore.SignalPlanReject, IdempotencyKey: "reject-1", ActorID: "operator-1"},
	}
	for _, signal := range signals {
		if err := ValidatePlanSignal(&signal); err != nil {
			t.Fatalf("ValidatePlanSignal(%s): %v", signal.Type, err)
		}
	}
}

func TestValidatePlanSignalRejectsUnsupportedSignal(t *testing.T) {
	t.Parallel()

	err := ValidatePlanSignal(&agentoscore.Signal{
		Type:           agentoscore.SignalUserMessage,
		IdempotencyKey: "message-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("error = %v, want ErrInvalidSignal", err)
	}
}
