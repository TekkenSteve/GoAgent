package agentosplan

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanSignalRequiresRetryNodeID(t *testing.T) {
	err := ValidatePlanSignal(agentos.Signal{
		Type:           agentos.SignalPlanNodeRetry,
		IdempotencyKey: "retry-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("error = %v, want ErrInvalidSignal", err)
	}
}

func TestValidatePlanSignalAcceptsPlanSignals(t *testing.T) {
	signals := []agentos.Signal{
		{
			Type:           agentos.SignalPlanNodeRetry,
			IdempotencyKey: "retry-1",
			ActorID:        "operator-1",
			Payload:        map[string]any{SignalPayloadNodeID: "node-1"},
		},
		{Type: agentos.SignalPlanApprove, IdempotencyKey: "approve-1", ActorID: "operator-1"},
		{Type: agentos.SignalPlanReject, IdempotencyKey: "reject-1", ActorID: "operator-1"},
	}
	for _, signal := range signals {
		if err := ValidatePlanSignal(signal); err != nil {
			t.Fatalf("ValidatePlanSignal(%s): %v", signal.Type, err)
		}
	}
}

func TestValidatePlanSignalRejectsUnsupportedSignal(t *testing.T) {
	err := ValidatePlanSignal(agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "message-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("error = %v, want ErrInvalidSignal", err)
	}
}
