package temporal

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestPlanRuntimeSignalPlanValidatesSignalBeforeAudit(t *testing.T) {
	rt := &planRuntime{}

	err := rt.SignalPlan(t.Context(), "plan-1", agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "message-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidSignal", err)
	}

	err = rt.SignalPlan(t.Context(), "plan-1", agentos.Signal{
		Type:           agentos.SignalPlanNodeRetry,
		IdempotencyKey: "retry-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan retry error = %v, want ErrInvalidSignal", err)
	}
}
