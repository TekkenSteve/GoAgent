package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const (
	SignalPayloadNodeID = "node_id"
	SignalPayloadReason = "reason"
)

// ValidatePlanSignal validates signals owned by the RunPlan control plane.
func ValidatePlanSignal(signal agentos.Signal) error {
	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentos.ErrInvalidSignal)
	}
	if signal.IdempotencyKey == "" {
		return fmt.Errorf("%w: signal idempotency key is required", agentos.ErrInvalidSignal)
	}
	switch signal.Type {
	case agentos.SignalPlanNodeRetry:
		if _, err := PlanSignalNodeID(signal); err != nil {
			return err
		}

		return nil
	case agentos.SignalPlanApprove, agentos.SignalPlanReject:
		return nil
	default:
		return fmt.Errorf("%w: unsupported plan signal %q", agentos.ErrInvalidSignal, signal.Type)
	}
}

// PlanSignalNodeID returns the target node id from a node-scoped plan signal.
func PlanSignalNodeID(signal agentos.Signal) (string, error) {
	nodeID, _ := signal.Payload[SignalPayloadNodeID].(string)
	if nodeID == "" {
		return "", fmt.Errorf("%w: payload.%s is required", agentos.ErrInvalidSignal, SignalPayloadNodeID)
	}

	return nodeID, nil
}

// PlanSignalReason returns an optional human/system reason from a plan signal.
func PlanSignalReason(signal agentos.Signal) string {
	reason, _ := signal.Payload[SignalPayloadReason].(string)

	return reason
}
