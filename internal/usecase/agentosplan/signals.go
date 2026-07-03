package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const (
	SignalPayloadNodeID = agentos.SignalPayloadNodeID
	SignalPayloadReason = agentos.SignalPayloadReason
)

// ValidatePlanSignal validates signals owned by the RunPlan control plane.
func ValidatePlanSignal(signal *agentos.Signal) error {
	if signal == nil {
		return fmt.Errorf("%w: signal is required", agentos.ErrInvalidSignal)
	}

	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentos.ErrInvalidSignal)
	}

	if signal.IdempotencyKey == "" {
		return fmt.Errorf("%w: signal idempotency key is required", agentos.ErrInvalidSignal)
	}

	if signal.ActorID == "" {
		return fmt.Errorf("%w: signal actor id is required", agentos.ErrInvalidSignal)
	}

	switch signal.Type {
	case agentos.SignalPlanNodeRetry:
		if _, err := PlanSignalNodeID(signal); err != nil {
			return err
		}

		return nil
	case agentos.SignalPlanApprove, agentos.SignalPlanReject:
		return nil
	case agentos.SignalControlPause, agentos.SignalControlResume, agentos.SignalControlCancel,
		agentos.SignalUserMessage, agentos.SignalUserApproval, agentos.SignalUserReject,
		agentos.SignalToolResult, agentos.SignalHumanFeedback, agentos.SignalConfigPatch,
		agentos.SignalMemoryPatch:
		return fmt.Errorf("%w: unsupported plan signal %q", agentos.ErrInvalidSignal, signal.Type)
	default:
		return fmt.Errorf("%w: unsupported plan signal %q", agentos.ErrInvalidSignal, signal.Type)
	}
}

// PlanSignalNodeID returns the target node id from a node-scoped plan signal.
func PlanSignalNodeID(signal *agentos.Signal) (string, error) {
	if signal == nil {
		return "", fmt.Errorf("%w: signal is required", agentos.ErrInvalidSignal)
	}

	nodeID, ok := signal.Payload[SignalPayloadNodeID].(string)
	if !ok || nodeID == "" {
		return "", fmt.Errorf("%w: payload.%s is required", agentos.ErrInvalidSignal, SignalPayloadNodeID)
	}

	return nodeID, nil
}

// PlanSignalReason returns an optional human/system reason from a plan signal.
func PlanSignalReason(signal *agentos.Signal) string {
	if signal == nil {
		return ""
	}

	reason, ok := signal.Payload[SignalPayloadReason].(string)
	if !ok {
		return ""
	}

	return reason
}
