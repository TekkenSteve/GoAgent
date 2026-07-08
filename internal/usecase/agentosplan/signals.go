package agentosplan

import (
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

const (
	SignalPayloadNodeID = agentoscore.SignalPayloadNodeID
	SignalPayloadReason = agentoscore.SignalPayloadReason
)

// ValidatePlanSignal validates signals owned by the RunPlan control plane.
func ValidatePlanSignal(signal *agentoscore.Signal) error {
	if signal == nil {
		return fmt.Errorf("%w: signal is required", agentoscore.ErrInvalidSignal)
	}

	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentoscore.ErrInvalidSignal)
	}

	if signal.IdempotencyKey == "" {
		return fmt.Errorf("%w: signal idempotency key is required", agentoscore.ErrInvalidSignal)
	}

	if signal.ActorID == "" {
		return fmt.Errorf("%w: signal actor id is required", agentoscore.ErrInvalidSignal)
	}

	switch signal.Type {
	case agentoscore.SignalPlanNodeRetry:
		if _, err := PlanSignalNodeID(signal); err != nil {
			return err
		}

		return nil
	case agentoscore.SignalPlanApprove, agentoscore.SignalPlanReject:
		return nil
	case agentoscore.SignalControlPause, agentoscore.SignalControlResume, agentoscore.SignalControlCancel,
		agentoscore.SignalUserMessage, agentoscore.SignalUserApproval, agentoscore.SignalUserReject,
		agentoscore.SignalToolResult, agentoscore.SignalHumanFeedback, agentoscore.SignalConfigPatch,
		agentoscore.SignalMemoryPatch:
		return fmt.Errorf("%w: unsupported plan signal %q", agentoscore.ErrInvalidSignal, signal.Type)
	default:
		return fmt.Errorf("%w: unsupported plan signal %q", agentoscore.ErrInvalidSignal, signal.Type)
	}
}

// PlanSignalNodeID returns the target node id from a node-scoped plan signal.
func PlanSignalNodeID(signal *agentoscore.Signal) (string, error) {
	if signal == nil {
		return "", fmt.Errorf("%w: signal is required", agentoscore.ErrInvalidSignal)
	}

	nodeID, ok := signal.Payload[SignalPayloadNodeID].(string)
	if !ok || nodeID == "" {
		return "", fmt.Errorf("%w: payload.%s is required", agentoscore.ErrInvalidSignal, SignalPayloadNodeID)
	}

	return nodeID, nil
}

// PlanSignalReason returns an optional human/system reason from a plan signal.
func PlanSignalReason(signal *agentoscore.Signal) string {
	if signal == nil {
		return ""
	}

	reason, ok := signal.Payload[SignalPayloadReason].(string)
	if !ok {
		return ""
	}

	return reason
}
