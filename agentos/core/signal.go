package core

import "time"

// Signal carries business input to a running agent.
type Signal struct {
	Type           SignalType     `json:"type"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	ActorID        string         `json:"actor_id,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
	SentAt         time.Time      `json:"sent_at,omitzero" schema:"optional"`
}

// SignalType identifies a cross-backend control-plane signal.
type SignalType string

const (
	SignalControlPause  SignalType = "control.pause"
	SignalControlResume SignalType = "control.resume"
	SignalControlCancel SignalType = "control.cancel"
	SignalPlanNodeRetry SignalType = "plan.node.retry"
	SignalPlanApprove   SignalType = "plan.approve"
	SignalPlanReject    SignalType = "plan.reject"
	SignalUserMessage   SignalType = "user.message"
	SignalUserApproval  SignalType = "user.approval"
	SignalUserReject    SignalType = "user.reject"
	SignalToolResult    SignalType = "tool.result"
	SignalHumanFeedback SignalType = "human.feedback"
	SignalConfigPatch   SignalType = "config.patch"
	SignalMemoryPatch   SignalType = "memory.patch"
)

const (
	// SignalPayloadNodeID is the node identifier key for node-scoped plan signals.
	SignalPayloadNodeID = "node_id"
	// SignalPayloadReason is the human/system reason key for plan signals.
	SignalPayloadReason = "reason"
)
