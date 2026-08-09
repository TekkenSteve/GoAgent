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
	// SignalControlPause carries a control-plane pause signal to a running agent.
	SignalControlPause SignalType = "control.pause"
	// SignalControlResume carries a control-plane resume signal to a running agent.
	SignalControlResume SignalType = "control.resume"
	// SignalControlCancel carries a control-plane cancel signal to a running agent.
	SignalControlCancel SignalType = "control.cancel"
	// SignalPlanNodeRetry requests a retry of a plan node.
	SignalPlanNodeRetry SignalType = "plan.node.retry"
	// SignalPlanApprove approves a pending plan decision.
	SignalPlanApprove SignalType = "plan.approve"
	// SignalPlanReject rejects a pending plan decision.
	SignalPlanReject SignalType = "plan.reject"
	// SignalUserMessage carries a user message to a running agent.
	SignalUserMessage SignalType = "user.message"
	// SignalUserApproval carries user approval for a pending request.
	SignalUserApproval SignalType = "user.approval"
	// SignalUserReject carries user rejection for a pending request.
	SignalUserReject SignalType = "user.reject"
	// SignalToolResult delivers an asynchronous tool result to a running agent.
	SignalToolResult SignalType = "tool.result"
	// SignalHumanFeedback delivers human feedback to a running agent.
	SignalHumanFeedback SignalType = "human.feedback"
	// SignalConfigPatch applies a configuration patch to a running agent.
	SignalConfigPatch SignalType = "config.patch"
	// SignalMemoryPatch applies a memory patch to a running agent.
	SignalMemoryPatch SignalType = "memory.patch"
)

const (
	// SignalPayloadNodeID is the node identifier key for node-scoped plan signals.
	SignalPayloadNodeID = "node_id"
	// SignalPayloadReason is the human/system reason key for plan signals.
	SignalPayloadReason = "reason"
)
