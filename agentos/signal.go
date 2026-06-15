package agentos

import "time"

// Signal carries business input to a running agent.
type Signal struct {
	Type           SignalType
	IdempotencyKey string
	Payload        map[string]any
	SentAt         time.Time
}

// SignalType identifies a cross-backend control-plane signal.
type SignalType string

const (
	SignalControlPause  SignalType = "control.pause"
	SignalControlResume SignalType = "control.resume"
	SignalControlCancel SignalType = "control.cancel"
	SignalUserMessage   SignalType = "user.message"
	SignalUserApproval  SignalType = "user.approval"
	SignalUserReject    SignalType = "user.reject"
	SignalToolResult    SignalType = "tool.result"
	SignalHumanFeedback SignalType = "human.feedback"
	SignalConfigPatch   SignalType = "config.patch"
	SignalMemoryPatch   SignalType = "memory.patch"
)
