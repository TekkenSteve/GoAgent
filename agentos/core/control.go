// Package core defines the public AgentOS control types shared across runtimes.
package core

import (
	"fmt"
	"time"
)

// ControlOperation defines an external run-control intent.
type ControlOperation string

const (
	// ControlPause requests that a run pause its active work.
	ControlPause ControlOperation = "pause"
	// ControlResume requests that a run resume its paused work.
	ControlResume ControlOperation = "resume"
	// ControlCancel requests that a run cancel its active work.
	ControlCancel ControlOperation = "cancel"
)

// ControlRequest carries a lifecycle control intent with idempotency metadata.
type ControlRequest struct {
	Operation      ControlOperation  `json:"operation"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitzero" schema:"optional"`
	ActorID        string            `json:"actor_id,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// ValidateControlRequest validates a lifecycle control request.
func ValidateControlRequest(req *ControlRequest) error {
	if req == nil {
		return fmt.Errorf("%w: control request is required", ErrInvalidControlOperation)
	}

	switch req.Operation {
	case ControlPause, ControlResume, ControlCancel:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrInvalidControlOperation, req.Operation)
	}
}
