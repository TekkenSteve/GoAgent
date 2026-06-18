package agentos

import (
	"fmt"
	"time"
)

// ControlOperation defines an external run-control intent.
type ControlOperation string

const (
	ControlPause  ControlOperation = "pause"
	ControlResume ControlOperation = "resume"
	ControlCancel ControlOperation = "cancel"
)

// ControlRequest carries a lifecycle control intent with idempotency metadata.
type ControlRequest struct {
	Operation      ControlOperation  `json:"operation"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitempty"`
	ActorID        string            `json:"actor_id,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// ValidateControlRequest validates a lifecycle control request.
func ValidateControlRequest(req ControlRequest) error {
	switch req.Operation {
	case ControlPause, ControlResume, ControlCancel:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrInvalidControlOperation, req.Operation)
	}
}
