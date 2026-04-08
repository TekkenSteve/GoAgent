package orchestration

import (
	"context"
	"time"
)

const (
	// EchoActivityName is a bootstrap activity used to validate worker wiring.
	EchoActivityName = "agentfw.echo-activity.v1"
)

// EchoActivityInput is a minimal bootstrap input payload.
type EchoActivityInput struct {
	RunID string
}

// EchoActivityResult is a minimal bootstrap output payload.
type EchoActivityResult struct {
	RunID     string
	HandledAt time.Time
}

// EchoActivity is a placeholder activity to validate activity registration.
func EchoActivity(_ context.Context, input EchoActivityInput) (EchoActivityResult, error) {
	return EchoActivityResult{
		RunID:     input.RunID,
		HandledAt: time.Now().UTC(),
	}, nil
}
