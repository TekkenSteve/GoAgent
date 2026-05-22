package stream

import (
	"github.com/TekkenSteve/GoAgent/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/entity"
)

// SSEGateway is a stateless gateway that serializes events directly as JSON.
// This is suitable for SSE transport where each event is sent immediately.
type SSEGateway struct{}

// NewSSEGateway creates an SSEGateway.
func NewSSEGateway() *SSEGateway {
	return &SSEGateway{}
}

// Convert directly marshals the event to JSON.
func (g *SSEGateway) Convert(event entity.StreamEvent) ([]byte, error) {
	return entity.MarshalEvent(event)
}

// compile-time interface checks
var _ stream.StatelessGateway = (*SSEGateway)(nil)
