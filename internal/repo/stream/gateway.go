package stream

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// SSEGateway is a stateless gateway that serializes events directly as JSON.
// This is suitable for SSE transport where each event is sent immediately.
type SSEGateway struct {
	codec entity.EventCodec
}

// NewSSEGateway creates an SSEGateway.
func NewSSEGateway() *SSEGateway {
	return NewSSEGatewayWithCodec(entity.NewEventCodec(nil))
}

// NewSSEGatewayWithCodec creates an SSEGateway with an explicit event codec.
func NewSSEGatewayWithCodec(codec entity.EventCodec) *SSEGateway {
	return &SSEGateway{codec: codec}
}

// Convert directly marshals the event to JSON.
func (g *SSEGateway) Convert(event entity.StreamEvent) ([]byte, error) {
	return g.codec.MarshalEvent(event)
}

// compile-time interface checks.
var _ stream.StatelessGateway = (*SSEGateway)(nil)
