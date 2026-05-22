package stream

import (
	"github.com/TekkenSteve/GoAgent/entity"
)

// StatelessGateway converts a StreamEvent to a wire format byte slice.
// Each Convert call is independent — no buffering, no aggregation.
type StatelessGateway interface {
	Convert(event entity.StreamEvent) ([]byte, error)
}

// StatefulGateway extends StatelessGateway with buffered output and state reset.
// Used when events need to be aggregated or transformed across multiple events
// before being flushed (e.g., AGUI protocol).
type StatefulGateway interface {
	StatelessGateway
	Flush() ([]byte, error)
	Reset()
}

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
var _ StatelessGateway = (*SSEGateway)(nil)
