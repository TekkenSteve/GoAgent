package stream

import "fmt"

const (
	// DefaultVocabulary is the wire vocabulary a Handle carries unless
	// the control plane negotiates otherwise.
	DefaultVocabulary = "ag-ui/v1"
	// DefaultBatchMs is the publish batching hint used when BatchMs is unset.
	DefaultBatchMs = 100
)

// Handle is the only contract the control plane hands a backend at run start.
// Everything the backend needs to publish is here — it never learns about the
// control plane's auth, persistence, or frontend connections.
//
// The channel encodes the exact frontend session: backends publish to it and
// the bus (Centrifugo in production) delivers precisely to the frontends that
// hold a subscription for that channel.
type Handle struct {
	// Channel is the session channel, e.g. "$agentos:run:{tenant}:{run_id}".
	Channel string
	// Vocabulary is the wire event vocabulary ("ag-ui/v1").
	Vocabulary string
	// BatchMs hints the adapter to batch hot-path delta publishes; 0 means
	// DefaultBatchMs.
	BatchMs int
}

// NewHandle builds a handle with contract defaults applied.
func NewHandle(channel string) *Handle {
	return &Handle{Channel: channel, Vocabulary: DefaultVocabulary, BatchMs: DefaultBatchMs}
}

// Validate checks the handle. The channel must be non-empty; the vocabulary
// must be set (defaulted by NewHandle).
func (h *Handle) Validate() error {
	if h == nil {
		return fmt.Errorf("%w: stream handle is required", ErrInvalidStreamHandle)
	}

	if h.Channel == "" {
		return fmt.Errorf("%w: channel is required", ErrInvalidStreamHandle)
	}

	if h.Vocabulary == "" {
		return fmt.Errorf("%w: vocabulary is required", ErrInvalidStreamHandle)
	}

	return nil
}
