package centrifugo

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/streamconformance"
	"github.com/stretchr/testify/require"
)

// TestCentrifugoConformance runs the shared data-plane conformance suite
// against the real Centrifugo transport: a publisher over the server HTTP API
// and a subscriber over centrifuge-go. Each sub-test starts a fresh embedded
// node so every scenario runs against a clean channel history — the same
// isolation the memstream suite gets from a fresh Bus.
func TestCentrifugoConformance(t *testing.T) {
	t.Parallel()

	handle := stream.NewHandle("$agentos:run:acme:conformance-run")

	streamconformance.RunStreamConformance(t, &streamconformance.ConformanceCase{
		Name:   "centrifugo",
		Handle: handle,
		New: func() (stream.Publisher, stream.Subscriber) {
			server := startTestServer(t)

			pub, err := NewPublisher(Config{BaseURL: server.URL, APIKey: apiKey})
			require.NoError(t, err, "publisher config")

			sub, err := NewSubscriber(Config{BaseURL: server.URL, APIKey: apiKey})
			require.NoError(t, err, "subscriber config")

			return pub, sub
		},
	})
}

// TestPublisherRejectsInvalidInput locks the local validation boundary: a
// backend publishing garbage must be rejected before anything reaches the
// wire, and transport errors are surfaced to the caller (the runtime decides
// whether to fail open, not the transport).
func TestPublisherRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	server := startTestServer(t)

	pub, err := NewPublisher(Config{BaseURL: server.URL, APIKey: apiKey})
	require.NoError(t, err)

	if err := pub.Publish(t.Context(), nil, stream.NewRunStarted("t", "r")); err == nil {
		t.Fatal("publish with nil handle: want error")
	}

	if err := pub.Publish(t.Context(), stream.NewHandle(channel), stream.NewEvent("")); err == nil {
		t.Fatal("publish with empty event type: want error")
	}
}

// TestPublisherSurfacesTransportErrors verifies a failed publish (e.g. wrong
// API key) returns an error instead of being swallowed.
func TestPublisherSurfacesTransportErrors(t *testing.T) {
	t.Parallel()

	server := startTestServer(t)

	pub, err := NewPublisher(Config{BaseURL: server.URL, APIKey: "wrong-key"})
	require.NoError(t, err)

	err = pub.Publish(t.Context(), stream.NewHandle(channel), stream.NewRunStarted("t", "r"))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPublishUnexpectedStatus)
}
