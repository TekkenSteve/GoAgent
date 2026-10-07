package centrifugo

import (
	"os"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/streamconformance"
	"github.com/stretchr/testify/require"
)

// conformanceEndpoint is the Centrifugo the suite runs against, and the API key
// for it.
//
// By default that is an embedded node from the centrifuge library. Setting
// CENTRIFUGO_TEST_URL and CENTRIFUGO_TEST_API_KEY points the suite at a real
// server instead — which is the only way to test the version actually deployed,
// because the embedded node is the library, not the server binary.
//
// The key is required rather than defaulted: the stack's key lives in
// configs/centrifugo/config.json, and a copy of it here would be a second
// source of truth that silently goes stale.
func conformanceEndpoint(t *testing.T) (baseURL, apiKeyForRun string) {
	t.Helper()

	external := os.Getenv("CENTRIFUGO_TEST_URL")
	if external == "" {
		return startTestServer(t).URL, apiKey
	}

	externalKey := os.Getenv("CENTRIFUGO_TEST_API_KEY")
	if externalKey == "" {
		t.Fatal("CENTRIFUGO_TEST_API_KEY is required alongside CENTRIFUGO_TEST_URL")
	}

	return external, externalKey
}

// TestCentrifugoConformance runs the shared data-plane conformance suite
// against the real Centrifugo transport: a publisher over the server HTTP API
// and a subscriber over centrifuge-go.
//
// Each sub-test starts a fresh embedded node so every scenario runs against a
// clean channel history — the same isolation the memstream suite gets from a
// fresh Bus. Point CENTRIFUGO_TEST_URL at a running server to run the same
// suite against the version the stack deploys.
func TestCentrifugoConformance(t *testing.T) {
	t.Parallel()

	handle := stream.NewHandle("agentos:run:acme:conformance-run")

	streamconformance.RunStreamConformance(t, &streamconformance.ConformanceCase{
		Name:   "centrifugo",
		Handle: handle,
		New: func() (stream.Publisher, stream.Subscriber) {
			baseURL, endpointAPIKey := conformanceEndpoint(t)

			pub, err := NewPublisher(Config{BaseURL: baseURL, APIKey: endpointAPIKey})
			require.NoError(t, err, "publisher config")

			sub, err := NewSubscriber(Config{BaseURL: baseURL, APIKey: endpointAPIKey})
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
