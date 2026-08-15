// Package centrifugo implements the production data-plane transport on
// Centrifugo: a Publisher over the server HTTP API and a Subscriber over the
// centrifuge-go websocket client. Any bus that satisfies agentos/stream's
// conformance suite is a drop-in; this package is the production one.
package centrifugo

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config describes one Centrifugo data-plane endpoint.
//
// History retention is NOT part of the client config: it is a server-side
// namespace concern. The operator must configure the $agentos:run:* namespace
// with history_size / history_ttl (and force_positioning / force_recovery) so
// that every publish to a run channel retains the replay window the
// Subscriber reads. The embedded test harness is the exception — it passes
// history flags per-publish because the in-memory broker has no namespace.
type Config struct {
	// BaseURL is the Centrifugo server root, e.g. "http://127.0.0.1:8000".
	BaseURL string

	// APIKey authenticates server API publish requests (X-API-Key header).
	APIKey string

	// ConnectTimeout bounds websocket connection and subscription setup.
	// Zero value means defaultConnectTimeout.
	ConnectTimeout time.Duration

	// HTTPClient overrides the client used for publish requests. Zero value
	// means http.DefaultClient.
	HTTPClient *http.Client
}

const (
	// defaultConnectTimeout bounds websocket establishment and subscribe.
	defaultConnectTimeout = 5 * time.Second

	// maxResponseBodySize caps how much of an error response we read.
	maxResponseBodySize = 4096

	// subscriptionBuffer is the per-subscription live delivery buffer before a
	// slow consumer drops, mirroring the memstream fan-out policy.
	subscriptionBuffer = 256

	// historyLimit asks the server for the whole retained window. The server
	// clamps to its own namespace limits; the memory broker returns whatever
	// history it retained.
	historyLimit = 100000
)

// normalize applies defaults to a copy of the config.
func (c Config) normalize() Config {
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}

	if c.HTTPClient == nil {
		c.HTTPClient = http.DefaultClient
	}

	return c
}

// validate checks the config before it is used.
func (c Config) validate() error {
	if c.BaseURL == "" {
		return fmt.Errorf("%w: base url is required", ErrInvalidConfig)
	}

	parsed, err := url.Parse(c.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%w: invalid base url %q", ErrInvalidConfig, c.BaseURL)
	}

	if c.APIKey == "" {
		return fmt.Errorf("%w: api key is required", ErrInvalidConfig)
	}

	return nil
}

// wsEndpoint derives the websocket connection endpoint from the HTTP base URL.
// The centrifuge-go client requires a ws:// or wss:// scheme.
func wsEndpoint(baseURL string) string {
	return "ws" + strings.TrimPrefix(baseURL, "http") + "/connection/websocket"
}
