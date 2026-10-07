package centrifugo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
)

// publishRequest is the Centrifugo server API body for POST /api/publish.
type publishRequest struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// Publisher implements stream.Publisher over the Centrifugo server HTTP API.
// Every backend publishes through it: map its internal output to AG-UI, then
// Publish — the server fans the event out to the run channel and retains it
// in the namespace history window.
type Publisher struct {
	client *http.Client
	config Config
}

// NewPublisher creates a Publisher for one Centrifugo endpoint.
func NewPublisher(config Config) (*Publisher, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	config = config.normalize()

	return &Publisher{client: config.HTTPClient, config: config}, nil
}

// Publish sends one AG-UI event to the run channel. The handle and event are
// validated locally first so a backend publishing garbage is rejected here,
// not at the frontend — mirroring the memstream publish boundary. Transport
// failures are returned to the caller (the runtime decides whether to fail
// open); the contract layer never swallows publish errors.
func (p *Publisher) Publish(ctx context.Context, handle *stream.Handle, ev *stream.Event) error {
	if err := handle.Validate(); err != nil {
		return err
	}

	if err := ev.Validate(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	body, err := json.Marshal(publishRequest{Channel: handle.Channel, Data: data})
	if err != nil {
		return fmt.Errorf("marshal publish request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.config.BaseURL+"/api/publish", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build publish request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Both header forms are accepted across Centrifugo v5/v6; send both so a
	// version bump never silently breaks publish.
	req.Header.Set("X-API-Key", p.config.APIKey)
	req.Header.Set("Authorization", "apikey "+p.config.APIKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", p.config.BaseURL, err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: status %d: %s", ErrPublishUnexpectedStatus, resp.StatusCode, errorBody(resp))
	}

	return nil
}

// errorBody reads the error response body for the message, capped at
// maxResponseBodySize. An unreadable body degrades to an empty message rather
// than masking the status error itself.
func errorBody(resp *http.Response) string {
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
	if err != nil {
		return ""
	}

	return string(data)
}
