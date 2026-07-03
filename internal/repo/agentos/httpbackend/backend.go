package httpbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

var (
	errHTTPBackendSubscriberNotConfigured = errors.New("http backend: event subscriber is not configured")
	errHTTPBackendUnexpectedStatus        = errors.New("http backend: unexpected status")
)

// Backend adapts a remote HTTP agent runtime to AgentOS.
type Backend struct {
	client     *http.Client
	subscriber agentosruntime.EventSubscriber
	config     Config
}

// NewBackend creates an HTTP backend.
func NewBackend(client *http.Client, subscriber agentosruntime.EventSubscriber, config Config) (*Backend, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	if client == nil {
		client = http.DefaultClient
	}

	return &Backend{
		client:     client,
		subscriber: subscriber,
		config:     config,
	}, nil
}

// Start starts a remote HTTP agent run.
func (b *Backend) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	if spec == nil {
		return agentos.RunStatus{}, fmt.Errorf("%w: run spec is required", agentos.ErrInvalidRunSpec)
	}

	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	if spec.Backend != b.config.Ref() {
		return agentos.RunStatus{}, fmt.Errorf(
			"%w: run backend %s/%s does not match http backend %s/%s",
			agentos.ErrInvalidBackendRef,
			spec.Backend.Kind,
			spec.Backend.Name,
			b.config.Ref().Kind,
			b.config.Ref().Name,
		)
	}

	var status agentos.RunStatus
	if err := b.doJSON(ctx, http.MethodPost, "/runs", startRequestFromSpec(spec), &status); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("http backend - start: %w", err)
	}

	if status.RunID == "" {
		status.RunID = spec.RunID
	}

	return status, nil
}

// Signal sends a business signal to a remote HTTP agent run.
func (b *Backend) Signal(ctx context.Context, runID string, signal *agentos.Signal) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	if signal == nil {
		return fmt.Errorf("%w: signal is required", agentos.ErrInvalidSignal)
	}

	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentos.ErrInvalidSignal)
	}

	if err := b.doJSON(ctx, http.MethodPost, "/runs/"+runID+"/signals", signalRequestFromSignal(signal), nil); err != nil {
		return fmt.Errorf("http backend - signal: %w", err)
	}

	return nil
}

// Control sends a lifecycle control operation to a remote HTTP agent run.
func (b *Backend) Control(ctx context.Context, runID string, control *agentos.ControlRequest) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}

	if err := b.doJSON(ctx, http.MethodPost, "/runs/"+runID+"/control", controlRequestFromControl(control), nil); err != nil {
		return fmt.Errorf("http backend - control: %w", err)
	}

	return nil
}

// Status returns remote HTTP agent run status.
func (b *Backend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	if runID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	var status agentos.RunStatus
	if err := b.doJSON(ctx, http.MethodGet, "/runs/"+runID+"/status", nil, &status); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("http backend - status: %w", err)
	}

	if status.RunID == "" {
		status.RunID = runID
	}

	return status, nil
}

// Subscribe returns the shared AgentOS event stream for the run.
func (b *Backend) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	if b.subscriber == nil {
		return nil, errHTTPBackendSubscriberNotConfigured
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

// Capabilities reports baseline HTTP backend features.
func (b *Backend) Capabilities() agentosruntime.BackendCapabilities {
	return agentosruntime.BackendCapabilities{
		SupportsSignal:            true,
		SupportsSignalUserMessage: true,
		SupportsPause:             true,
		SupportsResume:            true,
		SupportsCancel:            true,
		SupportsStreaming:         b.subscriber != nil,
	}
}

const maxResponseBodySize = 4096

func checkHTTPResponse(resp *http.Response) error {
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	return fmt.Errorf("%w: status %d: %s", errHTTPBackendUnexpectedStatus, resp.StatusCode, string(data))
}

func (b *Backend) doJSON(ctx context.Context, method, path string, input, output any) error {
	body, err := jsonBody(input)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, b.config.endpoint(path), body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	applyJSONHeaders(req, input != nil, b.config.Headers)

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkHTTPResponse(resp); err != nil {
		return err
	}

	return decodeJSONResponse(resp, output)
}

func jsonBody(input any) (io.Reader, error) {
	if input == nil {
		return nil, nil
	}

	data, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	return bytes.NewReader(data), nil
}

func applyJSONHeaders(req *http.Request, hasInput bool, headers map[string]string) {
	req.Header.Set("Accept", "application/json")

	if hasInput {
		req.Header.Set("Content-Type", "application/json")
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}
}

func decodeJSONResponse(resp *http.Response, output any) error {
	if output == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
