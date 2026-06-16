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
func (b *Backend) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.Backend != b.config.Ref() {
		return agentos.RunStatus{}, fmt.Errorf("%w: run backend %s/%s does not match http backend %s/%s",
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
func (b *Backend) Signal(ctx context.Context, runID string, signal agentos.Signal) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
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
func (b *Backend) Control(ctx context.Context, runID string, op agentos.ControlOperation) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	switch op {
	case agentos.ControlPause, agentos.ControlResume, agentos.ControlCancel:
	default:
		return fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
	}

	if err := b.doJSON(ctx, http.MethodPost, "/runs/"+runID+"/control", controlRequest{Operation: op}, nil); err != nil {
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
		return nil, errors.New("http backend: event subscriber is not configured")
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

func (b *Backend) doJSON(ctx context.Context, method, path string, input any, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, b.config.endpoint(path), body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range b.config.Headers {
		req.Header.Set(key, value)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(data))
	}
	if output == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
