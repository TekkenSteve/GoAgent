// Package grpcbackend implements an AgentOS backend over gRPC.
package grpcbackend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var errGRPCBackendSubscriberNotConfigured = errors.New("grpc backend: event subscriber is not configured")

// Backend adapts a remote gRPC agent runtime to AgentOS.
type Backend struct {
	conn       *grpc.ClientConn
	subscriber agentosruntime.EventSubscriber
	lifecycle  agentosruntime.LifecyclePublisher
	config     Config
}

// NewBackend creates a gRPC backend.
func NewBackend(subscriber agentosruntime.EventSubscriber, lifecycle agentosruntime.LifecyclePublisher, config *Config) (*Backend, error) {
	if err := config.normalize(); err != nil {
		return nil, err
	}

	options := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	}
	if config.Insecure {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})))
	}

	if config.Authority != "" {
		options = append(options, grpc.WithAuthority(config.Authority))
	}

	conn, err := grpc.NewClient(config.Target, options...)
	if err != nil {
		return nil, fmt.Errorf("grpc backend - new client: %w", err)
	}

	return &Backend{
		conn:       conn,
		subscriber: subscriber,
		lifecycle:  lifecycle,
		config:     *config,
	}, nil
}

// Start starts a remote gRPC agent run.
func (b *Backend) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	if spec == nil {
		return agentos.RunStatus{}, fmt.Errorf("%w: run spec is required", agentoscore.ErrInvalidRunSpec)
	}

	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	if spec.Backend != b.config.Ref() {
		return agentos.RunStatus{}, fmt.Errorf(
			"%w: run backend %s/%s does not match grpc backend %s/%s",
			agentoscore.ErrInvalidBackendRef,
			spec.Backend.Kind,
			spec.Backend.Name,
			b.config.Ref().Kind,
			b.config.Ref().Name,
		)
	}

	var status agentos.RunStatus
	if err := b.invoke(ctx, b.config.Methods.Start, startRequestFromSpec(spec), &status); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("grpc backend - start: %w", err)
	}

	if status.RunID == "" {
		status.RunID = spec.RunID
	}

	b.publishStarted(ctx, spec, &status)

	return status, nil
}

// Signal sends a business signal to a remote gRPC agent run.
func (b *Backend) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	if signal == nil {
		return fmt.Errorf("%w: signal is required", agentoscore.ErrInvalidSignal)
	}

	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentoscore.ErrInvalidSignal)
	}

	var response emptyResponse
	if err := b.invoke(ctx, b.config.Methods.Signal, signalRequestFromSignal(runID, signal), &response); err != nil {
		return fmt.Errorf("grpc backend - signal: %w", err)
	}

	return nil
}

// Control sends a lifecycle control operation to a remote gRPC agent run.
func (b *Backend) Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	if err := agentoscore.ValidateControlRequest(control); err != nil {
		return err
	}

	var response emptyResponse
	if err := b.invoke(ctx, b.config.Methods.Control, controlRequestFromControl(runID, control), &response); err != nil {
		return fmt.Errorf("grpc backend - control: %w", err)
	}

	return nil
}

// Status returns remote gRPC agent run status.
func (b *Backend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	if runID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunSpec)
	}

	var status agentos.RunStatus
	if err := b.invoke(ctx, b.config.Methods.Status, statusRequest{RunID: runID}, &status); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("grpc backend - status: %w", err)
	}

	if status.RunID == "" {
		status.RunID = runID
	}

	b.publishStatus(ctx, runID, &status)

	return status, nil
}

// Subscribe returns the shared AgentOS event stream for the run.
func (b *Backend) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	if b.subscriber == nil {
		return nil, errGRPCBackendSubscriberNotConfigured
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

// Capabilities reports baseline gRPC backend features.
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

// publishStarted mirrors a successful Start onto the data plane. A nil
// lifecycle adapter (unwired backend) degrades to a no-op.
func (b *Backend) publishStarted(ctx context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) {
	if b.lifecycle == nil {
		return
	}

	b.lifecycle.PublishStarted(ctx, spec, status)
}

// publishStatus mirrors a Status observation onto the data plane, publishing
// the run's terminal milestone once the remote reports one.
func (b *Backend) publishStatus(ctx context.Context, runID string, status *agentos.RunStatus) {
	if b.lifecycle == nil {
		return
	}

	b.lifecycle.PublishStatus(ctx, runID, status)
}

// Close closes the underlying gRPC client connection.
func (b *Backend) Close() error {
	return b.conn.Close()
}

func (b *Backend) invoke(ctx context.Context, method string, request, response any) error {
	return b.conn.Invoke(ctx, b.config.method(method), request, response, grpc.CallContentSubtype(jsonCodecName))
}
