package v1

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
	"github.com/go-playground/validator/v10"
)

// CancelWorkflowFn cancels a remote workflow by its workflow ID.
// Set by app.go when Temporal is available; nil for in-process mode.
type CancelWorkflowFn func(ctx context.Context, workflowID string) error

// SignalWorkflowFn sends a named signal with payload to a running workflow.
// Set by app.go when Temporal is available; nil for in-process mode.
type SignalWorkflowFn func(ctx context.Context, workflowID, signalName string, arg interface{}) error

// V1 -.
type V1 struct {
	t   usecase.AgentExecutor
	o   usecase.OrchestrationExecutor
	h   usecase.HistoryQuery
	s   usecase.StreamExecutor
	l   logger.Interface
	v   *validator.Validate
	rdb *redis.Redis

	// Event Sourcing components (Phase 2+)
	eventStore stream.EventStore
	subscriber stream.Subscriber
	gateway    stream.StatelessGateway

	// WebSocket Hub for connection tracking (Phase 4)
	wsHub          *stream.WebSocketHub
	cancelWorkflow  CancelWorkflowFn  // non-nil only when running with Temporal
	signalWorkflow  SignalWorkflowFn  // non-nil only when running with Temporal
}
