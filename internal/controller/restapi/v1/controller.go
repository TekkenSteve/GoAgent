package v1

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/eventing"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/go-playground/validator/v10"
)

// CancelWorkflowFn cancels a remote workflow by its workflow ID.
// Set by app.go when Temporal is available; nil for in-process mode.
type CancelWorkflowFn func(ctx context.Context, workflowID string) error

// SignalWorkflowFn sends a named signal with payload to a running workflow.
// Set by app.go when Temporal is available; nil for in-process mode.
type SignalWorkflowFn func(ctx context.Context, workflowID, signalName string, arg any) error

// V1 -.
type V1 struct {
	t usecase.AgentExecutor
	o usecase.OrchestrationExecutor
	l logger.Interface
	v *validator.Validate

	eventIngest     *eventing.Service
	cancelWorkflow  CancelWorkflowFn // non-nil only when running with Temporal
	signalWorkflow  SignalWorkflowFn // non-nil only when running with Temporal
	agentOSRuntime  agentos.Runtime
	planRuntime     agentos.PlanRuntime
	platformRuntime agentosplatform.Runtime
}
