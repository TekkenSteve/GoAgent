// Package nexusapi defines the AgentOS Nexus service contract: the service and
// operation names plus every operation's input and output type.
//
// Callers depend on this package alone. The handler implementation lives where
// the worker is assembled (agentos/temporal), so a caller in another team,
// Namespace or language can adopt the contract without pulling in AgentOS
// internals. Changing an operation name or a field on these types is a breaking
// contract change: add a new operation or an optional field instead.
package nexusapi

import (
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

const (
	// ServiceName is the Nexus service exposed on the AgentOS endpoint. The
	// endpoint itself is a deployment resource configured per environment.
	ServiceName = "AgentOS"

	// RunOperationName starts a run asynchronously. The operation completes when
	// the run reaches a terminal lifecycle state, and canceling the operation
	// cancels the run.
	RunOperationName = "run"
	// RunSignalOperationName delivers a signal to one run.
	RunSignalOperationName = "run.signal"
	// RunControlOperationName delivers a pause, resume or cancel control request
	// to one run.
	RunControlOperationName = "run.control"
	// RunStatusOperationName reads one run's current status.
	RunStatusOperationName = "run.status"
)

// RunRequest is the wire input of the async run operation.
type RunRequest struct {
	// RequestID is the caller-owned idempotency key. Retried operation starts
	// with the same RequestID bind to the same run-operation workflow and the
	// same derived run, so the promise is exactly-once for the caller.
	RequestID string `json:"request_id"`
	// Spec describes the run to start. When Spec.RunID is empty it is derived
	// from RequestID.
	Spec *agentos.RunSpec `json:"spec"`
	// PlanScope binds the run to one plan node execution so the handler records
	// plan-node ownership atomically with the start. Callers outside the plan
	// plane leave it nil.
	PlanScope *RunPlanScope `json:"plan_scope,omitempty"`
}

// RunPlanScope identifies the plan node execution owning a scoped run.
type RunPlanScope struct {
	PlanID string `json:"plan_id"`
	NodeID string `json:"node_id"`
}

// RunOutput is the durable result of the async run operation: the run's
// terminal status.
type RunOutput struct {
	Status agentos.RunStatus `json:"status"`
}

// RunSignalRequest delivers a signal to one run.
type RunSignalRequest struct {
	RunID  string              `json:"run_id"`
	Signal *agentoscore.Signal `json:"signal"`
}

// RunControlRequest delivers a control request to one run.
type RunControlRequest struct {
	RunID   string                      `json:"run_id"`
	Control *agentoscore.ControlRequest `json:"control"`
}

// RunStatusRequest reads one run's status.
type RunStatusRequest struct {
	RunID string `json:"run_id"`
}

// RunStatusOutput returns one run's status.
type RunStatusOutput struct {
	Status agentos.RunStatus `json:"status"`
}
