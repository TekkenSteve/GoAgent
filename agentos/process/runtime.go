package process

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

// Runtime is the durable business-process boundary for generic domain
// resources.
type Runtime interface {
	StartProcess(ctx context.Context, spec *Spec) (Status, error)
	StatusProcess(ctx context.Context, ref Ref) (Status, error)
	DescribeProcess(ctx context.Context, ref Ref) (Description, error)
	ListProcesses(ctx context.Context, scope *Scope) ([]Status, error)
	SignalProcess(ctx context.Context, ref Ref, signal *core.Signal) error
	ControlProcess(ctx context.Context, ref Ref, control *core.ControlRequest) error
	SubscribeProcess(ctx context.Context, scope *StreamScope) (core.Subscription, error)
	ListProcessEvents(ctx context.Context, scope *EventScope) ([]Event, error)
}

// LedgerRuntime records append-only decisions, evidence, prompts, responses,
// action references, and artifacts for generic AgentOS processes and resources.
type LedgerRuntime interface {
	AppendLedgerEntry(ctx context.Context, spec *LedgerEntrySpec) (LedgerEntry, error)
	ListLedgerEntries(ctx context.Context, scope *LedgerScope) ([]LedgerEntry, error)
}

// GovernedActionRuntime coordinates coarse-grained actions that require
// dry-run, risk review, approval, execution, cancellation, or compensation.
type GovernedActionRuntime interface {
	RequestAction(ctx context.Context, spec *GovernedActionSpec) (GovernedActionStatus, error)
	StatusAction(ctx context.Context, ref ActionRef) (GovernedActionStatus, error)
	ListActions(ctx context.Context, scope *ActionScope) ([]GovernedActionStatus, error)
	RecordActionDryRun(ctx context.Context, ref ActionRef, result *ActionDryRunResult) (GovernedActionStatus, error)
	ResolveActionApproval(ctx context.Context, ref ActionRef, decision *ActionApprovalDecision) (GovernedActionStatus, error)
	CompleteAction(ctx context.Context, ref ActionRef, result *ActionExecutionResult) (GovernedActionStatus, error)
	CancelAction(ctx context.Context, ref ActionRef, req *ActionCancelRequest) (GovernedActionStatus, error)
}

// BatchRuntime coordinates coarse-grained batch worksets and chunk progress
// without modeling each data record as a workflow.
type BatchRuntime interface {
	StartWorkset(ctx context.Context, spec *WorksetSpec) (WorksetStatus, error)
	StatusWorkset(ctx context.Context, ref WorksetRef) (WorksetStatus, error)
	ListWorksets(ctx context.Context, scope *WorksetScope) ([]WorksetStatus, error)
	RecordWorksetChunk(ctx context.Context, ref WorksetRef, result *WorksetChunkResult) (WorksetStatus, error)
	CancelWorkset(ctx context.Context, ref WorksetRef, control *core.ControlRequest) (WorksetStatus, error)
}

// ProjectionRuntime is the read-model boundary for REST, MCP, UI, and
// operators. Implementations read durable projections instead of querying
// Temporal workflow state on hot paths.
type ProjectionRuntime interface {
	GetResourceProjection(ctx context.Context, scope *ResourceProjectionScope) (ResourceProjection, error)
	ListResourceProjections(ctx context.Context, scope *ResourceProjectionListScope) ([]ResourceProjectionSummary, error)
}
