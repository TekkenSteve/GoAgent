package orchestration

import (
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	agentuc "github.com/TekkenSteve/GoAgent/internal/usecase/agent"
)

// ——— Workflow type names ———

// AgentWorkflowName and StreamWorkflowName are the Temporal workflow types
// for the native agent workflows; the remaining constants are the control
// signals and query name they handle.
const (
	AgentWorkflowName  = "agentfw.agent-workflow.v1"
	StreamWorkflowName = "agentfw.stream-workflow.v1"
	AgentCommandSignal = "agent-command"
	AgentMessageSignal = "agent-user-message"
	AgentCmdCancel     = "cancel"
	AgentCmdPause      = "pause"
	AgentCmdResume     = "resume"
	QueryRunStatus     = "agentfw.query.run-status"
)

// Lifecycle state constants.
const (
	LifecycleStateFailed    = "failed"
	LifecycleStateCanceled  = "canceled"
	LifecycleStateCompleted = "completed"
)

// ——— Activity names ———

// PrepareActivityName and the other activity name constants are the
// Temporal activity types registered by AgentActivities.
const (
	PrepareActivityName         = "agentfw.prepare.v1"
	LLMStepActivityName         = "agentfw.llm-step.v1"
	LLMStreamActivityName       = "agentfw.llm-stream.v1"
	ToolExecActivityName        = "agentfw.tool-exec.v1"
	ToolExecStreamActivityName  = "agentfw.tool-exec-stream.v1"
	InitStreamActivityName      = "agentfw.init-stream.v1"
	FinishStreamActivityName    = "agentfw.finish-stream.v1"
	SnapshotHistoryActivityName = "agentfw.snapshot-history.v1"
	LoadHistoryActivityName     = "agentfw.load-history.v1"
)

// ——— Activity input/output types ———

// PrepareInput is the input for the prep activity.
type PrepareInput struct {
	AccountID        string
	SystemPrompt     string
	Message          string
	History          []entity.Message
	Tools            []entity.ToolDef
	Config           entity.LLMConfig
	MCPServerConfigs []entity.MCPServerConfig
}

// PrepareOutput is the output of the prep activity.
// It contains assembled messages plus prep pipeline check results.
type PrepareOutput struct {
	Messages []entity.Message
	Tools    []entity.ToolDef
	Billing  *PrepBillingOutput
	ToolDefs *PrepToolsOutput
	// Warnings collects what failed without forbidding the run: an
	// unreachable MCP server, a malformed tool definition, a billing lookup
	// that errored. They are reported to operators, and a tool call that does
	// fail reaches the model as a tool error it can react to.
	Warnings []string
}

// CanProceed reports whether execution may start.
//
// Billing gates it: it is the one check that says this run must not run at
// all. Everything that merely failed is reported in Warnings and does not stop
// the run — an agent that cannot reach one toolset can still answer with the
// rest.
//
// Concurrency admission is deliberately absent: a limit enforced from a
// process-local counter is not a limit on a fleet, and a gate that cannot deny
// is worse than none. A real one needs a shared counter with leases and a
// release path that survives a crashed worker.
func (r *PrepareOutput) CanProceed() bool {
	return r.Billing == nil || r.Billing.Approved
}

// PrepBillingInput is the input for billing/quota validation.
type PrepBillingInput struct {
	AccountID string
	ModelRef  string
}

// PrepBillingOutput is the output of billing validation.
type PrepBillingOutput struct {
	Approved  bool
	Remaining int
	Currency  string
}

// PrepToolsInput is the input for tool definition validation.
type PrepToolsInput struct {
	Tools []entity.ToolDef
}

// PrepToolsOutput is the output of tool validation.
type PrepToolsOutput struct {
	Tools    []entity.ToolDef
	Resolved int
	Failed   []string
}

// PrepMCPInput is the input for MCP tool resolution.
type PrepMCPInput struct {
	// ServerConfigs is the list of MCP server configurations to connect to.
	ServerConfigs []entity.MCPServerConfig
}

// PrepMCPOutput is the output of MCP tool resolution.
type PrepMCPOutput struct {
	Tools []entity.ToolDef
	// Failures are the MCP servers that could not be connected. Advisory:
	// their tools are simply absent from this run.
	Failures []string
}

// LLMStepInput is the input for a single sync LLM call activity.
type LLMStepInput struct {
	AccountID string
	RunID     string
	Messages  []entity.Message
	Tools     []entity.ToolDef
	Config    entity.LLMConfig
}

// LLMStepOutput is the output of a single LLM call activity.
type LLMStepOutput struct {
	Content      string
	ToolCalls    []entity.ToolCall
	Usage        entity.Usage
	FinishReason string
}

// ToolInput is the input for a single tool execution activity.
type ToolInput struct {
	AccountID  string
	ProjectID  string
	RunID      string
	ToolCallID string
	ToolName   string
	// Args is the JSON arguments string, verbatim as the model emitted it.
	// It stays a string for the whole hop so no encoding round-trip can
	// alter it: re-marshaling a decoded map rewrites number literals and
	// silently corrupts values beyond float64 precision, and the tool sees
	// exactly what the model sent or nothing at all.
	Args string
}

// identity is the tenant and run this tool call belongs to. It travels with
// the call because the tool pipeline authorizes the call as the run: an
// identity that stops at the activity is an unauthorized call.
func (in *ToolInput) identity() agentuc.RunIdentity {
	return agentuc.RunIdentity{
		RunID:     in.RunID,
		AccountID: in.AccountID,
		ProjectID: in.ProjectID,
	}
}

// toolCall presents the input as the model's tool call. The activities hand
// this to the executor, so the arguments the model emitted are the arguments
// the tool receives.
func (in *ToolInput) toolCall() entity.ToolCall {
	return entity.ToolCall{
		ID:   in.ToolCallID,
		Type: "function",
		Function: entity.ToolCallFunction{
			Name:      in.ToolName,
			Arguments: in.Args,
		},
	}
}

// ToolOutput is the output of a single tool execution activity.
type ToolOutput struct {
	Output     string
	ExitCode   int
	IsError    bool
	DurationMs int64
}

// InitStreamInput is the input for the streaming init activity.
type InitStreamInput struct {
	AccountID        string
	ProjectID        string
	SessionID        string
	RunID            string
	SystemPrompt     string
	Message          string
	History          []entity.Message
	Tools            []entity.ToolDef
	Config           entity.LLMConfig
	MCPServerConfigs []entity.MCPServerConfig
	TaskQueues       WorkflowTaskQueues
	// Delegate is the delegation tree's allowance, shared with the
	// step-level agent loop.
	Delegate DelegateBudget
}

// DelegateBudget is what a delegation tree may still spend. It travels down
// with every child, so the limit is the tree's and not one workflow's.
type DelegateBudget struct {
	// Depth is how many delegation levels separate this workflow from the
	// run's root. The root is 0.
	Depth int
	// MaxDepth bounds the tree. Zero takes defaultMaxDelegateDepth, and a
	// child inherits the limit its parent resolved.
	MaxDepth int
	// TokenBudget bounds what the tree may spend; zero means no bound.
	TokenBudget int64
	// BaselineSpent is what the tree had spent before this workflow
	// started: its ancestors plus their earlier children.
	BaselineSpent int64
	// OwnTokensSpent is what this workflow and its own children have spent.
	// A child reports this as a delta, which is what keeps the tree's total
	// linear instead of counting an ancestor's cost once per level.
	OwnTokensSpent int64
}

// InitStreamOutput is the output of the streaming init activity.
type InitStreamOutput struct {
	Messages []entity.Message
	Tools    []entity.ToolDef
}

// LLMStreamInput is the input for a single streaming LLM call activity.
type LLMStreamInput struct {
	AccountID string
	SessionID string
	RunID     string
	Messages  []entity.Message
	Tools     []entity.ToolDef
	Config    entity.LLMConfig
}

// LLMStreamOutput is the output of a single streaming LLM call activity.
type LLMStreamOutput struct {
	ToolCalls    []entity.ToolCall
	Usage        entity.Usage
	FinishReason string
}

// ——— Shared types ———

// RunStatus is a stable status payload for query and stream layers.
type RunStatus struct {
	RunID          string
	LifecycleState string
	Step           int32
	Reason         string
	UpdatedAt      time.Time
	Output         string // final assistant text output; carried for delegation
}

// UserMessageSignal carries a user turn into a running native agent workflow.
type UserMessageSignal struct {
	MessageID      string
	IdempotencyKey string
	Content        string
	Attachments    []map[string]any
	Context        map[string]any
	ReceivedAtUnix int64
}

// WorkflowResult is the deterministic workflow output payload.
type WorkflowResult struct {
	RunID          string
	LifecycleState string
	// Reason says why the run reached its state. A completed run that hit the
	// round cap carries reasonMaxRounds; a run that finished its work carries
	// none, and the two are different situations.
	Reason      string
	Step        int32
	CompletedAt time.Time
	Output      string // final assistant text output; populated for delegation
	// TokensSpent is what this workflow and its delegation subtree consumed,
	// not counting what the tree had already spent when it started. A parent
	// adds it to its own running total, which is how a budget stays global
	// across a delegation tree without double counting.
	TokensSpent int64
}

// AgentWorkflowInput is the input for the step-level AgentWorkflow.
type AgentWorkflowInput struct {
	AccountID        string
	ProjectID        string
	RunID            string
	SystemPrompt     string
	Message          string
	History          []entity.Message
	Tools            []entity.ToolDef
	Config           entity.LLMConfig
	MCPServerConfigs []entity.MCPServerConfig
	ContinuePolicy   ContinueAsNewPolicy
	Continuation     ContinuationPayload
	AwaitUserInput   bool
	// AwaitUserInputTimeout bounds the waiting_input state so a run never
	// waits for user input indefinitely. Zero uses defaultAwaitUserInputTimeout.
	AwaitUserInputTimeout time.Duration
	TaskQueues            WorkflowTaskQueues

	// Delegate is the delegation tree's allowance. Both agent loops carry
	// it, so neither can recurse past the limit or past the budget.
	Delegate DelegateBudget
}

// WorkflowInput wraps the business orchestration input with Temporal worker
// routing. Queue names are infrastructure details and stay out of internal/entity.
type WorkflowInput struct {
	Input      entity.OrchestrationInput
	TaskQueues WorkflowTaskQueues
}

// WorkflowTaskQueues carries worker routing decisions into workflow history.
type WorkflowTaskQueues struct {
	NativeControl string
	NativeLLM     string
	NativeTool    string
	Stream        string
}
