package orchestration

import (
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"time"
)

// ——— Workflow type names ———

const (
	AgentWorkflowName   = "agentfw.agent-workflow.v1"
	StreamWorkflowName  = "agentfw.stream-workflow.v1"
	AgentCommandSignal  = "agent-command"
	QueryRunStatus      = "agentfw.query.run-status"
)

// ——— Activity names ———

const (
	PrepareActivityName         = "agentfw.prepare.v1"
	LLMStepActivityName         = "agentfw.llm-step.v1"
	LLMStreamActivityName       = "agentfw.llm-stream.v1"
	ToolExecActivityName        = "agentfw.tool-exec.v1"
	ToolExecStreamActivityName  = "agentfw.tool-exec-stream.v1"
	InitStreamActivityName      = "agentfw.init-stream.v1"
	FinishStreamActivityName    = "agentfw.finish-stream.v1"
	TriggerFireWorkflowName     = "agentfw.trigger-fire.v1"
	FireTriggerActivityName     = "agentfw.fire-trigger.v1"
)

// ——— Activity input/output types ———

// PrepareInput is the input for the prep activity.
type PrepareInput struct {
	SystemPrompt string
	Message      string
	History      []entity.Message
	Tools        []entity.ToolDef
	Config       entity.LLMConfig
}

// PrepareOutput is the output of the prep activity.
// It contains assembled messages plus prep pipeline check results.
type PrepareOutput struct {
	Messages []entity.Message
	Tools    []entity.ToolDef
	Billing  *PrepBillingOutput
	Limits   *PrepLimitsOutput
	ToolDefs *PrepToolsOutput
	Errors   []string
}

// CanProceed returns true if all mandatory checks passed.
// Billing and limits gate execution; tool failures are non-fatal.
func (r *PrepareOutput) CanProceed() bool {
	if r.Billing != nil && !r.Billing.Approved {
		return false
	}
	if r.Limits != nil && !r.Limits.Approved {
		return false
	}
	return len(r.Errors) == 0
}

// PrepBillingInput is the input for billing/quota validation.
type PrepBillingInput struct {
	AccountID string
	ModelRef  string
}

// PrepBillingOutput is the output of billing validation.
type PrepBillingOutput struct {
	Approved   bool
	Remaining  int
	Currency   string
}

// PrepLimitsInput is the input for rate/context limit validation.
type PrepLimitsInput struct {
	AccountID    string
	MessageCount int
	ModelRef     string
}

// PrepLimitsOutput is the output of limit validation.
type PrepLimitsOutput struct {
	Approved     bool
	RateLimit    int
	ContextLimit int
}

// PrepToolsInput is the input for tool definition validation.
type PrepToolsInput struct {
	Tools []entity.ToolDef
}

// PrepToolsOutput is the output of tool validation.
type PrepToolsOutput struct {
	Tools      []entity.ToolDef
	Resolved   int
	Failed     []string
}

// PrepMCPInput is the input for MCP tool resolution.
type PrepMCPInput struct {
	ServerRefs []string
}

// PrepMCPOutput is the output of MCP tool resolution.
type PrepMCPOutput struct {
	Tools []entity.ToolDef
}

// LLMStepInput is the input for a single sync LLM call activity.
type LLMStepInput struct {
	RunID    string
	Messages []entity.Message
	Tools    []entity.ToolDef
	Config   entity.LLMConfig
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
	RunID      string
	ToolCallID string
	ToolName   string
	Args       map[string]any
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
	SessionID    string
	RunID        string
	SystemPrompt string
	Message      string
	History      []entity.Message
	Tools        []entity.ToolDef
	Config       entity.LLMConfig
}

// InitStreamOutput is the output of the streaming init activity.
type InitStreamOutput struct {
	Messages []entity.Message
	Tools    []entity.ToolDef
}

// LLMStreamInput is the input for a single streaming LLM call activity.
type LLMStreamInput struct {
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
}

// WorkflowResult is the deterministic workflow output payload.
type WorkflowResult struct {
	RunID          string
	LifecycleState string
	Step           int32
	CompletedAt    time.Time
}

// AgentWorkflowInput is the input for the step-level AgentWorkflow.
type AgentWorkflowInput struct {
	RunID          string
	SystemPrompt   string
	Message        string
	History        []entity.Message
	Tools          []entity.ToolDef
	Config         entity.LLMConfig
	ContinuePolicy ContinueAsNewPolicy
	Continuation   ContinuationPayload
}
