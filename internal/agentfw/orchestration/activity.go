// Package orchestration implements the Temporal workflows and activities
// that execute step-based agent runs.
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	agentuc "github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	billinguc "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

const prepCompletePercent = 100

// errHistoryBlobStoreRequired reports that a history snapshot cannot be
// restored because no blob store is configured. Static so classification and
// wrapping stay stable across call sites.
var errHistoryBlobStoreRequired = errors.New("blob store is required to restore history snapshot")

// AgentActivities provides Temporal activity implementations for agent execution.
type AgentActivities struct {
	agentUC    *agentuc.UseCase
	billingUC  *billinguc.UseCase
	mcpManager MCPManagerProvider
	// blobStore backs the claim-check history snapshot (see
	// SnapshotHistoryActivity). Optional: without it, long conversations stay
	// in workflow state and continue-as-new carries no snapshot.
	blobStore artifactBlobStore
	logger    logger.Interface
	// streamPub is the data-plane publisher every streaming activity writes
	// its AG-UI timeline through (fail-open). The app assembles it via the
	// streaming data plane (memstream default / Centrifugo); nil keeps the
	// activities running without a live mirror.
	streamPub agentosstream.Publisher
	// facts is the milestone recorder every streaming activity makes its
	// milestones durable through (fail-hard: a persist error fails the
	// activity so Temporal retries the fact). Nil keeps the runtime a pure
	// bus mirror.
	facts *streamadapter.MilestoneRecorder
}

// artifactBlobStore is the subset of artifact.BlobStore needed for history
// snapshots (payloads written outside Temporal history).
type artifactBlobStore interface {
	Put(ctx context.Context, key string, payload []byte) (artifact.BlobObject, error)
	Get(ctx context.Context, uri string) ([]byte, error)
}

// MCPManagerProvider is the subset of the MCP manager needed by activities.
// Server configs are passed in at Prep time,
// and connections are established on demand.
type MCPManagerProvider interface {
	// EnsureConnected JIT-connects to the given MCP servers (if not already
	// connected), discovers their tools, and returns the combined definitions
	// plus any connection errors. Tool registration for execution happens
	// internally in the manager.
	EnsureConnected(ctx context.Context, configs []entity.MCPServerConfig) ([]entity.ToolDef, []string)
}

// NewAgentActivities creates activities wired to the agent usecase. The
// streaming activities publish onto the data plane only when a publisher is
// attached via WithStreamPublisher.
func NewAgentActivities(uc *agentuc.UseCase, l logger.Interface) *AgentActivities {
	return &AgentActivities{agentUC: uc, logger: l}
}

// WithMCPManager sets the MCP manager for tool discovery in PrepareActivity.
func (a *AgentActivities) WithMCPManager(m MCPManagerProvider) *AgentActivities {
	a.mcpManager = m

	return a
}

// WithBilling sets the billing usecase for credit checks and usage deduction.
func (a *AgentActivities) WithBilling(uc *billinguc.UseCase) *AgentActivities {
	a.billingUC = uc

	return a
}

// WithBlobStore enables claim-check history snapshots for Continue-As-New
// (see SnapshotHistoryActivity). Without a blob store, workflows keep the
// full conversation in their own state.
func (a *AgentActivities) WithBlobStore(store artifactBlobStore) *AgentActivities {
	a.blobStore = store

	return a
}

// WithStreamPublisher attaches the data-plane publisher the streaming
// activities write their AG-UI timeline through. The app wires it via the
// streaming data plane; without it the runtime runs without a live mirror.
// Publishing is fail-open, so a dead bus never breaks a run.
func (a *AgentActivities) WithStreamPublisher(pub agentosstream.Publisher) *AgentActivities {
	a.streamPub = pub

	return a
}

// WithMilestoneRecorder makes every streaming activity's milestones durable
// at the writer: each fact is appended (with its fact-log publication intent)
// before the bus publish, and a persist failure fails the activity so the
// platform retries it. Optional — without it the runtime is a pure mirror.
func (a *AgentActivities) WithMilestoneRecorder(recorder *streamadapter.MilestoneRecorder) *AgentActivities {
	a.facts = recorder

	return a
}

// ——— Activities (step-level, Temporal-native) ———.
type billingResult struct {
	out *PrepBillingOutput
	err error
}

// PrepareActivity runs the full prep pipeline: tool validation, message assembly,
// billing, and tool definition validation. It returns a composite PrepareOutput
// with all check results, the warnings they produced, and a CanProceed() gate
// for the workflow.
func (a *AgentActivities) PrepareActivity(ctx context.Context, input *PrepareInput) (*PrepareOutput, error) {
	a.logger.Info("PrepareActivity: started, mcpManager=%v serverConfigs=%d", a.mcpManager != nil, len(input.MCPServerConfigs))

	billingRes, toolsOut, mcpOut := a.runPrepChecks(ctx, input)
	a.logger.Info("PrepareActivity: pre-checks done, billing.err=%v", billingRes.err)

	// Warnings, not failures: none of these stops the run, and each one is
	// reported so an operator can see a degraded toolset.
	var warnings []string

	if billingRes.err != nil {
		warnings = append(warnings, fmt.Sprintf("billing: %v", billingRes.err))
	}

	for _, name := range toolsOut.Failed {
		warnings = append(warnings, fmt.Sprintf("tool: %s missing name or type", name))
	}

	for _, failure := range mcpOut.Failures {
		warnings = append(warnings, fmt.Sprintf("mcp: %s", failure))
	}

	// Run Prep for message assembly
	req := agentuc.PrepRequest{
		SystemPrompt: input.SystemPrompt,
		UserMessage:  input.Message,
		History:      input.History,
		Tools:        input.Tools,
		Config:       input.Config,
	}

	prepResult, err := a.agentUC.Prep(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("PrepareActivity - Prep: %w", err)
	}

	a.logger.Info("PrepareActivity: Prep done, messages=%d", len(prepResult.Messages))

	// Merge MCP-discovered tools with the prep result tools
	allTools := prepResult.Tools
	allTools = append(allTools, mcpOut.Tools...)

	a.logger.Info("PrepareActivity: completed, warnings=%v", warnings)

	return &PrepareOutput{
		Messages: prepResult.Messages,
		Tools:    allTools,
		Billing:  billingRes.out,
		ToolDefs: toolsOut,
		Warnings: warnings,
	}, nil
}

// runPrepChecks runs the billing check concurrently with inline tools/MCP
// validation: billing is the one check that can deny the run, and it is a
// remote call, so it overlaps with the local work.
func (a *AgentActivities) runPrepChecks(ctx context.Context, input *PrepareInput) (billingRes billingResult, toolsOut *PrepToolsOutput, mcpOut *PrepMCPOutput) {
	billingCh := make(chan billingResult, 1)

	go func() {
		billing, err := a.prepBilling(ctx, PrepBillingInput{
			AccountID: input.AccountID,
			ModelRef:  input.Config.Model,
		})
		billingCh <- billingResult{billing, err}
	}()

	// Tool validation runs inline
	toolsOut = a.prepTools(ctx, PrepToolsInput{Tools: input.Tools})

	// MCP tool resolution runs in the prep pipeline
	mcpOut = a.prepMCP(ctx, PrepMCPInput{
		ServerConfigs: input.MCPServerConfigs,
	})

	billingRes = <-billingCh

	return billingRes, toolsOut, mcpOut
}

// prepBilling validates account billing/quota.
// Checks the account balance; rejects if balance is zero or negative.
func (a *AgentActivities) prepBilling(ctx context.Context, input PrepBillingInput) (*PrepBillingOutput, error) {
	if a.billingUC == nil || input.AccountID == "" {
		return &PrepBillingOutput{
			Approved:  true,
			Remaining: 0,
		}, nil
	}

	acct, err := a.billingUC.GetBalance(ctx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("prepBilling - GetBalance: %w", err)
	}

	if acct.Balance <= 0 {
		return &PrepBillingOutput{
			Approved:  false,
			Remaining: 0,
			Currency:  acct.Currency,
		}, nil
	}

	return &PrepBillingOutput{
		Approved:  true,
		Remaining: int(acct.Balance),
		Currency:  acct.Currency,
	}, nil
}

// prepTools validates tool definitions and resolves any tool references.
func (a *AgentActivities) prepTools(_ context.Context, input PrepToolsInput) *PrepToolsOutput {
	var failed []string

	for _, tool := range input.Tools {
		if tool.Function.Name == "" {
			failed = append(failed, "<unnamed>")

			continue
		}

		if tool.Type == "" {
			failed = append(failed, tool.Function.Name)

			continue
		}
	}

	return &PrepToolsOutput{
		Tools:    input.Tools,
		Resolved: len(input.Tools) - len(failed),
		Failed:   failed,
	}
}

// prepMCP resolves MCP server references into tool definitions via JIT connection.
// Server configs are passed in, connections are established
// on demand during Prep. Returns empty results without error if no manager configured.
func (a *AgentActivities) prepMCP(ctx context.Context, input PrepMCPInput) *PrepMCPOutput {
	if a.mcpManager == nil || len(input.ServerConfigs) == 0 {
		return &PrepMCPOutput{}
	}

	// JIT: connect to servers, discover tools on demand
	tools, errs := a.mcpManager.EnsureConnected(ctx, input.ServerConfigs)

	return &PrepMCPOutput{
		Tools:    tools,
		Failures: errs,
	}
}

// LLMStepActivity performs a single sync LLM call and returns the result.
// Deducts usage cost from account balance after a successful LLM call.
// Long inference is reported via heartbeats so a live activity is never
// marked dead and retried (which would duplicate the LLM call and its cost).
func (a *AgentActivities) LLMStepActivity(ctx context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
	a.logger.Info("LLMStepActivity: started, model=%s messages=%d tools=%d", input.Config.Model, len(input.Messages), len(input.Tools))

	// The non-stream path owes the same timeline as the streaming one: the
	// completed assistant message is one delta plus the flush that closes
	// it, so TEXT_MESSAGE_END — with its content — lands as a fact.
	writer := &streamEventWriter{publish: a.newPublishWriter(input.AccountID, input.RunID, input.RunID)}

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "llm-inference", WrittenEvents: writer.written.Load()}
	})
	defer stopHeartbeat()

	result, err := retryRateLimited(ctx, func() (*agentuc.LLMStepResult, error) {
		return a.agentUC.LLMStep(ctx, input.Messages, input.Tools, input.Config)
	})
	if err != nil {
		return nil, toActivityError(fmt.Errorf("LLMStepActivity - LLMStep: %w", err))
	}

	if result.AssistantMsg.Content != "" {
		if err := writer.WriteEvent(ctx, entity.NewTextDeltaEvent(result.AssistantMsg.Content, 0)); err != nil {
			return nil, toActivityError(fmt.Errorf("LLMStepActivity - publish text: %w", err))
		}
	}

	if err := writer.flush(ctx); err != nil {
		return nil, toActivityError(fmt.Errorf("LLMStepActivity - flush text: %w", err))
	}

	a.logger.Info("LLMStepActivity: completed, finish_reason=%s tool_calls=%d", result.FinishReason, len(result.ToolCalls))

	// Deduct usage cost after successful LLM call
	if a.billingUC != nil && input.AccountID != "" && result.Usage.TotalTokens > 0 {
		if _, _, err := a.billingUC.DeductUsage(ctx, input.AccountID, input.Config.Model, result.Usage); err != nil {
			a.logger.Warn("LLMStepActivity - DeductUsage: %v (non-fatal)", err)
		}
	}

	return &LLMStepOutput{
		Content:      result.AssistantMsg.Content,
		ToolCalls:    result.ToolCalls,
		Usage:        result.Usage,
		FinishReason: result.FinishReason,
	}, nil
}

// ToolExecActivity performs a single tool execution and returns the result.
// Long-running tools (web scrape, code execution) are reported via
// heartbeats with the tool name as progress detail.
func (a *AgentActivities) ToolExecActivity(ctx context.Context, input *ToolInput) (*ToolOutput, error) {
	publish := a.newPublishWriter(input.AccountID, input.RunID, input.RunID)

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "tool-exec", ToolName: input.ToolName}
	})
	defer stopHeartbeat()

	// The tool-call arc is two facts, mirroring the streaming path; either
	// persist failure fails the activity so the platform retries the
	// idempotent appends.
	if err := a.publishStreamEvent(ctx, publish, entity.NewToolExecStartEvent(input.ToolCallID, input.ToolName)); err != nil {
		return nil, err
	}

	execStart := time.Now()
	result, err := a.agentUC.ExecTool(ctx, input.identity(), input.toolCall())
	durationMs := time.Since(execStart).Milliseconds()

	if err != nil {
		// No tool ran: the call could not be authorized as this run. That is a
		// platform fault, so the run fails visibly instead of handing the model
		// a tool error it would try to work around.
		return nil, nonRetryableAfterSideEffect(fmt.Errorf("tool exec %s: %w", input.ToolName, err))
	}

	output, exitCode, isError := result.Output, result.ExitCode, result.IsError

	finishErr := a.publishStreamEvent(ctx, publish, entity.NewToolExecFinishEvent(
		input.ToolCallID, input.ToolName, output, exitCode, isError, durationMs,
	))

	// The finish fact is the record of what happened; losing it loses the
	// run's history. The fact check comes first because a tool can fail
	// after its side effect, so even a failed answer must not be retried.
	if finishErr != nil {
		return nil, nonRetryableAfterSideEffect(fmt.Errorf("ToolExecActivity - finish fact: %w", finishErr))
	}

	// A tool failure is the tool's answer, not the platform's: it reaches the
	// model as a failed tool message, and the run decides what happens next.
	// Returning it as an activity error would make the platform re-run a tool
	// that may already have had its side effect.
	if err != nil {
		return &ToolOutput{
			Output:     output,
			ExitCode:   exitCode,
			IsError:    isError,
			DurationMs: durationMs,
		}, nil
	}

	return &ToolOutput{
		Output:     output,
		ExitCode:   exitCode,
		IsError:    isError,
		DurationMs: durationMs,
	}, nil
}

// InitStreamActivity publishes stream init events onto the data plane and runs
// Prep. Combines AgentRunStart + PrepStage → Prep → PrepStage ready in one
// activity.
func (a *AgentActivities) InitStreamActivity(ctx context.Context, input *InitStreamInput) (*InitStreamOutput, error) {
	publish := a.newPublishWriter(input.AccountID, input.SessionID, input.RunID)

	// Open the timeline and the first prep stage; RUN_STARTED is a fact, so a
	// persist failure fails the activity and the platform retries the start.
	if err := a.publishStreamEvent(ctx, publish, entity.NewAgentRunStartEvent(input.Message)); err != nil {
		return nil, err
	}

	if err := a.publishStreamEvent(ctx, publish, entity.NewPrepStageEvent("initializing", 0)); err != nil {
		return nil, err
	}

	prepResult, err := a.agentUC.Prep(ctx, &agentuc.PrepRequest{
		SystemPrompt: input.SystemPrompt,
		UserMessage:  input.Message,
		History:      input.History,
		Tools:        input.Tools,
		Config:       input.Config,
	})
	if err != nil {
		return nil, fmt.Errorf("InitStreamActivity - Prep: %w", err)
	}
	// MCP tool resolution for streaming path
	mcpOut := a.prepMCP(ctx, PrepMCPInput{
		ServerConfigs: input.MCPServerConfigs,
	})
	allTools := prepResult.Tools
	allTools = append(allTools, mcpOut.Tools...)

	if err := a.publishStreamEvent(ctx, publish, entity.NewPrepStageEvent("ready", prepCompletePercent)); err != nil {
		return nil, err
	}

	return &InitStreamOutput{
		Messages: prepResult.Messages,
		Tools:    allTools,
	}, nil
}

// LLMStreamActivity performs a single streaming LLM call, writing delta events
// to the EventStore. Heartbeats carry the count of deltas already written so
// the platform can distinguish a live-but-slow stream from a dead worker
// (a dead worker is retried quickly; a live stream is never killed mid-call).
// Deducts usage cost from account balance after a successful LLM call.
func (a *AgentActivities) LLMStreamActivity(ctx context.Context, input *LLMStreamInput) (*LLMStreamOutput, error) {
	writer := &streamEventWriter{
		publish: a.newPublishWriter(input.AccountID, input.SessionID, input.RunID),
	}

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "llm-stream", WrittenEvents: writer.written.Load()}
	})
	defer stopHeartbeat()

	result, err := retryRateLimited(ctx, func() (*agentuc.LLMStepResult, error) {
		return a.agentUC.LLMStreamCall(ctx, input.Messages, input.Tools, input.Config, writer)
	})
	// Close any open message even if the stream was interrupted, so a
	// text-only round still terminates its message on the timeline; the
	// TEXT_MESSAGE_END fact is as mandatory as the stream's own outcome.
	if flushErr := writer.flush(ctx); flushErr != nil && err == nil {
		err = flushErr
	}

	if err != nil {
		return nil, toActivityError(fmt.Errorf("LLMStreamActivity - LLMStreamCall: %w", err))
	}

	// Deduct usage cost after successful LLM call
	if a.billingUC != nil && input.AccountID != "" && result.Usage.TotalTokens > 0 {
		if _, _, err := a.billingUC.DeductUsage(ctx, input.AccountID, input.Config.Model, result.Usage); err != nil {
			a.logger.Warn("LLMStreamActivity - DeductUsage: %v (non-fatal)", err)
		}
	}

	return &LLMStreamOutput{
		ToolCalls:    result.ToolCalls,
		Usage:        result.Usage,
		FinishReason: result.FinishReason,
	}, nil
}

// ToolExecStreamActivity executes a tool and publishes start/finish events onto
// the data plane.
func (a *AgentActivities) ToolExecStreamActivity(ctx context.Context, input *ToolInput) (*ToolOutput, error) {
	publish := a.newPublishWriter(input.AccountID, input.RunID, input.RunID)

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "tool-exec-stream", ToolName: input.ToolName}
	})
	defer stopHeartbeat()

	// The tool-call arc is two facts; either persist failure fails the
	// activity so the platform retries the idempotent appends.
	if err := a.publishStreamEvent(ctx, publish, entity.NewToolExecStartEvent(input.ToolCallID, input.ToolName)); err != nil {
		return nil, err
	}

	execStart := time.Now()
	result, err := a.agentUC.ExecTool(ctx, input.identity(), input.toolCall())
	durationMs := time.Since(execStart).Milliseconds()

	if err != nil {
		// No tool ran: the call could not be authorized as this run. The start
		// fact is already published and the finish fact is not, which is what
		// the timeline should show for a call that never reached a tool.
		return nil, nonRetryableAfterSideEffect(fmt.Errorf("tool exec %s: %w", input.ToolName, err))
	}

	output := result.Output
	exitCode := result.ExitCode
	isError := result.IsError

	// The finish fact records a tool that has already executed; a platform
	// retry would execute it again.
	if err := a.publishStreamEvent(ctx, publish, entity.NewToolExecFinishEvent(input.ToolCallID, input.ToolName, output, exitCode, isError, durationMs)); err != nil {
		return nil, nonRetryableAfterSideEffect(fmt.Errorf("ToolExecStreamActivity - finish fact: %w", err))
	}

	return &ToolOutput{
		Output:     output,
		ExitCode:   exitCode,
		IsError:    isError,
		DurationMs: durationMs,
	}, nil
}

// FinishStreamActivity makes the AgentRunFinish milestone durable and mirrors
// it onto the data plane. The fact write is the activity: a persist failure
// fails it, and the platform's retry re-attempts the idempotent append.
func (a *AgentActivities) FinishStreamActivity(ctx context.Context, input FinishStreamInput) error {
	return a.publishStreamEvent(ctx, a.newPublishWriter(input.AccountID, input.SessionID, input.RunID), input.Event)
}

// ——— Internal helpers ———

// streamEventWriter implements usecase.StreamEventWriter by making each
// milestone durable and publishing every event onto the data plane. The bus
// publish is fail-open (a bus hiccup never fails the stream); a milestone
// persist failure is returned so the enclosing activity — and therefore the
// platform's retry — owns the fact. written tracks the number of events
// delivered so the enclosing activity can report stream progress in its
// heartbeats.
type streamEventWriter struct {
	publish *streamadapter.PublishWriter
	written atomic.Int64
}

func (w *streamEventWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	if w.publish != nil {
		if err := w.publish.WriteEvent(ctx, event); err != nil {
			return err
		}
	}

	w.written.Add(1)

	return nil
}

// flush closes any open AG-UI message so a stream that ends without a
// following non-content event still terminates its message on the timeline.
// The TEXT_MESSAGE_END fact must land, so the error propagates.
func (w *streamEventWriter) flush(ctx context.Context) error {
	if w.publish != nil {
		return w.publish.Flush(ctx)
	}

	return nil
}

// newPublishWriter builds the per-run data-plane writer for the streaming
// activities. It returns nil when no publisher is configured — callers must
// guard with publishStreamEvent. The channel tenant falls back to "default"
// when the account is blank (single-tenant runs get a deterministic channel).
func (a *AgentActivities) newPublishWriter(accountID, sessionID, runID string) *streamadapter.PublishWriter {
	if a.streamPub == nil {
		return nil
	}

	writer := streamadapter.NewPublishWriter(a.streamPub, streamadapter.HandleForRun(accountID, runID), sessionID, runID, a.logger)

	if a.facts != nil {
		writer = writer.WithFacts(a.facts)
	}

	return writer
}

// publishStreamEvent mirrors one runtime event onto the data plane. A nil
// writer (no publisher configured) is a no-op. A returned error means a
// milestone could not be made durable — the caller fails so the platform
// retries the fact; bus-publish failures stay internal to the writer.
func (a *AgentActivities) publishStreamEvent(ctx context.Context, w *streamadapter.PublishWriter, ev entity.StreamEvent) error {
	if w == nil {
		return nil
	}

	return w.WriteEvent(ctx, ev)
}

// FinishStreamInput is the input for the streaming finish activity.
type FinishStreamInput struct {
	AccountID string
	SessionID string
	RunID     string
	Event     entity.StreamEvent
}

// SnapshotHistoryInput asks for the current message history to be snapshotted
// outside workflow history (claim-check) so a continued workflow can reload it
// by reference instead of carrying the full conversation in its input.
type SnapshotHistoryInput struct {
	RunID    string
	Round    int
	Messages []entity.Message
}

// SnapshotHistoryOutput carries the blob reference to hand to the continued
// workflow. An empty Ref means no snapshot was taken (no blob store).
type SnapshotHistoryOutput struct {
	Ref string
}

// LoadHistoryInput references a history snapshot taken by SnapshotHistoryActivity.
type LoadHistoryInput struct {
	Ref string
}

// LoadHistoryOutput is the restored authoritative conversation.
type LoadHistoryOutput struct {
	Messages []entity.Message
}

// ——— History snapshot (claim-check) ———

// historySnapshotKey derives an idempotent blob key from run + round, so a
// retried snapshot overwrites the same blob instead of duplicating it.
func historySnapshotKey(runID string, round int) string {
	return fmt.Sprintf("agentfw-hist/%s/%d", runID, round)
}

// SnapshotHistoryActivity persists the current message history to the blob
// store and returns a reference (claim-check pattern). With no blob store
// configured it returns an empty ref and the workflow keeps the conversation
// in its own state (pre-feature behavior).
func (a *AgentActivities) SnapshotHistoryActivity(ctx context.Context, input *SnapshotHistoryInput) (*SnapshotHistoryOutput, error) {
	if a.blobStore == nil {
		return &SnapshotHistoryOutput{}, nil
	}

	payload, err := json.Marshal(input.Messages)
	if err != nil {
		return nil, fmt.Errorf("SnapshotHistoryActivity - marshal messages: %w", err)
	}

	obj, err := a.blobStore.Put(ctx, historySnapshotKey(input.RunID, input.Round), payload)
	if err != nil {
		return nil, fmt.Errorf("SnapshotHistoryActivity - blobStore.Put: %w", err)
	}

	return &SnapshotHistoryOutput{Ref: obj.URI}, nil
}

// LoadHistoryActivity restores a message history snapshot by reference. The
// snapshot is the authoritative conversation: if it cannot be loaded, the
// workflow fails rather than silently continuing with a partial conversation.
func (a *AgentActivities) LoadHistoryActivity(ctx context.Context, input *LoadHistoryInput) (*LoadHistoryOutput, error) {
	if a.blobStore == nil {
		return nil, fmt.Errorf("LoadHistoryActivity - %w: ref %q", errHistoryBlobStoreRequired, input.Ref)
	}

	payload, err := a.blobStore.Get(ctx, input.Ref)
	if err != nil {
		return nil, fmt.Errorf("LoadHistoryActivity - blobStore.Get: %w", err)
	}

	var messages []entity.Message
	if err := json.Unmarshal(payload, &messages); err != nil {
		return nil, fmt.Errorf("LoadHistoryActivity - unmarshal messages: %w", err)
	}

	return &LoadHistoryOutput{Messages: messages}, nil
}
