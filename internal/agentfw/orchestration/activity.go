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
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/runprojection"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	agentuc "github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	billinguc "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

const (
	defaultConcurrentLimit = 10
	prepCompletePercent    = 100
)

// errHistoryBlobStoreRequired reports that a history snapshot cannot be
// restored because no blob store is configured. Static so classification and
// wrapping stay stable across call sites.
var errHistoryBlobStoreRequired = errors.New("blob store is required to restore history snapshot")

// AgentActivities provides Temporal activity implementations for agent execution.
type AgentActivities struct {
	agentUC    *agentuc.UseCase
	billingUC  *billinguc.UseCase
	eventStore stream.EventStore
	mcpManager MCPManagerProvider
	// blobStore backs the claim-check history snapshot (see
	// SnapshotHistoryActivity). Optional: without it, long conversations stay
	// in workflow state and continue-as-new carries no snapshot.
	blobStore artifactBlobStore
	logger    logger.Interface
	// streamPub is the optional data-plane publisher. When set, every
	// streaming activity mirrors its events onto the AG-UI bus (fail-open);
	// nil keeps the runtime pure-Redis with zero behavior change.
	streamPub agentosstream.Publisher
	// runProjection is the optional data-plane projection controller. When
	// set (with streamPub), the first publish of each streaming activity
	// attaches the control plane to the run channel as a projection consumer
	// (idempotent, fail-open); nil keeps the runtime unchanged.
	runProjection runprojection.Controller
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

// NewAgentActivities creates activities wired to the agent usecase.
func NewAgentActivities(uc *agentuc.UseCase, eventStore stream.EventStore, l logger.Interface) *AgentActivities {
	return &AgentActivities{agentUC: uc, eventStore: eventStore, logger: l}
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

// WithStreamPublisher enables the data-plane mirror: streaming activities
// publish their AG-UI timeline onto the bus in addition to writing the Redis
// event store. Optional — without it the runtime is unchanged. Publishing is
// fail-open, so a dead bus never breaks a run.
func (a *AgentActivities) WithStreamPublisher(pub agentosstream.Publisher) *AgentActivities {
	a.streamPub = pub

	return a
}

// WithRunProjectionController enables the durable run-event projection: the
// first publish of each streaming activity attaches the projector to the run
// channel (idempotent, fail-open). Optional — without it the runtime is
// unchanged.
func (a *AgentActivities) WithRunProjectionController(controller runprojection.Controller) *AgentActivities {
	a.runProjection = controller

	return a
}

// ——— Activities (step-level, Temporal-native) ———.
type billingResult struct {
	out *PrepBillingOutput
	err error
}

type limitsResult struct {
	out *PrepLimitsOutput
}

// PrepareActivity runs the full prep pipeline: tool validation, message assembly,
// billing/limits checks, and tool definition validation. It returns a composite
// PrepareOutput with all check results and a CanProceed() gate for the workflow.
func (a *AgentActivities) PrepareActivity(ctx context.Context, input *PrepareInput) (*PrepareOutput, error) {
	a.logger.Info("PrepareActivity: started, mcpManager=%v serverConfigs=%d", a.mcpManager != nil, len(input.MCPServerConfigs))

	billingRes, limitsRes, toolsOut, mcpOut := a.runPrepChecks(ctx, input)
	a.logger.Info("PrepareActivity: pre-checks done, billing.err=%v", billingRes.err)

	var errs []string
	if billingRes.err != nil {
		errs = append(errs, fmt.Sprintf("billing: %v", billingRes.err))
	}

	for _, name := range toolsOut.Failed {
		errs = append(errs, fmt.Sprintf("tool: %s missing name or type", name))
	}

	for _, err := range mcpOut.Errors {
		errs = append(errs, fmt.Sprintf("mcp: %s", err))
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

	a.logger.Info("PrepareActivity: completed, errors=%v", errs)

	return &PrepareOutput{
		Messages: prepResult.Messages,
		Tools:    allTools,
		Billing:  billingRes.out,
		Limits:   limitsRes.out,
		ToolDefs: toolsOut,
		Errors:   errs,
	}, nil
}

// runPrepChecks runs the concurrent billing/limits checks and inline tools/MCP validation.
func (a *AgentActivities) runPrepChecks(ctx context.Context, input *PrepareInput) (billingRes billingResult, limitsRes limitsResult, toolsOut *PrepToolsOutput, mcpOut *PrepMCPOutput) {
	billingCh := make(chan billingResult, 1)
	limitsCh := make(chan limitsResult, 1)

	go func() {
		billing, err := a.prepBilling(ctx, PrepBillingInput{
			AccountID: input.AccountID,
			ModelRef:  input.Config.Model,
		})
		billingCh <- billingResult{billing, err}
	}()
	go func() {
		limits := a.prepLimits(ctx, PrepLimitsInput{
			MessageCount: len(input.History),
			ModelRef:     input.Config.Model,
		})
		limitsCh <- limitsResult{limits}
	}()

	// Tool validation runs inline
	toolsOut = a.prepTools(ctx, PrepToolsInput{Tools: input.Tools})

	// Wait for concurrent checks
	billingRes = <-billingCh
	limitsRes = <-limitsCh

	// MCP tool resolution runs in the prep pipeline
	mcpOut = a.prepMCP(ctx, PrepMCPInput{
		ServerConfigs: input.MCPServerConfigs,
	})

	return billingRes, limitsRes, toolsOut, mcpOut
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

// prepLimits validates concurrent run limits (pass-through: no counter configured).
func (a *AgentActivities) prepLimits(_ context.Context, _ PrepLimitsInput) *PrepLimitsOutput {
	return &PrepLimitsOutput{
		Approved:        true,
		ConcurrentLimit: defaultConcurrentLimit,
	}
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
		Tools:  tools,
		Errors: errs,
	}
}

// LLMStepActivity performs a single sync LLM call and returns the result.
// Deducts usage cost from account balance after a successful LLM call.
// Long inference is reported via heartbeats so a live activity is never
// marked dead and retried (which would duplicate the LLM call and its cost).
func (a *AgentActivities) LLMStepActivity(ctx context.Context, input *LLMStepInput) (*LLMStepOutput, error) {
	a.logger.Info("LLMStepActivity: started, model=%s messages=%d tools=%d", input.Config.Model, len(input.Messages), len(input.Tools))

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "llm-inference"}
	})
	defer stopHeartbeat()

	result, err := retryRateLimited(ctx, func() (*agentuc.LLMStepResult, error) {
		return a.agentUC.LLMStep(ctx, input.RunID, input.Messages, input.Tools, input.Config)
	})
	if err != nil {
		return nil, toActivityError(fmt.Errorf("LLMStepActivity - LLMStep: %w", err))
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
func (a *AgentActivities) ToolExecActivity(ctx context.Context, input ToolInput) (*ToolOutput, error) {
	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "tool-exec", ToolName: input.ToolName}
	})
	defer stopHeartbeat()

	result, err := a.agentUC.ExecTool(ctx, input.RunID, entity.ToolCall{
		ID:   input.ToolCallID,
		Type: "function",
		Function: entity.ToolCallFunction{
			Name:      input.ToolName,
			Arguments: "", // args passed separately via input.Args; re-marshal
		},
	})
	if err != nil {
		return nil, fmt.Errorf("ToolExecActivity - ExecTool: %w", err)
	}

	return &ToolOutput{
		Output:     result.Output,
		ExitCode:   result.ExitCode,
		IsError:    result.IsError,
		DurationMs: result.DurationMs,
	}, nil
}

// InitStreamActivity writes stream init events to the EventStore and runs Prep.
// Combines AgentRunStart + PrepStage → Prep → PrepStage ready in one activity.
func (a *AgentActivities) InitStreamActivity(ctx context.Context, input *InitStreamInput) (*InitStreamOutput, error) {
	publish := a.newPublishWriter(input.AccountID, input.SessionID, input.RunID)

	// Write start and prep-stage events to EventStore, mirrored on the data
	// plane (fail-open).
	startEvent := entity.NewAgentRunStartEvent(input.Message)
	a.publishStreamEvent(ctx, publish, startEvent)

	if _, err := a.eventStore.Append(ctx, input.SessionID, input.RunID, startEvent); err != nil {
		a.logger.Warn("InitStreamActivity - append start event: %v", err)
	}

	prepInitEvent := entity.NewPrepStageEvent("initializing", 0)
	a.publishStreamEvent(ctx, publish, prepInitEvent)

	if _, err := a.eventStore.Append(ctx, input.SessionID, input.RunID, prepInitEvent); err != nil {
		a.logger.Warn("InitStreamActivity - append prep event: %v", err)
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

	prepReadyEvent := entity.NewPrepStageEvent("ready", prepCompletePercent)
	a.publishStreamEvent(ctx, publish, prepReadyEvent)

	if _, err := a.eventStore.Append(ctx, input.SessionID, input.RunID, prepReadyEvent); err != nil {
		a.logger.Warn("InitStreamActivity - append ready event: %v", err)
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
	writer := &eventStoreWriter{
		store:     a.eventStore,
		sessionID: input.SessionID,
		runID:     input.RunID,
		publish:   a.newPublishWriter(input.AccountID, input.SessionID, input.RunID),
	}

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "llm-stream", WrittenEvents: writer.written.Load()}
	})
	defer stopHeartbeat()

	result, err := retryRateLimited(ctx, func() (*agentuc.LLMStepResult, error) {
		return a.agentUC.LLMStreamCall(ctx, input.RunID, input.Messages, input.Tools, input.Config, writer)
	})
	// Close any open message even if the stream was interrupted, so a
	// text-only round still terminates its message on the timeline.
	writer.flush(ctx)

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

// ToolExecStreamActivity executes a tool and writes start/finish events to the EventStore.
func (a *AgentActivities) ToolExecStreamActivity(ctx context.Context, input ToolInput) (*ToolOutput, error) {
	sessionID, runID := input.RunID, input.RunID
	publish := a.newPublishWriter(input.AccountID, input.RunID, input.RunID)

	stopHeartbeat := startHeartbeatLoop(ctx, func() any {
		return heartbeatProgress{Phase: "tool-exec-stream", ToolName: input.ToolName}
	})
	defer stopHeartbeat()

	// Write ToolExecStart, mirrored on the data plane (fail-open).
	startEvent := entity.NewToolExecStartEvent(input.ToolCallID, input.ToolName)
	a.publishStreamEvent(ctx, publish, startEvent)

	if _, err := a.eventStore.Append(ctx, sessionID, runID, startEvent); err != nil {
		a.logger.Warn("ToolExecStreamActivity - append start event: %v", err)
	}

	execStart := time.Now()
	result, err := a.agentUC.ExecTool(ctx, runID, entity.ToolCall{
		ID:   input.ToolCallID,
		Type: "function",
		Function: entity.ToolCallFunction{
			Name:      input.ToolName,
			Arguments: "", // args re-marshaled from input.Args
		},
	})
	durationMs := time.Since(execStart).Milliseconds()

	output := result.Output
	exitCode := result.ExitCode
	isError := result.IsError

	if err != nil {
		output = fmt.Sprintf("Error executing tool %q: %v", input.ToolName, err)
		exitCode = 1
		isError = true
	}

	// Write ToolExecFinish, mirrored on the data plane (fail-open).
	finishEvent := entity.NewToolExecFinishEvent(input.ToolCallID, input.ToolName, output, exitCode, isError, durationMs)
	a.publishStreamEvent(ctx, publish, finishEvent)

	if _, err := a.eventStore.Append(ctx, sessionID, runID, finishEvent); err != nil {
		a.logger.Warn("ToolExecStreamActivity - append finish event: %v", err)
	}

	return &ToolOutput{
		Output:     output,
		ExitCode:   exitCode,
		IsError:    isError,
		DurationMs: durationMs,
	}, nil
}

// FinishStreamActivity writes the AgentRunFinish event to the EventStore,
// mirrored on the data plane (fail-open).
func (a *AgentActivities) FinishStreamActivity(ctx context.Context, input FinishStreamInput) error {
	a.publishStreamEvent(ctx, a.newPublishWriter(input.AccountID, input.SessionID, input.RunID), input.Event)

	_, err := a.eventStore.Append(ctx, input.SessionID, input.RunID, input.Event)

	return err
}

// ——— Internal helpers ———

// eventStoreWriter implements usecase.StreamEventWriter by appending to EventStore.
// When a data-plane publisher is configured it mirrors each event onto the AG-UI
// bus first (fail-open: a bus hiccup never fails the append).
// written tracks the number of deltas delivered so the enclosing activity can
// report stream progress in its heartbeats.
type eventStoreWriter struct {
	store     stream.EventStore
	sessionID string
	runID     string
	publish   *streamadapter.PublishWriter
	written   atomic.Int64
}

func (w *eventStoreWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	if w.publish != nil {
		//nolint:errcheck // fail-open: the writer always returns nil by contract.
		_ = w.publish.WriteEvent(ctx, event)
	}

	_, err := w.store.Append(ctx, w.sessionID, w.runID, event)
	if err == nil {
		w.written.Add(1)
	}

	return err
}

// flush closes any open AG-UI message so a stream that ends without a
// following non-content event still terminates its message on the timeline.
func (w *eventStoreWriter) flush(ctx context.Context) {
	if w.publish != nil {
		w.publish.Flush(ctx)
	}
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

	if a.runProjection != nil {
		controller := a.runProjection

		writer.WithEnsure(func(ctx context.Context, handle *agentosstream.Handle) error {
			return controller.EnsureSubscribed(ctx, runID, handle)
		})
	}

	return writer
}

// publishStreamEvent mirrors one runtime event onto the data plane, fail-open.
// A nil writer (no publisher configured) is a no-op.
func (a *AgentActivities) publishStreamEvent(ctx context.Context, w *streamadapter.PublishWriter, ev entity.StreamEvent) {
	if w != nil {
		//nolint:errcheck // fail-open: the writer always returns nil by contract.
		_ = w.WriteEvent(ctx, ev)
	}
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
