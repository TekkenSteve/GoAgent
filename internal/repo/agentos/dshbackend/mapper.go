package dshbackend

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// dsh SessionEvent type names. The fake test harness emits the same vocabulary,
// so the literals are shared constants rather than repeated strings.
const (
	eventTurnStart      = "turn/start"
	eventTurnEnd        = "turn/end"
	eventUserTurn       = "user/message"
	eventStepStart      = "step/start"
	eventStepEnd        = "step/end"
	eventAssistant      = "assistant/message"
	eventAssistantChunk = "assistant/chunk"
	eventToolCall       = "tool/call"
	eventToolResult     = "tool/result"
	eventRequestHeader  = "request/header"
	eventRequestContext = "request/context"
	eventSessionEndSeed = "session/end-seed"
)

// dsh StreamChunk type names inside assistant/chunk.data.chunk.
const (
	chunkTextDelta      = "text-delta"
	chunkReasoningDelta = "reasoning-delta"
	chunkToolCallDelta  = "tool-call-delta"
	chunkUsage          = "usage"
	chunkBlockStart     = "block-start"
	chunkBlockEnd       = "block-end"
	chunkFinish         = "finish"
)

// dsh turn/end reason values mapped onto AG-UI terminals.
const (
	turnReasonCompleted   = "completed"
	turnReasonError       = "error"
	turnReasonInterrupted = "interrupted"
	turnReasonBlocked     = "blocked"
	turnReasonMaxTokens   = "max-tokens"
	turnReasonAborted     = "aborted"
)

// mapEvent translates one dsh SessionEvent into the entity stream events that
// publish onto the run's data-plane channel. It is a pure function: done
// reports whether the event closes the run (a turn/end), and canceled selects
// the RUN_CANCELED terminal when the adapter-level cancel flag is set.
func mapEvent(ev sessionEvent, canceled bool) ([]entity.StreamEvent, bool) {
	switch ev.Type {
	case eventAssistantChunk:
		return mapChunk(ev), false
	case eventToolCall:
		return mapToolCall(ev), false
	case eventToolResult:
		return mapToolResult(ev), false
	case eventTurnEnd:
		return mapTurnEnd(ev, canceled), true
	default:
		// turn/start, step/start, step/end, user/message, assistant/message,
		// request/*, session/end-seed — no AG-UI counterpart on the run timeline.
		return nil, false
	}
}

// mapChunk maps an assistant/chunk event's token-level StreamChunk.
func mapChunk(ev sessionEvent) []entity.StreamEvent {
	var data assistantChunkData

	if err := json.Unmarshal(ev.Data, &data); err != nil {
		return nil
	}

	switch data.Chunk.Type {
	case chunkTextDelta:
		return []entity.StreamEvent{&entity.TextDeltaEvent{
			BaseEvent: streamBase(ev, entity.SourceLLM, entity.PhaseDelta, entity.ContentText),
			Content:   data.Chunk.Text,
			Index:     data.Chunk.Index,
		}}
	case chunkReasoningDelta:
		return []entity.StreamEvent{&entity.ReasoningDeltaEvent{
			BaseEvent: streamBase(ev, entity.SourceLLM, entity.PhaseDelta, entity.ContentReasoning),
			Reasoning: data.Chunk.Text,
		}}
	case chunkToolCallDelta:
		return []entity.StreamEvent{&entity.ToolCallDeltaEvent{
			BaseEvent:      streamBase(ev, entity.SourceLLM, entity.PhaseDelta, entity.ContentToolCall),
			ToolCallID:     data.Chunk.ID,
			ArgumentsDelta: data.Chunk.ArgumentsDelta,
		}}
	case chunkUsage:
		return []entity.StreamEvent{&entity.UsageFinishEvent{
			BaseEvent: streamBase(ev, entity.SourceLLM, entity.PhaseFinish, entity.ContentStatus),
			Usage: entity.Usage{
				PromptTokens:     data.Chunk.Usage.InputTokens,
				CompletionTokens: data.Chunk.Usage.OutputTokens,
				TotalTokens:      data.Chunk.Usage.TotalTokens,
			},
		}}
	default:
		// block-start, block-end, finish — the turn/end event owns the terminal.
		return nil
	}
}

// mapToolCall maps a tool/call event to the TOOL_CALL start milestone.
func mapToolCall(ev sessionEvent) []entity.StreamEvent {
	var data toolCallData

	if err := json.Unmarshal(ev.Data, &data); err != nil {
		return nil
	}

	if data.CallID == "" {
		return nil
	}

	return []entity.StreamEvent{&entity.ToolCallStartEvent{
		BaseEvent:  streamBase(ev, entity.SourceLLM, entity.PhaseStart, entity.ContentToolCall),
		ToolCallID: data.CallID,
		ToolName:   data.Name,
	}}
}

// mapToolResult maps a tool/result event to the completion arc: the TOOL_CALL
// end milestone plus the tool execution start/finish pair. dsh's result carries
// no tool name, so the consumer stamps names it learned from tool/call.
func mapToolResult(ev sessionEvent) []entity.StreamEvent {
	var data toolResultData

	if err := json.Unmarshal(ev.Data, &data); err != nil {
		return nil
	}

	callID, isErr, output := toolResultParts(data)

	if callID == "" {
		return nil
	}

	finish := &entity.ToolCallFinishEvent{
		BaseEvent:  streamBase(ev, entity.SourceLLM, entity.PhaseFinish, entity.ContentToolCall),
		ToolCallID: callID,
	}

	execStart := &entity.ToolExecStartEvent{
		BaseEvent:  streamBase(ev, entity.SourceTool, entity.PhaseStart, entity.ContentToolResult),
		ToolCallID: callID,
	}

	execFinish := &entity.ToolExecFinishEvent{
		BaseEvent:  streamBase(ev, entity.SourceTool, entity.PhaseFinish, entity.ContentToolResult),
		ToolCallID: callID,
		Output:     output,
		ExitCode:   exitCodeFor(isErr),
		IsError:    isErr,
	}

	return []entity.StreamEvent{finish, execStart, execFinish}
}

// toolResultParts extracts the call id, error flag, and output text from a
// tool/result payload. The correlated fields live in the ToolResultMessage's
// single ToolResultBlock.
func toolResultParts(data toolResultData) (callID string, isErr bool, output string) {
	if len(data.Error) > 0 && string(data.Error) != "null" {
		isErr = true
	}

	var msg toolResultMessage

	if err := json.Unmarshal(data.Message, &msg); err != nil {
		return "", isErr, ""
	}

	callID = firstToolCallID(msg)

	if callID == "" {
		return "", isErr, ""
	}

	return callID, isErr || hasBlockError(msg), joinResultText(msg)
}

// firstToolCallID returns the first correlated call id in a tool result.
func firstToolCallID(msg toolResultMessage) string {
	for _, block := range msg.Content {
		if block.ToolCallID != "" {
			return block.ToolCallID
		}
	}

	return ""
}

// hasBlockError reports whether any result block marks its call as failed.
func hasBlockError(msg toolResultMessage) bool {
	for _, block := range msg.Content {
		if block.IsError {
			return true
		}
	}

	return false
}

// joinResultText concatenates the text blocks of a tool result.
func joinResultText(msg toolResultMessage) string {
	var text []string

	for _, block := range msg.Content {
		for _, part := range block.Content {
			if part.Type == "text" && part.Text != "" {
				text = append(text, part.Text)
			}
		}
	}

	return strings.Join(text, "\n")
}

// mapTurnEnd maps a turn/end event to the run's terminal milestone. canceled
// forces RUN_CANCELED at the next turn boundary (the adapter-level cancel
// semantics: dsh has no SDK cancel, so the flag is honored here).
func mapTurnEnd(ev sessionEvent, canceled bool) []entity.StreamEvent {
	if canceled {
		return []entity.StreamEvent{&entity.AgentRunCanceledEvent{
			BaseEvent: streamBase(ev, entity.SourceSystem, entity.PhaseInterrupt, entity.ContentStatus),
		}}
	}

	var data turnEndData

	if err := json.Unmarshal(ev.Data, &data); err != nil {
		return nil
	}

	switch data.Reason {
	case turnReasonError, turnReasonBlocked:
		return []entity.StreamEvent{&entity.AgentErrorEvent{
			BaseEvent:    streamBase(ev, entity.SourceSystem, entity.PhaseError, entity.ContentStatus),
			ErrorMessage: "dsh turn ended in " + data.Reason,
			ErrorCode:    "dsh.turn." + data.Reason,
			Recoverable:  false,
		}}
	case turnReasonInterrupted:
		return []entity.StreamEvent{&entity.AgentRunCanceledEvent{
			BaseEvent: streamBase(ev, entity.SourceSystem, entity.PhaseInterrupt, entity.ContentStatus),
		}}
	default:
		return []entity.StreamEvent{&entity.AgentRunFinishEvent{
			BaseEvent:    streamBase(ev, entity.SourceAgent, entity.PhaseFinish, entity.ContentStatus),
			FinishReason: finishReasonFor(data.Reason),
		}}
	}
}

// finishReasonFor normalizes a dsh turn/end reason onto the entity finish-reason
// vocabulary; unknown reasons pass through for diagnostics.
func finishReasonFor(reason string) string {
	switch reason {
	case turnReasonCompleted:
		return string(entity.FinishStop)
	case turnReasonMaxTokens:
		return string(entity.FinishLength)
	default:
		return reason
	}
}

// exitCodeFor surfaces a failed tool result as a non-zero exit.
func exitCodeFor(isErr bool) int {
	if isErr {
		return 1
	}

	return 0
}

// streamBase builds the shared BaseEvent metadata for a mapped event. The
// PublishWriter overwrites the run/thread identity with the run's canonical
// scope; the source/phase/content-type vocabulary is decided here.
func streamBase(ev sessionEvent, source entity.EventSource, phase entity.EventPhase, contentType entity.EventContentType) entity.BaseEvent {
	return entity.BaseEvent{
		Source:      source,
		Phase:       phase,
		ContentType: contentType,
		Timestamp:   time.UnixMilli(ev.Time),
	}
}
