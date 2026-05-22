// Package compressor provides context compression for long-running agent conversations.
// It implements tiered strategies: LLM-based archival summarization, working memory
// truncation, and emergency truncation.
package compressor

import (
	"context"
	"fmt"
	"maps"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/repo"
)

const (
	// Default safety threshold ratio of context window.
	defaultSafetyRatio = 0.70

	// Working memory configuration (messages kept from the end during archival).
	defaultMaxWorkingMemory = 6
	defaultMinWorkingMemory = 2
	defaultMinToCompress    = 3

	// Default context window when model is unknown.
	defaultContextWindow = 128000

	// Truncation limits (in characters).
	toolOutputLimit      = 2000
	toolOutputLastLimit  = 3000
	userMsgLimit         = 4000
	assistantMsgLimit    = 2000
	emergencyMsgLimit    = 1200
	emergencyMaxMessages = 20

	summaryContentPrefix = "[Archived context — Earlier in the conversation]"

	// Model context window sizes (in tokens).
	gpt41ContextWindow      = 1000000
	gpt4oContextWindow      = 128000
	gpt4ContextWindow       = 8192
	gpt35TurboContextWindow = 16385
	claudeContextWindow     = 200000
	deepseekContextWindow   = 65536

	// Summarization max tokens.
	summarizeMaxTokens = 1024

	// Token estimation heuristics.
	charsPerTokenEstimate = 3

	// Token overhead for each message, function name, and function args in token estimation.
	tokenOverheadPerMessage  = 5
	tokenOverheadPerFuncName = 10
	tokenOverheadPerFuncArgs = 5

	// Truncation display constants.
	truncationIndicatorReserve = 50
	contentSplitParts          = 2
)

// Config configures the compressor.
type Config struct {
	// LLM is used for archival summarization. If nil, archival is skipped.
	LLM repo.LLMProvider
	// ModelWindows maps model name to context window token count.
	// Entries supplement or override the built-in defaults.
	ModelWindows map[string]int
	// SafetyRatio is the fraction of context window treated as the trigger threshold. Default 0.70.
	SafetyRatio float64
	// MaxWorkingMemory is the max recent messages to keep during archival. Default 6.
	MaxWorkingMemory int
	// MinWorkingMemory is the min recent messages to keep during archival. Default 2.
	MinWorkingMemory int
	// MinToCompress is the minimum messages required to attempt archival. Default 3.
	MinToCompress int
}

// Compressor implements repo.ContextCompressor.
type Compressor struct {
	llm              repo.LLMProvider
	modelWindows     map[string]int
	safetyRatio      float64
	maxWorkingMemory int
	minWorkingMemory int
	minToCompress    int
}

var defaultModelWindows = map[string]int{ //nolint:gochecknoglobals // model window size lookup table
	"gpt-4.1-mini":             gpt41ContextWindow,
	"gpt-4.1":                  gpt41ContextWindow,
	"gpt-4o-mini":              gpt4oContextWindow,
	"gpt-4o":                   gpt4oContextWindow,
	"gpt-4-turbo":              gpt4oContextWindow,
	"gpt-4":                    gpt4ContextWindow,
	"gpt-3.5-turbo":            gpt35TurboContextWindow,
	"claude-sonnet-4-20250514": claudeContextWindow,
	"claude-3-5-sonnet-latest": claudeContextWindow,
	"claude-3-haiku":           claudeContextWindow,
	"claude-opus-4-20250514":   claudeContextWindow,
	"deepseek-chat":            deepseekContextWindow,
	"deepseek-v4-flash":        deepseekContextWindow,
}

// New creates a Compressor. If cfg.LLM is nil, archival summarization is skipped.
func New(cfg Config) *Compressor {
	c := &Compressor{
		llm:          cfg.LLM,
		modelWindows: make(map[string]int),
		safetyRatio:  defaultSafetyRatio,
	}
	if cfg.SafetyRatio > 0 {
		c.safetyRatio = cfg.SafetyRatio
	}

	c.maxWorkingMemory = nonZero(cfg.MaxWorkingMemory, defaultMaxWorkingMemory)
	c.minWorkingMemory = nonZero(cfg.MinWorkingMemory, defaultMinWorkingMemory)
	c.minToCompress = nonZero(cfg.MinToCompress, defaultMinToCompress)

	// Copy built-in defaults
	maps.Copy(c.modelWindows, defaultModelWindows)
	// Apply user overrides
	maps.Copy(c.modelWindows, cfg.ModelWindows)

	return c
}

func nonZero(a, b int) int {
	if a > 0 {
		return a
	}

	return b
}

// Compress reduces message token count if approaching the model's context limit.
func (c *Compressor) Compress(ctx context.Context, messages []entity.Message, config entity.LLMConfig) ([]entity.Message, bool, error) {
	if len(messages) == 0 {
		return messages, false, nil
	}

	window := c.contextWindow(config.Model)
	threshold := int(float64(window) * c.safetyRatio)

	tokens := estimateTokens(messages)
	if tokens < threshold {
		return messages, false, nil
	}

	// Tier 1: LLM-based archival summarization
	if c.llm != nil && len(messages) > c.minToCompress+c.minWorkingMemory {
		archived, err := c.archive(ctx, messages, config)
		if err == nil {
			messages = archived

			tokens = estimateTokens(messages)
			if tokens < threshold {
				return messages, true, nil
			}
		}
	}

	// Tier 2: Working memory truncation
	messages = truncateWorkingMemory(messages)

	tokens = estimateTokens(messages)
	if tokens < threshold {
		return messages, true, nil
	}

	// Tier 3: Emergency truncation
	messages = emergencyTruncate(messages)

	return messages, true, nil
}

func (c *Compressor) contextWindow(model string) int {
	if w, ok := c.modelWindows[model]; ok {
		return w
	}

	return defaultContextWindow
}

// archive splits messages into archival + working memory, generates a summary via LLM,
// and replaces the archival portion with the summary message.
func (c *Compressor) archive(ctx context.Context, messages []entity.Message, config entity.LLMConfig) ([]entity.Message, error) {
	total := len(messages)
	workingSize := max(min(total-c.minToCompress, c.maxWorkingMemory), c.minWorkingMemory)

	splitIdx := total - workingSize
	// Don't split in the middle of a tool/assistant pair
	for splitIdx > c.minToCompress && messages[splitIdx].Role == entity.RoleTool {
		splitIdx--
	}

	toCompress := messages[:splitIdx]
	workingMemory := messages[splitIdx:]

	summary, err := c.summarize(ctx, toCompress, config)
	if err != nil {
		return nil, fmt.Errorf("compressor - archive - summarize: %w", err)
	}

	summaryMsg := entity.Message{
		Role:    entity.RoleUser,
		Content: summaryContentPrefix + "\n\n" + summary,
	}

	result := make([]entity.Message, 0, 1+len(workingMemory))
	result = append(result, summaryMsg)
	result = append(result, workingMemory...)

	return result, nil
}

func (c *Compressor) summarize(ctx context.Context, messages []entity.Message, config entity.LLMConfig) (string, error) {
	systemMsg := entity.Message{
		Role:    entity.RoleSystem,
		Content: "You are a conversation summarizer. Condense the following conversation into a concise summary, preserving key information, decisions, tool results, and user preferences. Focus on facts and context needed to continue the conversation seamlessly. Keep the summary under 500 words.",
	}
	summarizeMessages := make([]entity.Message, 0, 1+len(messages))
	summarizeMessages = append(summarizeMessages, systemMsg)
	summarizeMessages = append(summarizeMessages, messages...)

	resp, err := c.llm.Chat(ctx, &entity.LLMRequest{
		Messages: summarizeMessages,
		Config: entity.LLMConfig{
			Model:       config.Model,
			MaxTokens:   summarizeMaxTokens,
			Temperature: 0,
		},
	})
	if err != nil {
		return "", err
	}

	return resp.Content, nil
}

// estimateTokens returns a conservative token estimate for a message slice.
// Uses ~3 chars per token heuristic plus per-message overhead.
func estimateTokens(messages []entity.Message) int {
	tokens := 0
	for _, msg := range messages {
		tokens += len(msg.Content)/charsPerTokenEstimate + tokenOverheadPerMessage
		for _, tc := range msg.ToolCalls {
			tokens += len(tc.Function.Name)/charsPerTokenEstimate + tokenOverheadPerFuncName
			tokens += len(tc.Function.Arguments)/charsPerTokenEstimate + tokenOverheadPerFuncArgs
		}
	}

	return tokens
}

// truncateWorkingMemory applies tiered in-memory content truncation.
// Tier 1: tool outputs → Tier 2: user messages → Tier 3: assistant messages.
func truncateWorkingMemory(messages []entity.Message) []entity.Message {
	result := make([]entity.Message, len(messages))
	copy(result, messages)

	// Tier 1: truncate tool result messages (last 2 get more room)
	toolIndices := make([]int, 0, len(result))
	for i, m := range result {
		if m.Role == entity.RoleTool {
			toolIndices = append(toolIndices, i)
		}
	}

	for idx, i := range toolIndices {
		limit := toolOutputLimit
		if idx >= len(toolIndices)-2 {
			limit = toolOutputLastLimit
		}

		if len(result[i].Content) > limit {
			result[i].Content = safeTruncateContent(result[i].Content, limit)
		}
	}

	truncateLongMessages(result, entity.RoleUser, userMsgLimit)
	truncateLongMessages(result, entity.RoleAssistant, assistantMsgLimit)

	return result
}

// emergencyTruncate aggressively reduces message count and content size.
func emergencyTruncate(messages []entity.Message) []entity.Message {
	if len(messages) <= emergencyMaxMessages {
		result := make([]entity.Message, len(messages))
		copy(result, messages)

		for i, m := range result {
			if len(m.Content) > emergencyMsgLimit {
				result[i].Content = safeTruncateContent(m.Content, emergencyMsgLimit)
			}
		}

		return result
	}

	// Keep the last emergencyMaxMessages, but compress all content
	result := make([]entity.Message, emergencyMaxMessages)
	copy(result, messages[len(messages)-emergencyMaxMessages:])

	for i, m := range result {
		if len(m.Content) > emergencyMsgLimit {
			result[i].Content = safeTruncateContent(m.Content, emergencyMsgLimit)
		}
	}

	return result
}

// safeTruncateContent keeps the start and end of content, removing the middle.
func safeTruncateContent(content string, maxLen int) string {
	if len(content) <= maxLen {
		return content
	}

	keep := maxLen - truncationIndicatorReserve // reserve for truncation indicator
	startLen := keep / contentSplitParts
	endLen := keep - startLen
	start := content[:startLen]
	end := content[len(content)-endLen:]

	return start + "\n\n... (" + fmt.Sprintf("%d", len(content)-keep) + " chars truncated) ...\n\n" + end
}

// truncateLongMessages truncates message content that exceeds the limit for a given role.
func truncateLongMessages(messages []entity.Message, role entity.MessageRole, limit int) {
	for i, m := range messages {
		if m.Role == role && len(m.Content) > limit {
			messages[i].Content = safeTruncateContent(m.Content, limit)
		}
	}
}
