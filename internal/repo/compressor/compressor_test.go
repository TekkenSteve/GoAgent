package compressor_test

import (
	"context"
	"strings"
	"testing"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/compressor"
	"github.com/stretchr/testify/require"
)

func TestCompress_NoCompressionNeeded(t *testing.T) {
	t.Parallel()

	c := compressor.New(compressor.Config{
		ModelWindows: map[string]int{"test-model": 128000},
	})

	messages := []entity.Message{
		{Role: entity.RoleUser, Content: "Hi"},
		{Role: entity.RoleAssistant, Content: "Hello!"},
	}

	compressed, did, err := c.Compress(context.Background(), messages, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if did {
		t.Error("expected no compression for small messages")
	}

	if len(compressed) != 2 {
		t.Errorf("expected 2 messages, got %d", len(compressed))
	}
}

func TestCompress_EmptyMessages(t *testing.T) {
	t.Parallel()

	c := compressor.New(compressor.Config{
		ModelWindows: map[string]int{"test-model": 128000},
	})

	compressed, did, err := c.Compress(context.Background(), nil, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if did {
		t.Error("expected no compression for empty messages")
	}

	if compressed != nil {
		t.Errorf("expected nil, got %v", compressed)
	}
}

func TestCompress_TruncationOnly(t *testing.T) {
	t.Parallel()

	c := compressor.New(compressor.Config{
		ModelWindows: map[string]int{"test-model": 2000},
		SafetyRatio:  0.50, // 1000 token threshold
		// No LLM provider — archival skipped
	})

	longContent := strings.Repeat("A", 6000)
	messages := []entity.Message{
		{Role: entity.RoleUser, Content: "Short"},
		{Role: entity.RoleAssistant, Content: longContent},
		{Role: entity.RoleTool, Content: longContent, ToolCallID: "call-1"},
	}

	compressed, did, err := c.Compress(context.Background(), messages, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if !did {
		t.Error("expected compression for over-threshold messages")
	}
	// Assistant content should be truncated
	if len(compressed[1].Content) >= len(longContent) {
		t.Error("expected assistant content to be truncated")
	}
	// Tool content should be truncated
	if len(compressed[2].Content) >= len(longContent) {
		t.Error("expected tool content to be truncated")
	}
	// Short message should remain intact
	if compressed[0].Content != "Short" {
		t.Errorf("expected short message intact, got %q", compressed[0].Content)
	}
}

func TestCompress_ArchivalWithLLM(t *testing.T) {
	t.Parallel()

	summaryContent := "Archived summary of old conversation"
	c := compressor.New(compressor.Config{
		LLM: &mockLLM{response: entity.LLMResponse{
			Content: summaryContent,
		}},
		ModelWindows:     map[string]int{"test-model": 1000},
		SafetyRatio:      0.50,
		MaxWorkingMemory: 2,
		MinWorkingMemory: 2,
		MinToCompress:    1,
	})

	// Build enough messages to trigger archival (~576 tokens vs 500 threshold)
	messages := make([]entity.Message, 8)
	for i := range messages {
		role := entity.RoleUser
		if i%2 == 1 {
			role = entity.RoleAssistant
		}

		messages[i] = entity.Message{
			Role:    role,
			Content: strings.Repeat("x", 200),
		}
	}

	compressed, did, err := c.Compress(context.Background(), messages, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if !did {
		t.Error("expected compression")
	}
	// Should be summary msg + 2 working memory = 3
	if len(compressed) != 3 {
		t.Errorf("expected 3 messages (summary + 2 working), got %d", len(compressed))
	}
	// First message should be the summary
	if !strings.Contains(compressed[0].Content, summaryContent) {
		t.Errorf("expected summary message, got %q", compressed[0].Content)
	}
	// Summary prefix should be present
	if !strings.Contains(compressed[0].Content, "[Archived context") {
		t.Error("expected archived context prefix in summary message")
	}
}

func TestCompress_ArchivalWithToolPairProtection(t *testing.T) {
	t.Parallel()

	c := compressor.New(compressor.Config{
		LLM: &mockLLM{response: entity.LLMResponse{
			Content: "summary",
		}},
		ModelWindows:     map[string]int{"test-model": 1000},
		SafetyRatio:      0.50,
		MaxWorkingMemory: 3,
		MinWorkingMemory: 2,
		MinToCompress:    1,
	})

	// Messages: user, assistant(tool_call), tool, user, assistant(tool_call), tool, user, assistant
	// ~840 tokens total vs 500 threshold = triggers archival
	messages := []entity.Message{
		{Role: entity.RoleUser, Content: strings.Repeat("x", 300)},
		{Role: entity.RoleAssistant, Content: "", ToolCalls: []entity.ToolCall{{ID: "call-1"}}},
		{Role: entity.RoleTool, Content: strings.Repeat("y", 300), ToolCallID: "call-1"},
		{Role: entity.RoleUser, Content: strings.Repeat("x", 300)},
		{Role: entity.RoleAssistant, Content: "", ToolCalls: []entity.ToolCall{{ID: "call-2"}}},
		{Role: entity.RoleTool, Content: strings.Repeat("y", 300), ToolCallID: "call-2"},
		{Role: entity.RoleUser, Content: strings.Repeat("x", 300)},
		{Role: entity.RoleAssistant, Content: strings.Repeat("z", 300)},
	}

	compressed, did, err := c.Compress(context.Background(), messages, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if !did {
		t.Error("expected compression")
	}
	// Should not split in middle of tool-call pair
	for i, m := range compressed {
		if m.Role == entity.RoleTool {
			prevRole := entity.RoleAssistant
			if i > 0 {
				prevRole = compressed[i-1].Role
			}

			if prevRole != entity.RoleAssistant && prevRole != entity.RoleTool {
				t.Errorf("tool message at index %d is orphaned (previous role: %v)", i, prevRole)
			}
		}
	}
}

func TestCompress_EmergencyTruncation(t *testing.T) {
	t.Parallel()

	c := compressor.New(compressor.Config{
		ModelWindows: map[string]int{"test-model": 100},
		SafetyRatio:  0.50, // 50 token threshold — will always exceed
		// No LLM to force emergency path
	})

	// Many messages with very long content
	messages := make([]entity.Message, 30)
	for i := range messages {
		messages[i] = entity.Message{
			Role:    entity.RoleUser,
			Content: strings.Repeat("A", 5000),
		}
	}

	compressed, did, err := c.Compress(context.Background(), messages, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if !did {
		t.Error("expected compression")
	}
	// Emergency truncation caps at 20 messages
	if len(compressed) > 20 {
		t.Errorf("expected at most 20 messages after emergency truncation, got %d", len(compressed))
	}
	// All content should be truncated to emergencyMsgLimit (1200)
	for i, m := range compressed {
		if len(m.Content) > 1250 { // slight buffer
			t.Errorf("message %d still has %d chars after emergency truncation", i, len(m.Content))
		}
	}
}

func TestCompress_ModelWindowLookup(t *testing.T) {
	t.Parallel()

	// Known model
	c := compressor.New(compressor.Config{
		ModelWindows: map[string]int{"gpt-4.1-mini": 1000000},
	})

	short := []entity.Message{{Role: entity.RoleUser, Content: "hi"}}
	_, did, err := c.Compress(context.Background(), short, entity.LLMConfig{Model: "gpt-4.1-mini"})
	require.NoError(t, err)

	if did {
		t.Error("expected no compression for short messages with large window")
	}

	// Unknown model falls back to default (128K)
	_, did, err = c.Compress(context.Background(), short, entity.LLMConfig{Model: "unknown-model"})
	require.NoError(t, err)

	if did {
		t.Error("expected no compression for unknown model with default 128K window")
	}
}

func TestCompress_ArchivalLLMFallbackToTruncation(t *testing.T) {
	t.Parallel()

	// LLM returns error → should fall through to truncation
	c := compressor.New(compressor.Config{
		LLM:              &mockLLM{err: assertError("llm unavailable")},
		ModelWindows:     map[string]int{"test-model": 2000},
		SafetyRatio:      0.50,
		MinToCompress:    1,
		MaxWorkingMemory: 2,
		MinWorkingMemory: 2,
	})

	messages := []entity.Message{
		{Role: entity.RoleUser, Content: strings.Repeat("x", 3000)},
		{Role: entity.RoleAssistant, Content: strings.Repeat("y", 3000)},
	}

	compressed, did, err := c.Compress(context.Background(), messages, entity.LLMConfig{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}

	if !did {
		t.Error("expected compression even after LLM failure")
	}
	// Should fall through to truncation — content shortened
	if len(compressed[0].Content) >= 3000 || len(compressed[1].Content) >= 3000 {
		t.Error("expected content truncation after LLM archival failure")
	}
}

func TestEstimateTokens(t *testing.T) {
	t.Parallel()

	messages := []entity.Message{
		{Role: entity.RoleUser, Content: "hello world"},
		{Role: entity.RoleAssistant, Content: "hi there, how can I help you today?"},
		{Role: entity.RoleTool, Content: `{"result": "success", "data": [1, 2, 3]}`, ToolCallID: "call-1"},
	}

	tokens := estimateTokens(messages)
	if tokens <= 0 {
		t.Errorf("expected positive token estimate, got %d", tokens)
	}
}

func TestSafeTruncateContent(t *testing.T) {
	t.Parallel()

	short := "short text"

	result := safeTruncateContent(short, 100)
	if result != short {
		t.Errorf("expected unchanged text, got %q", result)
	}

	long := strings.Repeat("A", 500)

	result = safeTruncateContent(long, 100)
	if len(result) >= len(long) {
		t.Errorf("expected truncated content, got %d chars", len(result))
	}

	if !strings.Contains(result, "truncated") {
		t.Error("expected truncation indicator in output")
	}

	if !strings.HasPrefix(result, "AAAAA") {
		t.Error("expected content start preserved")
	}

	if !strings.HasSuffix(result, "AAAAA") {
		t.Error("expected content end preserved")
	}
}

// -- helpers --

func estimateTokens(messages []entity.Message) int {
	tokens := 0
	for _, msg := range messages {
		tokens += len(msg.Content)/3 + 5
		for _, tc := range msg.ToolCalls {
			tokens += len(tc.Function.Name)/3 + 10
			tokens += len(tc.Function.Arguments)/3 + 5
		}
	}

	return tokens
}

func safeTruncateContent(content string, maxLen int) string {
	if len(content) <= maxLen {
		return content
	}

	keep := maxLen - 50
	startLen := keep / 2
	endLen := keep - startLen
	start := content[:startLen]
	end := content[len(content)-endLen:]

	return start + "\n\n... (chars truncated) ...\n\n" + end
}

type mockLLM struct {
	response entity.LLMResponse
	err      error
}

func (m *mockLLM) Chat(_ context.Context, _ *entity.LLMRequest) (entity.LLMResponse, error) {
	if m.err != nil {
		return entity.LLMResponse{}, m.err
	}

	return m.response, nil
}

type assertError string

func (e assertError) Error() string { return string(e) }
