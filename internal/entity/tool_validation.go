package entity

// ToolCallValidationResult describes the state of tool call pairing in a message list.
type ToolCallValidationResult struct {
	Valid        bool     `json:"valid"`
	OrphanedIDs  []string `json:"orphaned_ids,omitempty"`  // tool results without matching calls
	UnansweredIDs []string `json:"unanswered_ids,omitempty"` // tool calls without results
}

// ValidateToolCallPairing checks that every tool-role message has a corresponding
// assistant tool_call, and every assistant tool_call has a corresponding tool result.
func ValidateToolCallPairing(messages []Message) ToolCallValidationResult {
	// Collect all tool call IDs from assistant messages
	callIDs := make(map[string]bool)
	for _, m := range messages {
		if m.Role == RoleAssistant {
			for _, tc := range m.ToolCalls {
				callIDs[tc.ID] = false // false = no result yet
			}
		}
	}

	// Mark those with tool results
	for _, m := range messages {
		if m.Role == RoleTool && m.ToolCallID != "" {
			if _, exists := callIDs[m.ToolCallID]; exists {
				callIDs[m.ToolCallID] = true
			} else {
				// Tool result without matching call = orphan
			}
		}
	}

	var orphaned, unanswered []string

	// Track orphaned tool results (tool messages whose call_id doesn't match any call)
	for _, m := range messages {
		if m.Role == RoleTool && m.ToolCallID != "" {
			if _, exists := callIDs[m.ToolCallID]; !exists {
				orphaned = append(orphaned, m.ToolCallID)
			}
		}
	}

	// Track unanswered tool calls
	for id, hasResult := range callIDs {
		if !hasResult {
			unanswered = append(unanswered, id)
		}
	}

	return ToolCallValidationResult{
		Valid:          len(orphaned) == 0 && len(unanswered) == 0,
		OrphanedIDs:    orphaned,
		UnansweredIDs:  unanswered,
	}
}

// RepairToolCallPairing removes orphaned tool results and unanswered tool calls,
// returning a clean message list suitable for LLM consumption.
func RepairToolCallPairing(messages []Message) []Message {
	result := ValidateToolCallPairing(messages)
	if result.Valid {
		return messages
	}

	orphanSet := make(map[string]bool, len(result.OrphanedIDs))
	for _, id := range result.OrphanedIDs {
		orphanSet[id] = true
	}
	unansweredSet := make(map[string]bool, len(result.UnansweredIDs))
	for _, id := range result.UnansweredIDs {
		unansweredSet[id] = true
	}

	cleaned := make([]Message, 0, len(messages))
	for _, m := range messages {
		if m.Role == RoleTool && orphanSet[m.ToolCallID] {
			continue // drop orphaned tool result
		}
		if m.Role == RoleAssistant {
			// Filter out unanswered tool calls
			var kept []ToolCall
			for _, tc := range m.ToolCalls {
				if !unansweredSet[tc.ID] {
					kept = append(kept, tc)
				}
			}
			if len(kept) == 0 && m.Content == "" {
				continue // drop empty assistant message with all calls removed
			}
			m.ToolCalls = kept
		}
		cleaned = append(cleaned, m)
	}

	return cleaned
}

// StripToolContent removes all tool-related messages entirely (emergency fallback).
func StripToolContent(messages []Message) []Message {
	cleaned := make([]Message, 0, len(messages))
	for _, m := range messages {
		if m.Role == RoleTool {
			continue // drop all tool results
		}
		if m.Role == RoleAssistant && len(m.ToolCalls) > 0 {
			m.ToolCalls = nil // strip tool calls but keep text content
		}
		cleaned = append(cleaned, m)
	}
	return cleaned
}
