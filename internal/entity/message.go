package entity

// MessageRecord is a warm-state message payload.
type MessageRecord struct {
	RunID   string `json:"run_id"    example:"run-550e8400-e29b-41d4-a716-446655440000"`
	Role    string `json:"role"      example:"user"`
	Content string `json:"content"    example:"Hello, can you help me?"`
} // @name entity.MessageRecord

// WarmRefs points to operational data persisted outside workflow history.
type WarmRefs struct {
	MessageStoreRef       string `json:"message_store_ref"        example:"msg-ref-550e8400-e29b-41d4-a716-446655440000"`
	ToolResultStoreRef    string `json:"tool_result_store_ref"    example:"tool-ref-550e8400-e29b-41d4-a716-446655440000"`
	PersistenceOutboxRef  string `json:"persistence_outbox_ref"   example:"outbox-550e8400-e29b-41d4-a716-446655440000"`
	ToolResultOutboxRef   string `json:"tool_result_outbox_ref"   example:"tool-outbox-550e8400-e29b-41d4-a716-446655440000"`
	RunTimingRef          string `json:"run_timing_ref"           example:"timing-550e8400-e29b-41d4-a716-446655440000"`
	InitialUserMessageRef string `json:"initial_user_message_ref" example:"init-msg-550e8400-e29b-41d4-a716-446655440000"`
	StreamChannelID       string `json:"stream_channel_id"         example:"stream-550e8400-e29b-41d4-a716-446655440000"`
} // @name entity.WarmRefs

// ColdRefs points to archival and large payload storage.
type ColdRefs struct {
	PromptSnapshotRef  string   `json:"prompt_snapshot_ref"  example:"prompt-550e8400-e29b-41d4-a716-446655440000"`
	ContextArchiveRefs []string `json:"context_archive_refs" example:"[\"archive-550e8400-e29b-41d4-a716-446655440000\"]"`
	LargePayloadRefs   []string `json:"large_payload_refs"   example:"[\"large-550e8400-e29b-41d4-a716-446655440000\"]"`
} // @name entity.ColdRefs
