package entity

// MessageRecord is a warm-state message payload.
type MessageRecord struct {
	RunID   string
	Role    string
	Content string
}

// WarmRefs points to operational data persisted outside workflow history.
type WarmRefs struct {
	MessageStoreRef       string
	ToolResultStoreRef    string
	PersistenceOutboxRef  string
	ToolResultOutboxRef   string
	RunTimingRef          string
	InitialUserMessageRef string
	StreamChannelID       string
}

// ColdRefs points to archival and large payload storage.
type ColdRefs struct {
	PromptSnapshotRef  string
	ContextArchiveRefs []string
	LargePayloadRefs   []string
}
