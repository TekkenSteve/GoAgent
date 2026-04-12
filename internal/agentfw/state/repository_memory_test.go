package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemoryRepositoryWarmColdBoundaries(t *testing.T) {
	repo := NewInMemoryRepository()
	ctx := context.Background()

	msgRef, err := repo.PersistMessage(ctx, MessageRecord{
		RunID:   "run-1",
		Role:    "assistant",
		Content: "hello",
	})
	require.NoError(t, err)
	require.NotEmpty(t, msgRef)

	toolRef, err := repo.PersistToolResult(ctx, ToolResultRecord{
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "search",
		ResultJSON: `{"ok":true}`,
	})
	require.NoError(t, err)
	require.NotEmpty(t, toolRef)

	archiveRef, err := repo.PersistArchive(ctx, ArchiveRecord{
		RunID:       "run-1",
		PayloadType: "context-summary",
		Content:     []byte("compressed"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, archiveRef)

	msg, ok, err := repo.GetMessage(ctx, msgRef)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "assistant", msg.Role)

	tool, ok, err := repo.GetToolResult(ctx, toolRef)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "search", tool.ToolName)

	archive, ok, err := repo.GetArchive(ctx, archiveRef)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "context-summary", archive.PayloadType)
}
