package conversation

import "testing"

func TestConversationStreamKeyIncludesTenantScope(t *testing.T) {
	t.Parallel()

	base := conversationStreamKey("account-1", "project-1", "thread-1")
	for _, other := range []string{
		conversationStreamKey("account-2", "project-1", "thread-1"),
		conversationStreamKey("account-1", "project-2", "thread-1"),
		conversationStreamKey("account-1", "project-1", "thread-2"),
	} {
		if base == other {
			t.Fatalf("conversation stream key collision: %q", base)
		}
	}
}

func TestConversationStreamKeyEscapesSeparators(t *testing.T) {
	t.Parallel()

	if conversationStreamKey("account:a", "project", "thread") ==
		conversationStreamKey("account", "a:project", "thread") {
		t.Fatal("conversation stream key must not collide across scoped components")
	}
}
