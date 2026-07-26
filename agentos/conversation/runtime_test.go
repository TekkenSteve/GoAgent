package conversation

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
)

func TestPrepareStartSpecGeneratesStableIdentityDefaults(t *testing.T) {
	t.Parallel()

	prepared, err := prepareStartSpec(&agentos.StartConversationRunSpec{
		ThreadID:       "thread-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		UserMessage:    "hello",
		IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatalf("prepare start spec: %v", err)
	}

	if prepared.RunID == "" || prepared.MessageID == "" || prepared.RequestedAt.IsZero() {
		t.Fatalf("generated identity is incomplete: %#v", prepared)
	}

	if prepared.Attachments == nil || prepared.MessageMetadata == nil || prepared.RunMetadata == nil {
		t.Fatalf("collection defaults must be non-nil: %#v", prepared)
	}
}

func TestPrepareStartSpecRequiresResumeInterruptIdentity(t *testing.T) {
	t.Parallel()

	_, err := prepareStartSpec(&agentos.StartConversationRunSpec{
		ThreadID:       "thread-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		UserMessage:    "answer",
		IdempotencyKey: "request-1",
		RequestedAt:    time.Now(),
		Resume:         &agentos.ConversationResume{},
	})
	if !errors.Is(err, ErrInvalidConversation) {
		t.Fatalf("error = %v, want ErrInvalidConversation", err)
	}
}

func TestPrepareExternalEventRequiresPositiveSourceSequence(t *testing.T) {
	t.Parallel()

	_, err := prepareExternalEvent(&agentos.ExternalConversationEvent{
		ThreadID:      "thread-1",
		RunID:         "run-1",
		AccountID:     "account-1",
		ProjectID:     "project-1",
		SourceEventID: "event-1",
		EventType:     agentos.ConversationEventRunStarted,
	})
	if !errors.Is(err, ErrInvalidConversation) {
		t.Fatalf("error = %v, want ErrInvalidConversation", err)
	}
}

func TestConversationLifecycleEventNamesMatchWireContract(t *testing.T) {
	t.Parallel()

	got := []string{
		string(agentos.ConversationEventRunStarted),
		string(agentos.ConversationEventTextMessageStart),
		string(agentos.ConversationEventTextMessageContent),
		string(agentos.ConversationEventTextMessageEnd),
		string(agentos.ConversationEventRunFinished),
		string(agentos.ConversationEventRunError),
	}

	want := []string{"RUN_STARTED", "TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END", "RUN_FINISHED", "RUN_ERROR"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("event %d = %q, want %q", index, got[index], want[index])
		}
	}
}

func TestConversationCancellationValuesMatchWireContract(t *testing.T) {
	t.Parallel()

	const cancelledWireValue = "cancelled" //nolint:misspell // agentos.conversation.v1 uses this wire value.

	if agentos.ConversationRunCancelled != cancelledWireValue {
		t.Fatalf("cancellation run status = %q", agentos.ConversationRunCancelled)
	}

	if agentos.ConversationOutcomeCancelled != cancelledWireValue {
		t.Fatalf("cancellation run outcome = %q", agentos.ConversationOutcomeCancelled)
	}
}
