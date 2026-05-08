package sse_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/TekkenSteve/GoAgent/pkg/sse"
)

// -- Reader tests --

func collect(t *testing.T, data string) []sse.Event {
	t.Helper()
	var events []sse.Event
	for evt, err := range sse.Read(strings.NewReader(data), nil) {
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		events = append(events, evt)
	}
	return events
}

func TestRead_Empty(t *testing.T) {
	events := collect(t, "")
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestRead_OnlyNewlines(t *testing.T) {
	events := collect(t, "\n\n\n")
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestRead_SingleData(t *testing.T) {
	events := collect(t, "data: hello\n\n")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != "hello" {
		t.Fatalf("expected data 'hello', got %q", events[0].Data)
	}
}

func TestRead_MultiLineData(t *testing.T) {
	input := "data: line1\ndata: line2\n\n"
	events := collect(t, input)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != "line1\nline2" {
		t.Fatalf("expected 'line1\\nline2', got %q", events[0].Data)
	}
}

func TestRead_EventType(t *testing.T) {
	input := "event: done\ndata: finished\n\n"
	events := collect(t, input)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != "done" {
		t.Fatalf("expected type 'done', got %q", events[0].Type)
	}
	if events[0].Data != "finished" {
		t.Fatalf("expected data 'finished', got %q", events[0].Data)
	}
}

func TestRead_EventID(t *testing.T) {
	input := "id: 42\ndata: hello\n\n"
	events := collect(t, input)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].LastEventID != "42" {
		t.Fatalf("expected id '42', got %q", events[0].LastEventID)
	}
}

func TestRead_IDCarriesForward(t *testing.T) {
	input := "id: 1\ndata: first\n\ndata: second\n\n"
	events := collect(t, input)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].LastEventID != "1" {
		t.Fatalf("expected id '1' on first, got %q", events[0].LastEventID)
	}
	if events[1].LastEventID != "1" {
		t.Fatalf("expected id '1' on second (carried), got %q", events[1].LastEventID)
	}
}

func TestRead_CommentIsBoundary(t *testing.T) {
	input := "data: first\n\ndata: second\n\n"
	events := collect(t, input)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestRead_OpenAIFinalChunk(t *testing.T) {
	input := "data: [DONE]\n\n"
	events := collect(t, input)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != "[DONE]" {
		t.Fatalf("expected [DONE], got %q", events[0].Data)
	}
}

func TestRead_OpenAIStream(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	events := collect(t, input)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Data != `{"choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}` {
		t.Fatalf("unexpected first event data: %q", events[0].Data)
	}
}

func TestRead_TrimLeadingSpace(t *testing.T) {
	// Per SSE spec, a space after the colon is stripped
	input := "data: hello\n\n"
	events := collect(t, input)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != "hello" {
		t.Fatalf("expected 'hello', got %q", events[0].Data)
	}
}

func TestRead_NoTrailingNewline(t *testing.T) {
	// Stream ends without a final newline — event should still be yielded
	events := collect(t, "data: hello")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != "hello" {
		t.Fatalf("expected 'hello', got %q", events[0].Data)
	}
}

func TestRead_ReaderError(t *testing.T) {
	r := &errReader{err: io.ErrUnexpectedEOF}
	var gotErr error
	for evt, err := range sse.Read(r, nil) {
		if err != nil {
			gotErr = err
			break
		}
		_ = evt
	}
	if gotErr != io.ErrUnexpectedEOF {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", gotErr)
	}
}

type errReader struct {
	err error
}

func (r *errReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// -- Writer tests --

func TestWriteEvent_DataOnly(t *testing.T) {
	var buf bytes.Buffer
	err := sse.WriteEvent(&buf, sse.Event{Data: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "data: hello\n\n" {
		t.Fatalf("expected 'data: hello\\n\\n', got %q", buf.String())
	}
}

func TestWriteEvent_WithType(t *testing.T) {
	var buf bytes.Buffer
	err := sse.WriteEvent(&buf, sse.Event{Type: "done", Data: "finished"})
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "event: done\ndata: finished\n\n" {
		t.Fatalf("expected 'event: done\\ndata: finished\\n\\n', got %q", buf.String())
	}
}

func TestWriteEvent_WithID(t *testing.T) {
	var buf bytes.Buffer
	err := sse.WriteEvent(&buf, sse.Event{LastEventID: "42", Data: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "id: 42\ndata: hello\n\n" {
		t.Fatalf("expected 'id: 42\\ndata: hello\\n\\n', got %q", buf.String())
	}
}

func TestWriteEvent_MultiLineData(t *testing.T) {
	var buf bytes.Buffer
	err := sse.WriteEvent(&buf, sse.Event{Data: "line1\nline2"})
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "data: line1\ndata: line2\n\n" {
		t.Fatalf("expected 'data: line1\\ndata: line2\\n\\n', got %q", buf.String())
	}
}

func TestWriteEvent_Empty(t *testing.T) {
	var buf bytes.Buffer
	err := sse.WriteEvent(&buf, sse.Event{})
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "\n" {
		t.Fatalf("expected '\\n', got %q", buf.String())
	}
}

func TestWriteEvent_RoundTrip(t *testing.T) {
	input := sse.Event{
		LastEventID: "1",
		Type:        "message",
		Data:        "hello\nworld",
	}
	var buf bytes.Buffer
	if err := sse.WriteEvent(&buf, input); err != nil {
		t.Fatal(err)
	}

	var events []sse.Event
	for evt, err := range sse.Read(strings.NewReader(buf.String()), nil) {
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, evt)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].LastEventID != input.LastEventID {
		t.Fatalf("expected id %q, got %q", input.LastEventID, events[0].LastEventID)
	}
	if events[0].Data != input.Data {
		t.Fatalf("expected data %q, got %q", input.Data, events[0].Data)
	}
}
