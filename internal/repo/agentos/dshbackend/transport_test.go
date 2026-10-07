package dshbackend

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Fake-dsh environment keys. A test re-execs its own binary with these set and
// -test.run=TestDSHFakeHelper; the helper then plays a scripted dsh SDK over
// stdio. The script name selects what the fake does after session/prompt.
const (
	envDSHFakeHelper = "GOAGENT_DSH_FAKE_HELPER"
	envDSHFakeScript = "GOAGENT_DSH_FAKE_SCRIPT"
)

// Fake dsh scripts. The transport never appends --profile when Profile is
// empty, so the re-exec'd test binary receives only -test.run and the script
// env — no flag registration needed.
const (
	fakeScriptMinimal     = "minimal"
	fakeScriptStream      = "stream"
	fakeScriptCancel      = "cancel"
	fakeScriptPromptError = "prompt-error"
	fakeScriptDieOnPrompt = "die-on-prompt"
	fakeScriptBadShutdown = "bad-shutdown"

	// fakeCancelHoldTime keeps the fake alive after it emits the turn/end on
	// shutdown, so the consumer drains the notification before the process exit
	// fires the client's done channel.
	fakeCancelHoldTime = 200 * time.Millisecond
)

// fakeBackendName identifies the fake dsh backend in test configs.
const fakeBackendName = "fake-dsh"

// testExecutable resolves the path of the running test binary, which the fake
// dsh re-execs as its subprocess.
func testExecutable(t *testing.T) string {
	t.Helper()

	exe, err := os.Executable()
	require.NoError(t, err)

	return exe
}

// newFakeClient spawns the test binary as a fake dsh SDK subprocess and shuts
// it down via the JSON-RPC shutdown handshake when the test ends.
func newFakeClient(t *testing.T, script string) *sdkClient {
	t.Helper()

	client := newSDKClient(&Config{
		Name:    fakeBackendName,
		Command: testExecutable(t),
		Args:    []string{"-test.run=TestDSHFakeHelper"},
		Env: []string{
			envDSHFakeHelper + "=1",
			envDSHFakeScript + "=" + script,
		},
	}, nil)

	require.NoError(t, client.connect())

	t.Cleanup(func() {
		require.NoError(t, client.close(context.Background()))
	})

	return client
}

// TestDSHFakeHelper is the fake dsh SDK process. It only acts when re-exec'd
// with the helper env set; in a regular test run it returns immediately.
func TestDSHFakeHelper(t *testing.T) {
	t.Parallel()

	if os.Getenv(envDSHFakeHelper) != "1" {
		return
	}

	script := os.Getenv(envDSHFakeScript)

	var sessionID string

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		var msg sdkMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}

		if len(msg.ID) == 0 {
			continue // client notifications are not part of the script
		}

		if serveFakeRequest(os.Stdout, &msg, script, &sessionID) {
			return
		}
	}
}

// serveFakeRequest answers one client frame according to the script, returning
// true when the fake process should exit. Requests without an id are client
// notifications, which are not part of the script.
func serveFakeRequest(w io.Writer, msg *sdkMessage, script string, sessionID *string) bool {
	switch msg.Method {
	case methodInitialize:
		writeFakeFrame(w, fakeInitResponse(msg.ID))
	case methodSessionPrompt:
		*sessionID = fakeSessionID(msg.Params)
		handleFakePrompt(w, msg.ID, script, msg.Params)

		if script == fakeScriptDieOnPrompt {
			return true // exit without a prompt response; the parent sees process death
		}
	case methodShutdown:
		serveFakeShutdown(w, msg, script, *sessionID)

		return true
	default:
		writeFakeFrame(w, &sdkMessage{JSONRPC: "2.0", ID: msg.ID, Error: &sdkError{Code: -32601, Message: "method not found"}})
	}

	return false
}

// serveFakeShutdown answers a shutdown request. The cancel script emits the
// deferred turn/end here, and holds the process briefly so the consumer drains
// it before the exit fires the client's done channel.
func serveFakeShutdown(w io.Writer, msg *sdkMessage, script, sessionID string) {
	if script == fakeScriptCancel {
		emitFakeTurnEnd(w, sessionID)
	}

	if script == fakeScriptBadShutdown {
		writeFakeFrame(w, &sdkMessage{JSONRPC: "2.0", ID: msg.ID, Error: &sdkError{Code: -32602, Message: "invalid params"}})
	} else {
		writeFakeFrame(w, fakeOKResponse(msg.ID))
	}

	if script == fakeScriptCancel {
		time.Sleep(fakeCancelHoldTime)
	}
}

// fakeSessionID extracts the session id from a session/prompt request's params.
func fakeSessionID(params json.RawMessage) string {
	var p sessionPromptParams
	if err := json.Unmarshal(params, &p); err != nil {
		return ""
	}

	return p.SessionID
}

// emitFakeTurnEnd writes a completed turn/end for a session. The cancel script
// uses it to close a run only after the parent applied its cancel flag.
func emitFakeTurnEnd(w io.Writer, sessionID string) {
	ev := sessionEvent{Type: eventTurnEnd, Seq: 1, Time: time.Now().UnixMilli(), Data: json.RawMessage(`{"turn":1,"reason":"completed"}`)}
	writeFakeNotification(w, methodSessionEvent, sessionEventNotification{SessionID: sessionID, Event: ev})
}

// handleFakePrompt answers a session/prompt request according to the script.
func handleFakePrompt(w io.Writer, id json.RawMessage, script string, params json.RawMessage) {
	switch script {
	case fakeScriptPromptError:
		writeFakeFrame(w, &sdkMessage{JSONRPC: "2.0", ID: id, Error: &sdkError{Code: -32603, Message: "no model available"}})
	case fakeScriptDieOnPrompt:
		return // no response: handled by the caller's exit
	default:
		writeFakeFrame(w, fakePromptResponse(id))

		if script == fakeScriptStream {
			emitFakeStream(w, params)
		}
	}
}

// emitFakeStream writes a scripted turn — two text deltas, a tool call and its
// result, and a completed turn/end — followed by an idle status.
func emitFakeStream(w io.Writer, params json.RawMessage) {
	var p sessionPromptParams
	if err := json.Unmarshal(params, &p); err != nil {
		return
	}

	now := time.Now().UnixMilli()
	events := []sessionEvent{
		{Type: eventTurnStart, Seq: 1, Time: now, Data: json.RawMessage(`{"turn":1,"step":0}`)},
		{Type: eventAssistantChunk, Seq: 2, Time: now, Data: json.RawMessage(`{"turn":1,"step":1,"chunk":{"type":"text-delta","index":0,"text":"Hello "}}`)},
		{Type: eventAssistantChunk, Seq: 3, Time: now, Data: json.RawMessage(`{"turn":1,"step":1,"chunk":{"type":"text-delta","index":0,"text":"world"}}`)},
		{Type: eventToolCall, Seq: 4, Time: now, Data: json.RawMessage(`{"turn":1,"step":1,"callId":"call_1","name":"web_search","arguments":"{}"}`)},
		{Type: eventToolResult, Seq: 5, Time: now, Data: json.RawMessage(`{"turn":1,"step":1,"message":{"content":[{"type":"tool-result","toolCallId":"call_1","content":[{"type":"text","text":"results"}]}]}}`)},
		{Type: eventTurnEnd, Seq: 6, Time: now, Data: json.RawMessage(`{"turn":1,"reason":"completed"}`)},
	}

	for _, ev := range events {
		writeFakeNotification(w, methodSessionEvent, sessionEventNotification{SessionID: p.SessionID, Event: ev})
	}

	writeFakeNotification(w, methodSessionStatus, sessionStatusNotification{SessionID: p.SessionID, Status: "idle"})
}

// ——— fake frame writers ———

func fakeInitResponse(id json.RawMessage) *sdkMessage {
	return &sdkMessage{JSONRPC: "2.0", ID: id, Result: rawResult(map[string]any{"serverInfo": map[string]any{"name": fakeBackendName, "version": "0.0.0"}})}
}

func fakePromptResponse(id json.RawMessage) *sdkMessage {
	return &sdkMessage{JSONRPC: "2.0", ID: id, Result: rawResult(map[string]any{"messageId": testMessageID})}
}

func fakeOKResponse(id json.RawMessage) *sdkMessage {
	return &sdkMessage{JSONRPC: "2.0", ID: id, Result: rawResult(map[string]any{})}
}

func writeFakeFrame(w io.Writer, msg *sdkMessage) {
	encoded, err := json.Marshal(msg)
	if err != nil {
		return
	}

	if _, err := w.Write(append(encoded, '\n')); err != nil {
		return
	}
}

func writeFakeNotification(w io.Writer, method string, params any) {
	raw, err := json.Marshal(params)
	if err != nil {
		return
	}

	writeFakeFrame(w, &sdkMessage{JSONRPC: "2.0", Method: method, Params: raw})
}

func rawResult(v any) json.RawMessage {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil
	}

	return encoded
}

// ——— transport tests ———

func TestSDKClientHandshake(t *testing.T) {
	t.Parallel()

	client := newFakeClient(t, fakeScriptMinimal)
	ctx := t.Context()

	require.NoError(t, client.ensureInitialized(ctx))

	result, err := client.prompt(ctx, testSessionID, testPromptText)
	require.NoError(t, err)
	require.JSONEq(t, `{"messageId":"`+testMessageID+`"}`, string(result))
}

func TestSDKClientDispatchesNotifications(t *testing.T) {
	t.Parallel()

	client := newFakeClient(t, fakeScriptStream)
	ctx := t.Context()

	require.NoError(t, client.ensureInitialized(ctx))

	events, _ := client.subscribe(testSessionID)

	_, err := client.prompt(ctx, testSessionID, testPromptText)
	require.NoError(t, err)

	wantEvents := []string{eventTurnStart, eventAssistantChunk, eventAssistantChunk, eventToolCall, eventToolResult, eventTurnEnd}

	for _, want := range wantEvents {
		select {
		case notification := <-events:
			require.Equal(t, methodSessionEvent, notification.Method)

			var envelope sessionEventNotification
			require.NoError(t, json.Unmarshal(notification.Params, &envelope))
			require.Equal(t, testSessionID, envelope.SessionID)
			require.Equal(t, want, envelope.Event.Type)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s notification", want)
		}
	}

	select {
	case notification := <-events:
		require.Equal(t, methodSessionStatus, notification.Method)

		var status sessionStatusNotification
		require.NoError(t, json.Unmarshal(notification.Params, &status))
		require.Equal(t, testSessionID, status.SessionID)
		require.Equal(t, "idle", status.Status)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for session.status notification")
	}
}

func TestSDKClientPromptErrorResponse(t *testing.T) {
	t.Parallel()

	client := newFakeClient(t, fakeScriptPromptError)
	ctx := t.Context()

	require.NoError(t, client.ensureInitialized(ctx))

	_, err := client.prompt(ctx, testSessionID, testPromptText)
	require.ErrorIs(t, err, errDSHCallFailed)
}

func TestSDKClientFailsWhenProcessDies(t *testing.T) {
	t.Parallel()

	client := newFakeClient(t, fakeScriptDieOnPrompt)
	ctx := t.Context()

	require.NoError(t, client.ensureInitialized(ctx))

	_, err := client.prompt(ctx, testSessionID, testPromptText)
	require.ErrorIs(t, err, errDSHProcessDied)
}

func TestSDKClientCloseToleratesFailedShutdown(t *testing.T) {
	t.Parallel()

	client := newFakeClient(t, fakeScriptBadShutdown)
	ctx := t.Context()

	require.NoError(t, client.ensureInitialized(ctx))

	require.NoError(t, client.close(ctx))
}
