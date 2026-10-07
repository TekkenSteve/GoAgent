package dshbackend

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// Transport constants.
const (
	// notifyBuffer is each session listener's event backlog. A listener slower
	// than the SDK's stream drops notifications (fail-open) rather than blocking
	// the reader and stalling every other session sharing the process.
	notifyBuffer = 1024

	// maxSDKLineBytes caps a single JSON-RPC frame from the SDK.
	maxSDKLineBytes = 4 * 1024 * 1024

	// scannerBufInitial is the Scanner's initial read buffer. Frames beyond it
	// grow up to maxSDKLineBytes; a small start keeps per-process memory lean.
	scannerBufInitial = 64 * 1024

	// shutdownTimeout bounds the graceful shutdown handshake and the post-kill
	// wait before Close gives up.
	shutdownTimeout = 2 * time.Second

	// sdkProvider and sdkModel are the initialize handshake's model route. dsh
	// rejects an initialize without provider/model; these target the DeepSeek API
	// and are the documented defaults until per-run model routing lands.
	sdkProvider = "deepseek"
	sdkModel    = "deepseek-chat"
)

// Transport error sentinels.
var (
	// errDSHProcessDied reports that the SDK subprocess exited before the run
	// reached a terminal, so the consumer closes the timeline with a failure.
	errDSHProcessDied = errors.New("dsh sdk process exited unexpectedly")

	// errDSHNotConnected reports a write attempted before the subprocess spawned.
	errDSHNotConnected = errors.New("dsh sdk not connected")

	// errDSHShutdownTimedOut reports the process did not exit within the timeout.
	errDSHShutdownTimedOut = errors.New("dsh sdk process did not exit in time")

	// errDSHCallFailed reports a JSON-RPC error response from the SDK.
	errDSHCallFailed = errors.New("dsh sdk call failed")
)

// sdkClient drives one dsh SDK subprocess over newline-delimited JSON-RPC 2.0.
// Requests are matched to responses by id; notifications are broadcast to a
// per-session buffered channel. The client is shared across runs: one process
// hosts many sessions.
type sdkClient struct {
	config Config
	logger logger.Interface

	mu       sync.Mutex
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	pending  map[string]chan sdkResponse
	sessions map[string]chan sdkNotification
	dropped  map[string]bool
	reqSeq   int
	done     chan struct{}
	doneOnce sync.Once
	readErr  error

	initMu      sync.Mutex
	initialized bool
}

// newSDKClient wires a client for one dsh backend config.
func newSDKClient(config *Config, l logger.Interface) *sdkClient {
	return &sdkClient{
		config: *config,
		logger: l,
	}
}

// connect spawns the SDK subprocess and starts its reader, exactly once. The
// call serializes under the client mutex so concurrent Start calls cannot spawn
// a second process.
func (c *sdkClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cmd != nil {
		return nil
	}

	cmd, stdin, stdout, err := spawnSDK(&c.config)
	if err != nil {
		return err
	}

	c.cmd = cmd
	c.stdin = stdin
	c.done = make(chan struct{})

	// The reader blocks on the client mutex until connect returns; it only
	// dispatches to listeners registered afterwards.
	go c.readLoop(stdout)

	return nil
}

// ensureInitialized performs the initialize handshake once per process.
func (c *sdkClient) ensureInitialized(ctx context.Context) error {
	c.initMu.Lock()
	defer c.initMu.Unlock()

	if c.initialized {
		return nil
	}

	if _, err := c.call(ctx, methodInitialize, initializeParams{
		Cwd:      c.config.WorkingDir,
		Provider: sdkProvider,
		Model:    sdkModel,
	}); err != nil {
		return fmt.Errorf("dsh sdk: initialize: %w", err)
	}

	c.initialized = true

	return nil
}

// prompt sends one session/prompt request. The response is synchronous: dsh
// accepts the prompt before any event streams for the session.
func (c *sdkClient) prompt(ctx context.Context, sessionID, text string) (json.RawMessage, error) {
	return c.call(ctx, methodSessionPrompt, sessionPromptParams{
		SessionID: sessionID,
		ContentBlocks: []promptContentBlock{{
			Type: "text",
			Text: text,
		}},
	})
}

// call sends one JSON-RPC request and waits for its matching response.
func (c *sdkClient) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.nextID()

	var rawParams json.RawMessage

	if params != nil {
		encoded, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("dsh sdk: marshal %s params: %w", method, err)
		}

		rawParams = encoded
	}

	respCh := make(chan sdkResponse, 1)

	c.mu.Lock()
	if c.pending == nil {
		c.pending = make(map[string]chan sdkResponse)
	}

	c.pending[id] = respCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.writeLine(&sdkMessage{
		JSONRPC: "2.0",
		ID:      json.RawMessage(strconv.Quote(id)),
		Method:  method,
		Params:  rawParams,
	}); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.processErr()
	case resp := <-respCh:
		if resp.Error != nil {
			return nil, fmt.Errorf("%w: method %s: %s (code %d)", errDSHCallFailed, method, resp.Error.Message, resp.Error.Code)
		}

		return resp.Result, nil
	}
}

// subscribe registers a listener for one session's notifications. The returned
// channel is never closed; consumers select on it together with the client's
// done channel, so a process death unblocks them without a send-on-closed race.
func (c *sdkClient) subscribe(sessionID string) (events <-chan sdkNotification, unsubscribe func()) {
	c.mu.Lock()
	if c.sessions == nil {
		c.sessions = make(map[string]chan sdkNotification)
	}

	ch := make(chan sdkNotification, notifyBuffer)
	c.sessions[sessionID] = ch
	c.mu.Unlock()

	return ch, func() { c.unsubscribe(sessionID) }
}

// unsubscribe removes a session's listener; late notifications for it are
// dropped.
func (c *sdkClient) unsubscribe(sessionID string) {
	c.mu.Lock()
	delete(c.sessions, sessionID)
	delete(c.dropped, sessionID)
	c.mu.Unlock()
}

// close shuts the subprocess down gracefully: shutdown request, stdin EOF as
// the fallback exit trigger, then kill if it still refuses to exit.
func (c *sdkClient) close(ctx context.Context) error {
	c.mu.Lock()
	cmd := c.cmd
	stdin := c.stdin
	c.mu.Unlock()

	if cmd == nil {
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)

	if _, err := c.call(shutdownCtx, methodShutdown, nil); err != nil {
		c.warnf("dsh sdk: shutdown: %v (best-effort)", err)
	}

	cancel()

	_ = stdin.Close()

	select {
	case <-c.done:
		return nil
	case <-time.After(shutdownTimeout):
		if err := cmd.Process.Kill(); err != nil {
			c.warnf("dsh sdk: kill: %v", err)
		}
	}

	select {
	case <-c.done:
		return nil
	case <-time.After(shutdownTimeout):
		return errDSHShutdownTimedOut
	}
}

// readLoop drains the subprocess's stdout, dispatching frames until the process
// exits, then reaps it and fails the client.
func (c *sdkClient) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, scannerBufInitial), maxSDKLineBytes)

	for scanner.Scan() {
		var msg sdkMessage

		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue // dsh ignores malformed frames; so do we
		}

		if len(msg.ID) == 0 && msg.Method != "" {
			c.dispatchNotification(&msg)

			continue
		}

		if len(msg.ID) > 0 {
			c.deliverResponse(&msg)
		}
	}

	if err := c.cmd.Wait(); err != nil {
		c.warnf("dsh sdk: wait: %v", err)
	}

	if err := scanner.Err(); err != nil {
		c.fail(fmt.Errorf("%w: %w", errDSHProcessDied, err))

		return
	}

	c.fail(errDSHProcessDied)
}

// deliverResponse routes a response to its pending request by id.
func (c *sdkClient) deliverResponse(msg *sdkMessage) {
	var id string

	if err := json.Unmarshal(msg.ID, &id); err != nil {
		return
	}

	c.mu.Lock()
	ch, ok := c.pending[id]
	c.mu.Unlock()

	if !ok {
		return
	}

	ch <- sdkResponse{Result: msg.Result, Error: msg.Error}
}

// dispatchNotification forwards a server notification to its session's
// listener, dropping on a full backlog so one slow consumer never stalls the
// shared reader.
func (c *sdkClient) dispatchNotification(msg *sdkMessage) {
	var params struct {
		SessionID string `json:"sessionId"`
	}

	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return
	}

	c.mu.Lock()
	ch, ok := c.sessions[params.SessionID]
	c.mu.Unlock()

	if !ok {
		return
	}

	select {
	case ch <- sdkNotification{Method: msg.Method, Params: msg.Params}:
	default:
		// The notification is the only carrier of the events (and therefore of
		// the milestones) it holds, so dropping one leaves a hole the run would
		// otherwise report as a success. Blocking the reader instead would stall
		// every other session sharing the process, so the drop is recorded and
		// the owning run is failed by its consume loop.
		c.markDropped(params.SessionID)
		c.warnf("dsh sdk: dropping %s notification for session %s (listener backlog)", msg.Method, params.SessionID)
	}
}

// markDropped remembers that a notification for a session was dropped. The flag
// is sticky: once a run's event stream has a hole, it stays incomplete.
func (c *sdkClient) markDropped(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dropped == nil {
		c.dropped = make(map[string]bool)
	}

	c.dropped[sessionID] = true
}

// droppedNotification reports whether any notification for the session was
// dropped, which means the run's timeline is incomplete.
func (c *sdkClient) droppedNotification(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.dropped[sessionID]
}

// fail records the reader's terminal error and unblocks every waiter exactly
// once.
func (c *sdkClient) fail(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}

	done := c.done
	c.mu.Unlock()

	if done != nil {
		c.doneOnce.Do(func() { close(done) })
	}
}

// processErr returns the reader's failure, defaulting to the process-died
// sentinel when the failure carried no detail.
func (c *sdkClient) processErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.readErr != nil {
		return c.readErr
	}

	return errDSHProcessDied
}

// writeLine encodes and sends one frame, appending the newline delimiter.
func (c *sdkClient) writeLine(msg *sdkMessage) error {
	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("dsh sdk: encode frame: %w", err)
	}

	c.mu.Lock()
	stdin := c.stdin
	c.mu.Unlock()

	if stdin == nil {
		return errDSHNotConnected
	}

	if _, err := stdin.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("dsh sdk: write frame: %w", err)
	}

	return nil
}

// nextID assigns the next request id for this process.
func (c *sdkClient) nextID() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reqSeq++

	return strconv.Itoa(c.reqSeq)
}

// warnf forwards a diagnostic to the backend logger, if any.
func (c *sdkClient) warnf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Warn(format, args...)
	}
}

// spawnSDK builds and starts the SDK subprocess. The command and profile come
// from operator configuration, never from request input, so the spawned
// executable is server-controlled rather than attacker-influenced.
func spawnSDK(config *Config) (*exec.Cmd, io.WriteCloser, io.Reader, error) {
	args := make([]string, 0, len(config.Args))

	if config.Profile != "" {
		args = append(args, "--profile", config.Profile)
	}

	args = append(args, config.Args...)

	// #nosec G204 -- the dsh SDK subprocess is the adapter's transport; its
	// command path, profile, and arguments come from server config, not request data.
	cmd := exec.CommandContext(context.Background(), config.Command, args...)

	cmd.Dir = config.WorkingDir

	if len(config.Env) > 0 {
		cmd.Env = append(os.Environ(), config.Env...)
	}

	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dsh sdk: stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dsh sdk: stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("dsh sdk: start %s: %w", config.Command, err)
	}

	return cmd, stdin, stdout, nil
}
