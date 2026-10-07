package tool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	errTestTemporary    = errors.New("temporary")
	errTestMissingQuery = errors.New("missing query")
)

// ——— test doubles ———

// flakyExecutor fails its first failFirst calls, then succeeds.
type flakyExecutor struct {
	mu        sync.Mutex
	calls     int
	failFirst int
}

func (e *flakyExecutor) Execute(_ context.Context, _ *Request) (RawResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.calls++

	if e.calls <= e.failFirst {
		return RawResult{}, errTestTemporary
	}

	return RawResult{Payload: map[string]any{"value": "ok", "api_token": "secret-token"}}, nil
}

// alwaysFailExecutor fails every attempt, the way a genuinely broken tool or a
// dead dependency does.
type alwaysFailExecutor struct {
	mu    sync.Mutex
	calls int
}

func (e *alwaysFailExecutor) Execute(_ context.Context, _ *Request) (RawResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.calls++

	return RawResult{Payload: map[string]any{"partial": "garbage"}}, errTestTemporary
}

func (e *alwaysFailExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.calls
}

type mapIdempotency struct {
	mu   sync.RWMutex
	data map[string]Result
}

func newMapIdempotency() *mapIdempotency {
	return &mapIdempotency{data: map[string]Result{}}
}

func (m *mapIdempotency) Get(_ context.Context, key string) (Result, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[key]

	return v, ok, nil
}

func (m *mapIdempotency) Put(_ context.Context, key string, result *Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = *result

	return nil
}

type requiredArgValidator struct{}

func (requiredArgValidator) Validate(_ context.Context, req *Request) error {
	if _, ok := req.Args["query"]; !ok {
		return errTestMissingQuery
	}

	return nil
}

type staticPolicyProvider struct{ policy Policy }

func (s staticPolicyProvider) GetPolicy(_ string) Policy { return s.policy }

func testPipelineWith(t *testing.T, executor Executor, policies PolicyProvider) *Pipeline {
	t.Helper()

	return testPipeline(t, &PipelineConfig{
		Validator:  requiredArgValidator{},
		Authorizer: allowAllAuthorizer{},
		Executor:   executor,
		Policies:   policies,
	})
}

// ——— tests ———

func TestPipelineValidatesAuthorizesExecutesAndRedacts(t *testing.T) {
	t.Parallel()

	executor := &flakyExecutor{}
	pipeline := testPipelineWith(t, executor, &DefaultPolicyProvider{})

	result, err := pipeline.Execute(context.Background(), &Request{
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "search",
		Args:       map[string]any{"query": "hello"},
	})
	require.NoError(t, err)

	require.Equal(t, "call-1", result.ToolCallID)
	require.Equal(t, 1, result.Attempts)
	require.Equal(t, "ok", result.Output["value"])
	require.Equal(t, "[REDACTED]", result.Output["api_token"])
}

func TestPipelineValidationFailureStopsBeforeTheExecutor(t *testing.T) {
	t.Parallel()

	executor := &flakyExecutor{}
	pipeline := testPipelineWith(t, executor, &DefaultPolicyProvider{})

	_, err := pipeline.Execute(context.Background(), &Request{ToolName: "search", Args: map[string]any{}})

	require.ErrorIs(t, err, ErrValidation)
	require.Zero(t, executor.calls, "an invalid call must not reach the tool")
}

// A tool that fails every attempt must surface as an error carrying the
// attempt count. It once returned the last attempt's payload with a nil error,
// which recorded a tool result that never happened.
func TestPipelineExhaustedRetriesFails(t *testing.T) {
	t.Parallel()

	executor := &alwaysFailExecutor{}
	pipeline := testPipelineWith(t, executor, staticPolicyProvider{
		policy: Policy{Timeout: time.Second, MaxAttempts: 3, RetryBackoff: time.Millisecond},
	})

	result, err := pipeline.Execute(context.Background(), &Request{
		ToolName: "search",
		Args:     map[string]any{"query": "go"},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrExecution)
	require.Contains(t, err.Error(), "3 attempt(s)")
	require.Empty(t, result.Output, "a failed call has no output to record")
	require.Equal(t, 3, executor.callCount())
}

// The baseline policy runs a tool once. Retrying a call that may already have
// had its side effect is how a transient failure becomes a duplicate.
func TestDefaultPolicyDoesNotRetry(t *testing.T) {
	t.Parallel()

	executor := &alwaysFailExecutor{}
	pipeline := testPipelineWith(t, executor, &DefaultPolicyProvider{})

	_, err := pipeline.Execute(context.Background(), &Request{
		ToolName: "payments.charge",
		Args:     map[string]any{"query": "charge"},
	})
	require.Error(t, err)
	require.Equal(t, 1, executor.callCount(), "the baseline makes one attempt")
}

func TestDefaultPolicyOverridePerTool(t *testing.T) {
	t.Parallel()

	provider := &DefaultPolicyProvider{
		Policies: map[string]Policy{
			"flaky_search": {Timeout: time.Second, MaxAttempts: 3, RetryBackoff: time.Millisecond},
		},
	}

	require.Equal(t, 3, provider.GetPolicy("flaky_search").MaxAttempts)
	require.Equal(t, 1, provider.GetPolicy("payments.charge").MaxAttempts)
	require.Equal(t, defaultToolTimeout, provider.GetPolicy("payments.charge").Timeout)
}

// The same key answers with the recorded result, so the tool runs once.
func TestPipelineIdempotencyAnswersWithTheRecordedResult(t *testing.T) {
	t.Parallel()

	executor := &flakyExecutor{failFirst: 1}
	pipeline := testPipelineWith(t, executor, staticPolicyProvider{
		policy: Policy{Timeout: time.Second, MaxAttempts: 3, RetryBackoff: time.Millisecond, EnableIdempotent: true},
	})

	req := Request{
		RunID:          "run-1",
		ToolCallID:     "call-2",
		ToolName:       "search",
		IdempotencyKey: "idem-1",
		Args:           map[string]any{"query": "a"},
	}

	first, err := pipeline.Execute(context.Background(), &req)
	require.NoError(t, err)
	require.Equal(t, 2, first.Attempts)
	require.False(t, first.FromIdempotent)

	second, err := pipeline.Execute(context.Background(), &req)
	require.NoError(t, err)
	require.True(t, second.FromIdempotent)
	require.Equal(t, first.Output, second.Output)
	require.Equal(t, 2, executor.calls, "the second call must not touch the tool")
}

// Without the idempotency flag the pipeline executes every call: repeating a
// tool is the tool's business unless a policy says otherwise.
func TestPipelineWithoutIdempotentPolicyReexecutes(t *testing.T) {
	t.Parallel()

	executor := &flakyExecutor{}
	pipeline := testPipelineWith(t, executor, &DefaultPolicyProvider{})

	req := Request{
		ToolName:       "search",
		IdempotencyKey: "idem-1",
		Args:           map[string]any{"query": "a"},
	}

	_, err := pipeline.Execute(context.Background(), &req)
	require.NoError(t, err)

	_, err = pipeline.Execute(context.Background(), &req)
	require.NoError(t, err)

	require.Equal(t, 2, executor.calls)
}
