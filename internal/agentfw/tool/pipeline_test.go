package tool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	errTestMissingQuery = errors.New("missing query")
	errTestTemporary    = errors.New("temporary")
)

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(_ context.Context, _ *Request) error { return nil }

type requiredArgValidator struct{}

func (requiredArgValidator) Validate(_ context.Context, req *Request) error {
	if _, ok := req.Args["query"]; !ok {
		return errTestMissingQuery
	}

	return nil
}

type passthroughNormalizer struct{}

func (passthroughNormalizer) Normalize(_ context.Context, _ *Request, raw RawResult) (map[string]any, error) {
	return raw.Payload, nil
}

type mapPersister struct {
	mu      sync.Mutex
	data    map[string]map[string]any
	counter int
}

func newMapPersister() *mapPersister {
	return &mapPersister{data: map[string]map[string]any{}}
}

func (p *mapPersister) Persist(_ context.Context, _ *Request, normalized map[string]any) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.counter++
	ref := fmt.Sprintf("persist-%d", p.counter)
	p.data[ref] = normalized

	return ref, nil
}

type staticPolicyProvider struct {
	policy Policy
}

func (s staticPolicyProvider) GetPolicy(_ string) Policy { return s.policy }

type transientError struct{ error }

type flakyExecutor struct {
	mu            sync.Mutex
	calls         int
	failFirst     int
	lastArgsToken string
}

func (e *flakyExecutor) Execute(_ context.Context, req *Request) (RawResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.calls++
	if e.calls <= e.failFirst {
		return RawResult{}, transientError{error: errTestTemporary}
	}

	if token, ok := req.Args["api_token"].(string); ok {
		e.lastArgsToken = token
	}

	return RawResult{Payload: map[string]any{"value": "ok", "api_token": "secret-token"}}, nil
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

type auditSink struct {
	mu      sync.Mutex
	records []AuditRecord
}

func (a *auditSink) Write(_ context.Context, record *AuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.records = append(a.records, *record)

	return nil
}

func TestPipelineValidateAuthorizeExecuteNormalizePersist(t *testing.T) {
	t.Parallel()

	executor := &flakyExecutor{}
	persister := newMapPersister()

	p := Pipeline{
		Validator:   requiredArgValidator{},
		Authorizer:  allowAllAuthorizer{},
		Executor:    executor,
		Normalizer:  passthroughNormalizer{},
		Persister:   persister,
		Policies:    staticPolicyProvider{policy: Policy{Timeout: time.Second, MaxAttempts: 1}},
		Isolation:   SideEffectIsolationPolicy{},
		Redactor:    DefaultSecretRedactor{},
		Idempotency: newMapIdempotency(),
	}

	result, err := p.Execute(context.Background(), &Request{
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "search",
		Args:       map[string]any{"query": "hello"},
	})
	require.NoError(t, err)
	require.Equal(t, "call-1", result.ToolCallID)
	require.Equal(t, 1, result.Attempts)
	require.NotEmpty(t, result.PersistedRef)
	require.Equal(t, "[REDACTED]", result.Output["api_token"])
}

func TestPipelineRetryAndIdempotency(t *testing.T) {
	t.Parallel()

	executor := &flakyExecutor{failFirst: 1}
	p := Pipeline{
		Validator:   requiredArgValidator{},
		Authorizer:  allowAllAuthorizer{},
		Executor:    executor,
		Normalizer:  passthroughNormalizer{},
		Persister:   newMapPersister(),
		Policies:    staticPolicyProvider{policy: Policy{Timeout: time.Second, MaxAttempts: 3, RetryBackoff: time.Millisecond, EnableIdempotent: true}},
		Idempotency: newMapIdempotency(),
		Redactor:    DefaultSecretRedactor{},
	}

	req := Request{
		RunID:          "run-1",
		ToolCallID:     "call-2",
		ToolName:       "search",
		IdempotencyKey: "idem-1",
		Args:           map[string]any{"query": "a"},
	}

	first, err := p.Execute(context.Background(), &req)
	require.NoError(t, err)
	require.Equal(t, 2, first.Attempts)

	second, err := p.Execute(context.Background(), &req)
	require.NoError(t, err)
	require.True(t, second.FromIdempotent)
	require.Equal(t, 2, executor.calls)
}

func TestConflictDomainScheduling(t *testing.T) {
	t.Parallel()

	batches := BuildExecutionPlan([]Request{
		{ToolCallID: "1", ConflictDomain: "db"},
		{ToolCallID: "2", ConflictDomain: "db"},
		{ToolCallID: "3", ConflictDomain: "cache"},
		{ToolCallID: "4", ConflictDomain: ""},
	}, 3)

	require.Len(t, batches, 2)
	require.Len(t, batches[0].Requests, 3)
	require.Len(t, batches[1].Requests, 1)
	require.Equal(t, "2", batches[1].Requests[0].ToolCallID)
}

func TestTierAuthorizationAndAudit(t *testing.T) {
	t.Parallel()

	audit := &auditSink{}
	auth := TierAuthorizer{
		Policy: StaticTierPolicy{
			Allow: map[Tier]map[string]struct{}{
				TierPro: {"search": {}},
			},
		},
		Audit: audit,
	}

	err := auth.Authorize(context.Background(), &Request{
		RunID:     "run-2",
		AccountID: "acc",
		ProjectID: "proj",
		ToolName:  "admin-tool",
		Tier:      TierFree,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAuthorization)
	require.Len(t, audit.records, 1)
	require.False(t, audit.records[0].Allowed)
}

func TestIsolationRouting(t *testing.T) {
	t.Parallel()

	policy := SideEffectIsolationPolicy{}
	require.Equal(t, ExecutionIsolationShared, policy.Resolve(&Request{ToolName: "search"}))
	require.Equal(t, ExecutionIsolationIsolated, policy.Resolve(&Request{ToolName: "write-db", SideEffecting: true}))
}
