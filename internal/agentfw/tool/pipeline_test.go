package tool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(_ context.Context, _ ToolRequest) error { return nil }

type requiredArgValidator struct{}

func (requiredArgValidator) Validate(_ context.Context, req ToolRequest) error {
	if _, ok := req.Args["query"]; !ok {
		return errors.New("missing query")
	}
	return nil
}

type passthroughNormalizer struct{}

func (passthroughNormalizer) Normalize(_ context.Context, _ ToolRequest, raw RawResult) (map[string]any, error) {
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

func (p *mapPersister) Persist(_ context.Context, _ ToolRequest, normalized map[string]any) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter++
	ref := "persist-" + time.Unix(0, int64(p.counter)).UTC().Format("150405.000")
	p.data[ref] = normalized
	return ref, nil
}

type staticPolicyProvider struct {
	policy ToolPolicy
}

func (s staticPolicyProvider) GetPolicy(_ string) ToolPolicy { return s.policy }

type transientError struct{ error }

type transientClassifier struct{}

func (transientClassifier) IsTransient(err error) bool {
	_, ok := err.(transientError)
	return ok
}

type flakyExecutor struct {
	mu            sync.Mutex
	calls         int
	failFirst     int
	lastArgsToken string
}

func (e *flakyExecutor) Execute(_ context.Context, req ToolRequest) (RawResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.calls <= e.failFirst {
		return RawResult{}, transientError{error: errors.New("temporary")}
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

func (m *mapIdempotency) Put(_ context.Context, key string, result Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = result
	return nil
}

type auditSink struct {
	mu      sync.Mutex
	records []AuditRecord
}

func (a *auditSink) Write(_ context.Context, record AuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)
	return nil
}

func TestPipelineValidateAuthorizeExecuteNormalizePersist(t *testing.T) {
	executor := &flakyExecutor{}
	persister := newMapPersister()

	p := Pipeline{
		Validator:   requiredArgValidator{},
		Authorizer:  allowAllAuthorizer{},
		Executor:    executor,
		Normalizer:  passthroughNormalizer{},
		Persister:   persister,
		Policies:    staticPolicyProvider{policy: ToolPolicy{Timeout: time.Second, MaxAttempts: 1}},
		Isolation:   SideEffectIsolationPolicy{},
		Redactor:    DefaultSecretRedactor{},
		Idempotency: newMapIdempotency(),
	}

	result, err := p.Execute(context.Background(), ToolRequest{
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
	executor := &flakyExecutor{failFirst: 1}
	p := Pipeline{
		Validator:           requiredArgValidator{},
		Authorizer:          allowAllAuthorizer{},
		Executor:            executor,
		Normalizer:          passthroughNormalizer{},
		Persister:           newMapPersister(),
		Policies:            staticPolicyProvider{policy: ToolPolicy{Timeout: time.Second, MaxAttempts: 3, RetryBackoff: time.Millisecond, EnableIdempotent: true}},
		Idempotency:         newMapIdempotency(),
		TransientClassifier: transientClassifier{},
		Redactor:            DefaultSecretRedactor{},
	}

	req := ToolRequest{
		RunID:          "run-1",
		ToolCallID:     "call-2",
		ToolName:       "search",
		IdempotencyKey: "idem-1",
		Args:           map[string]any{"query": "a"},
	}

	first, err := p.Execute(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 2, first.Attempts)

	second, err := p.Execute(context.Background(), req)
	require.NoError(t, err)
	require.True(t, second.FromIdempotent)
	require.Equal(t, 2, executor.calls)
}

func TestConflictDomainScheduling(t *testing.T) {
	batches := BuildExecutionPlan([]ToolRequest{
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
	audit := &auditSink{}
	auth := TierAuthorizer{
		Policy: StaticTierPolicy{
			Allow: map[Tier]map[string]struct{}{
				TierPro: {"search": {}},
			},
		},
		Audit: audit,
	}

	err := auth.Authorize(context.Background(), ToolRequest{
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
	policy := SideEffectIsolationPolicy{}
	require.Equal(t, IsolationShared, policy.Resolve(ToolRequest{ToolName: "search"}))
	require.Equal(t, IsolationIsolated, policy.Resolve(ToolRequest{ToolName: "write-db", SideEffecting: true}))
}
