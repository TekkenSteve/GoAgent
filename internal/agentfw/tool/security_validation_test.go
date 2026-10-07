package tool

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/stretchr/testify/require"
)

// The security suite states what the pipeline protects: a decision is recorded
// before a call runs, credentials never reach the record, and a pipeline
// missing a protective stage refuses to be built at all.

type recordAuditSink struct {
	records []AuditRecord
}

func (s *recordAuditSink) Write(_ context.Context, record *AuditRecord) error {
	s.records = append(s.records, *record)

	return nil
}

// failingAuditSink cannot record anything, the way an unreachable audit store
// behaves.
type failingAuditSink struct{}

func (failingAuditSink) Write(_ context.Context, _ *AuditRecord) error {
	return errAuditUnavailable
}

var errAuditUnavailable = errors.New("audit store unavailable")

// allowAllAuthorizer permits everything; tests that are about what happens
// around the decision use it, and the ones about the decision use the tenant
// authorizer below.
type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(_ context.Context, _ *Request) error { return nil }

type staticRawExecutor struct{ calls int }

func (e *staticRawExecutor) Execute(_ context.Context, _ *Request) (RawResult, error) {
	e.calls++

	return RawResult{Payload: map[string]any{
		"password":     "p@ss",
		"access_token": "secret-token",
		"token_count":  42,
		"safe_field":   "ok",
	}}, nil
}

type countingIdempotency struct{ results map[string]Result }

func newCountingIdempotency() *countingIdempotency {
	return &countingIdempotency{results: map[string]Result{}}
}

func (s *countingIdempotency) Get(_ context.Context, key string) (Result, bool, error) {
	result, ok := s.results[key]

	return result, ok, nil
}

func (s *countingIdempotency) Put(_ context.Context, key string, result *Result) error {
	s.results[key] = *result

	return nil
}

func testPipeline(t *testing.T, cfg *PipelineConfig) *Pipeline {
	t.Helper()

	if cfg.Redactor == nil {
		cfg.Redactor = NewRedactor(nil)
	}

	if cfg.Policies == nil {
		cfg.Policies = &DefaultPolicyProvider{}
	}

	if cfg.Idempotency == nil {
		cfg.Idempotency = newCountingIdempotency()
	}

	pipeline, err := NewPipeline(cfg)
	require.NoError(t, err)

	return pipeline
}

func TestNewPipelineRefusesAMissingProtectiveStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(cfg *PipelineConfig)
		missing string
	}{
		{
			name:    "no authorizer",
			mutate:  func(cfg *PipelineConfig) { cfg.Authorizer = nil },
			missing: "Authorizer",
		},
		{
			name:    "no executor",
			mutate:  func(cfg *PipelineConfig) { cfg.Executor = nil },
			missing: "Executor",
		},
		{
			name:    "no redactor",
			mutate:  func(cfg *PipelineConfig) { cfg.Redactor = nil },
			missing: "Redactor",
		},
		{
			name:    "no policies",
			mutate:  func(cfg *PipelineConfig) { cfg.Policies = nil },
			missing: "Policies",
		},
		{
			name:    "no idempotency store",
			mutate:  func(cfg *PipelineConfig) { cfg.Idempotency = nil },
			missing: "Idempotency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &PipelineConfig{
				Authorizer:  allowAllAuthorizer{},
				Executor:    &staticRawExecutor{},
				Redactor:    NewRedactor(nil),
				Policies:    &DefaultPolicyProvider{},
				Idempotency: newCountingIdempotency(),
			}

			tt.mutate(cfg)

			pipeline, err := NewPipeline(cfg)

			require.ErrorIs(t, err, ErrPipelineAssembly)
			require.Contains(t, err.Error(), tt.missing)
			require.Nil(t, pipeline)
		})
	}
}

func TestPipelineRedactsCredentialsBeforeRecording(t *testing.T) {
	t.Parallel()

	pipeline := testPipeline(t, &PipelineConfig{
		Authorizer: allowAllAuthorizer{},
		Executor:   &staticRawExecutor{},
	})

	result, err := pipeline.Execute(context.Background(), &Request{
		RunID:      "run-sec",
		ToolCallID: "call-1",
		ToolName:   "dangerous",
	})
	require.NoError(t, err)

	require.Equal(t, "[REDACTED]", result.Output["password"])
	require.Equal(t, "[REDACTED]", result.Output["access_token"])
	require.Equal(t, "ok", result.Output["safe_field"])

	// An exact key list, not a search for suspicious substrings: a token
	// *count* is a number a tool legitimately returns.
	require.Equal(t, 42, result.Output["token_count"])
}

func TestPipelineRedactsNestedCredentials(t *testing.T) {
	t.Parallel()

	pipeline := testPipeline(t, &PipelineConfig{
		Authorizer: allowAllAuthorizer{},
		Executor: executorFunc(func(_ context.Context, _ *Request) (RawResult, error) {
			return RawResult{Payload: map[string]any{
				"headers": map[string]any{"authorization": "Bearer x", "accept": "json"},
				"items": []any{
					map[string]any{"client_secret": "s", "name": "a"},
				},
			}}, nil
		}),
	})

	result, err := pipeline.Execute(context.Background(), &Request{ToolName: "http"})
	require.NoError(t, err)

	headers, ok := result.Output["headers"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", headers["authorization"])
	require.Equal(t, "json", headers["accept"])

	items, ok := result.Output["items"].([]any)
	require.True(t, ok)

	first, ok := items[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", first["client_secret"])
}

// Credentials hide inside containers as readily as at the top level: an array
// of arrays of objects is where a key-name check that does not walk every
// container type leaks one.
func TestPipelineRedactsCredentialsInsideNestedArrays(t *testing.T) {
	t.Parallel()

	pipeline := testPipeline(t, &PipelineConfig{
		Authorizer: allowAllAuthorizer{},
		Executor: executorFunc(func(_ context.Context, _ *Request) (RawResult, error) {
			return RawResult{Payload: map[string]any{
				"batches": []any{
					[]any{
						map[string]any{"access_token": "t1", "name": "first"},
						[]any{map[string]any{"client_secret": "s1", "ok": true}},
					},
					map[string]any{"nested": map[string]any{"private_key": "k1"}},
				},
			}}, nil
		}),
	})

	result, err := pipeline.Execute(context.Background(), &Request{ToolName: "http"})
	require.NoError(t, err)

	batches, ok := result.Output["batches"].([]any)
	require.True(t, ok)

	firstBatch, ok := batches[0].([]any)
	require.True(t, ok)

	firstEntry, ok := firstBatch[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", firstEntry["access_token"])
	require.Equal(t, "first", firstEntry["name"])

	inner, ok := firstBatch[1].([]any)
	require.True(t, ok)

	innerEntry, ok := inner[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", innerEntry["client_secret"])
	require.Equal(t, true, innerEntry["ok"])

	secondBatch, ok := batches[1].(map[string]any)
	require.True(t, ok)

	nested, ok := secondBatch["nested"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", nested["private_key"])

	// Nothing sensitive survives anywhere in the recorded payload.
	encoded, err := json.Marshal(result.Output)
	require.NoError(t, err)

	for _, leaked := range []string{"t1", "s1", "k1"} {
		require.NotContains(t, string(encoded), leaked, "a credential survived redaction")
	}
}

// A denied call is refused and the decision is on record.
func TestPipelineDeniedCallIsRefusedAndAudited(t *testing.T) {
	t.Parallel()

	audit := &recordAuditSink{}
	executor := &staticRawExecutor{}

	pipeline := testPipeline(t, &PipelineConfig{
		Authorizer: &TenantAuthorizer{
			Authorizer: denyAllPolicy{},
			Audit:      audit,
		},
		Executor: executor,
	})

	_, err := pipeline.Execute(context.Background(), &Request{
		RunID:     "run-denied",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		ToolName:  "payments.charge",
	})

	require.ErrorIs(t, err, ErrAuthorization)
	require.Zero(t, executor.calls, "a denied tool must not execute")

	require.Len(t, audit.records, 1)
	require.False(t, audit.records[0].Allowed)
	require.Equal(t, "payments.charge", audit.records[0].ToolName)
	require.Equal(t, "acct-1", audit.records[0].AccountID)
	require.False(t, audit.records[0].OccurredAt.IsZero(), "a decision records when it was made")
}

// An allowed call is recorded too: an audit trail that only holds denials
// cannot answer what ran.
func TestPipelineAllowedCallIsAudited(t *testing.T) {
	t.Parallel()

	audit := &recordAuditSink{}

	pipeline := testPipeline(t, &PipelineConfig{
		Authorizer: &TenantAuthorizer{Authorizer: allowOwnTenantPolicy{}, Audit: audit},
		Executor:   &staticRawExecutor{},
	})

	_, err := pipeline.Execute(context.Background(), &Request{
		AccountID: "acct-1",
		ProjectID: "proj-1",
		ToolName:  "search",
	})
	require.NoError(t, err)

	require.Len(t, audit.records, 1)
	require.True(t, audit.records[0].Allowed)
}

// A decision that cannot be recorded fails the call: an audit trail with holes
// cannot answer who ran what.
func TestPipelineUnrecordableDecisionFailsClosed(t *testing.T) {
	t.Parallel()

	executor := &staticRawExecutor{}

	pipeline := testPipeline(t, &PipelineConfig{
		Authorizer: &TenantAuthorizer{Authorizer: allowOwnTenantPolicy{}, Audit: failingAuditSink{}},
		Executor:   executor,
	})

	_, err := pipeline.Execute(context.Background(), &Request{
		AccountID: "acct-1",
		ProjectID: "proj-1",
		ToolName:  "search",
	})

	require.Error(t, err)
	require.Zero(t, executor.calls, "an unrecordable decision must not run the tool")
}

// The tool-level subject is the run's own principal: the account came from the
// run the platform launched under a verified credential.
func TestTenantAuthorizerUsesTheRunsPrincipal(t *testing.T) {
	t.Parallel()

	seen := &capturingAuthorizer{}

	authorizer := &TenantAuthorizer{Authorizer: seen}

	err := authorizer.Authorize(context.Background(), &Request{
		AccountID: "acct-1",
		ProjectID: "proj-1",
		ToolName:  "search",
	})
	require.NoError(t, err)
	require.NotNil(t, seen.request)

	require.Equal(t, agentoscore.PrincipalSourceRun, seen.request.Principal.Source)
	require.Equal(t, "acct-1", seen.request.Principal.AccountID)
	require.Equal(t, agentoscore.ActionToolExecute, seen.request.Action)
	require.Equal(t, "tool", seen.request.Object.Kind)
	require.Equal(t, "search", seen.request.Object.ID)
	require.Equal(t, "acct-1", seen.request.Object.Tenant.AccountID)
	require.Equal(t, "proj-1", seen.request.Object.Tenant.ProjectID)
}

// An empty account cannot be authorized: the request is incomplete, and the
// only safe answer to "I cannot tell" is no.
func TestTenantAuthorizerRefusesAnIncompleteRequest(t *testing.T) {
	t.Parallel()

	authorizer := &TenantAuthorizer{Authorizer: allowOwnTenantPolicy{}}

	require.Error(t, authorizer.Authorize(context.Background(), &Request{ToolName: "search"}))
}

func TestTenantAuthorizerRefusesWithoutAPolicy(t *testing.T) {
	t.Parallel()

	authorizer := &TenantAuthorizer{}

	require.ErrorIs(t, authorizer.Authorize(context.Background(), &Request{AccountID: "a"}), ErrNoAuthorizer)
}

// ——— test doubles ———

type executorFunc func(context.Context, *Request) (RawResult, error)

func (f executorFunc) Execute(ctx context.Context, req *Request) (RawResult, error) {
	return f(ctx, req)
}

// allowOwnTenantPolicy is the built-in platform decision: an object may be
// acted on when it belongs to the principal's account.
type allowOwnTenantPolicy struct{}

func (allowOwnTenantPolicy) Authorize(_ context.Context, request *agentoscore.AuthorizeRequest) error {
	return request.Validate()
}

// denyAllPolicy is a policy engine that refuses everything.
type denyAllPolicy struct{}

func (denyAllPolicy) Authorize(_ context.Context, request *agentoscore.AuthorizeRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}

	return agentoscore.ErrForbidden
}

type capturingAuthorizer struct{ request *agentoscore.AuthorizeRequest }

func (c *capturingAuthorizer) Authorize(_ context.Context, request *agentoscore.AuthorizeRequest) error {
	c.request = request

	return request.Validate()
}
