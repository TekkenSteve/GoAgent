package tool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type denyAllPolicy struct{}

func (denyAllPolicy) IsAllowed(_ Tier, _ string) bool { return false }

type allowPolicy struct{}

func (allowPolicy) IsAllowed(t Tier, name string) bool {
	return t == TierEnterprise && name == "admin-tool"
}

type recordAuditSink struct {
	records []AuditRecord
}

func (s *recordAuditSink) Write(_ context.Context, record AuditRecord) error {
	s.records = append(s.records, record)
	return nil
}

type nopValidator struct{}

func (nopValidator) Validate(_ context.Context, _ ToolRequest) error { return nil }

type staticRawExecutor struct{}

func (staticRawExecutor) Execute(_ context.Context, _ ToolRequest) (RawResult, error) {
	return RawResult{Payload: map[string]any{
		"password":  "p@ss",
		"token":     "secret-token",
		"safe_field": "ok",
	}}, nil
}

type staticPersister struct{ saved map[string]any }

func (p *staticPersister) Persist(_ context.Context, _ ToolRequest, normalized map[string]any) (string, error) {
	p.saved = normalized
	return "ref-1", nil
}

type allowAllAuth struct{}

func (allowAllAuth) Authorize(_ context.Context, _ ToolRequest) error { return nil }

func TestSecuritySuiteIsolationBoundaryRouting(t *testing.T) {
	policy := SideEffectIsolationPolicy{}
	require.Equal(t, IsolationShared, policy.Resolve(ToolRequest{ToolName: "search", SideEffecting: false}))
	require.Equal(t, IsolationIsolated, policy.Resolve(ToolRequest{ToolName: "payments.charge", SideEffecting: true}))
}

func TestSecuritySuiteSecretRedactionBeforePersistence(t *testing.T) {
	persister := &staticPersister{}
	pipeline := Pipeline{
		Validator:  nopValidator{},
		Authorizer: allowAllAuth{},
		Executor:   staticRawExecutor{},
		Persister:  persister,
		Policies: staticPolicyProvider{policy: ToolPolicy{
			Timeout:     time.Second,
			MaxAttempts: 1,
		}},
		Redactor: DefaultSecretRedactor{},
	}

	result, err := pipeline.Execute(context.Background(), ToolRequest{
		RunID:      "run-sec",
		ToolCallID: "call-1",
		ToolName:   "dangerous",
	})
	require.NoError(t, err)
	require.Equal(t, "[REDACTED]", result.Output["password"])
	require.Equal(t, "[REDACTED]", result.Output["token"])
	require.Equal(t, "ok", result.Output["safe_field"])
	require.Equal(t, result.Output, persister.saved)
}

func TestSecuritySuitePolicyBypassRegressionDeniedByDefault(t *testing.T) {
	audit := &recordAuditSink{}
	authorizer := TierAuthorizer{Policy: nil, Audit: audit}

	err := authorizer.Authorize(context.Background(), ToolRequest{Tier: TierEnterprise, ToolName: "admin-tool"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAuthorization)
	require.Len(t, audit.records, 1)
	require.False(t, audit.records[0].Allowed)
}

func TestSecuritySuitePolicyBypassRegressionExplicitAllowOnly(t *testing.T) {
	audit := &recordAuditSink{}
	authorizer := TierAuthorizer{Policy: allowPolicy{}, Audit: audit}

	err := authorizer.Authorize(context.Background(), ToolRequest{Tier: TierEnterprise, ToolName: "admin-tool"})
	require.NoError(t, err)

	err = authorizer.Authorize(context.Background(), ToolRequest{Tier: TierPro, ToolName: "admin-tool"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAuthorization)
	require.Len(t, audit.records, 2)
	require.True(t, audit.records[0].Allowed)
	require.False(t, audit.records[1].Allowed)
}

func TestSecuritySuiteAuthorizationErrorDoesNotGetMasked(t *testing.T) {
	authorizer := TierAuthorizer{Policy: denyAllPolicy{}}
	err := authorizer.Authorize(context.Background(), ToolRequest{Tier: TierFree, ToolName: "payments"})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAuthorization))
}
