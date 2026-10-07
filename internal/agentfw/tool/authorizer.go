package tool

import (
	"context"
	"fmt"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// TenantAuthorizer answers the tool-level question with the platform's own
// authorization policy.
//
// The control plane authorizes (principal, action, object) for every write it
// accepts; a tool call is one more object a run acts on, so it is authorized
// through the same port rather than through a second policy of its own. One
// policy, two enforcement points: a deployment that swaps the authorizer for
// OpenFGA or SpiceDB decides tool access with the same rules it decides
// everything else.
type TenantAuthorizer struct {
	// Authorizer is the platform's policy. Required.
	Authorizer agentoscore.Authorizer
	// Audit records every decision. Optional, but a denial nobody recorded
	// cannot be investigated after the fact.
	Audit AuditSink
}

// ErrNoAuthorizer is returned when a tenant authorizer is built without a policy.
var ErrNoAuthorizer = fmt.Errorf("%w: Authorizer", ErrPipelineAssembly)

// Authorize asks the platform policy whether this run may execute this tool.
func (a *TenantAuthorizer) Authorize(ctx context.Context, req *Request) error {
	if a.Authorizer == nil {
		return ErrNoAuthorizer
	}

	// The account arrived with the run, not with this call: the platform
	// launched the run under a verified principal, and this decision inherits
	// it (PrincipalSourceRun). The actor defaults to the account, which is
	// what EffectiveActor documents for a run without a separate actor.
	principal := agentoscore.Principal{AccountID: req.AccountID, Source: agentoscore.PrincipalSourceRun}

	tenant, err := principal.Tenant(req.ProjectID)
	if err != nil {
		if auditErr := a.record(ctx, req, err); auditErr != nil {
			return auditErr
		}

		return fmt.Errorf("%w: tool %q: %w", ErrAuthorization, req.ToolName, err)
	}

	authorizeErr := a.Authorizer.Authorize(ctx, &agentoscore.AuthorizeRequest{
		Principal: principal,
		Action:    agentoscore.ActionToolExecute,
		Object: agentoscore.ObjectRef{
			Kind:   "tool",
			ID:     req.ToolName,
			Tenant: tenant,
		},
	})

	if auditErr := a.record(ctx, req, authorizeErr); auditErr != nil {
		return auditErr
	}

	if authorizeErr != nil {
		return fmt.Errorf("%w: tool %q: %w", ErrAuthorization, req.ToolName, authorizeErr)
	}

	return nil
}

func (a *TenantAuthorizer) record(ctx context.Context, req *Request, authorizeErr error) error {
	if a.Audit == nil {
		return nil
	}

	allowed := authorizeErr == nil
	reason := "allowed"

	if authorizeErr != nil {
		reason = authorizeErr.Error()
	}

	// A decision that cannot be recorded fails the call: an audit trail with
	// holes in it cannot answer who ran what.
	if err := a.Audit.Write(ctx, &AuditRecord{
		RunID:      req.RunID,
		AccountID:  req.AccountID,
		ProjectID:  req.ProjectID,
		ToolName:   req.ToolName,
		Allowed:    allowed,
		Reason:     reason,
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("%w: record decision for tool %q: %w", ErrAuthorization, req.ToolName, err)
	}

	return nil
}
