package tool

import (
	"context"
	"fmt"
)

// TierPolicy resolves whether a tool is allowed for a tier.
type TierPolicy interface {
	IsAllowed(tier Tier, toolName string) bool
}

// AuditRecord captures policy decisions for auditing.
type AuditRecord struct {
	RunID      string
	AccountID  string
	ProjectID  string
	ToolName   string
	Allowed    bool
	Reason     string
	OccurredAt string
}

// AuditSink writes audit records.
type AuditSink interface {
	Write(ctx context.Context, record AuditRecord) error
}

// TierAuthorizer enforces subscription-tier access and writes audit logs.
type TierAuthorizer struct {
	Policy TierPolicy
	Audit  AuditSink
}

// Authorize checks whether a tool can execute for a given tier.
func (a TierAuthorizer) Authorize(ctx context.Context, req *Request) error {
	allowed := a.Policy != nil && a.Policy.IsAllowed(req.Tier, req.ToolName)

	reason := "allowed"
	if !allowed {
		reason = fmt.Sprintf("tier %q is not allowed to execute tool %q", req.Tier, req.ToolName)
	}

	if a.Audit != nil {
		if err := a.Audit.Write(ctx, AuditRecord{
			RunID:      req.RunID,
			AccountID:  req.AccountID,
			ProjectID:  req.ProjectID,
			ToolName:   req.ToolName,
			Allowed:    allowed,
			Reason:     reason,
			OccurredAt: "policy-evaluated",
		}); err != nil {
			return fmt.Errorf("audit write failed: %w", err)
		}
	}

	if !allowed {
		return fmt.Errorf("%w: %s", ErrAuthorization, reason)
	}

	return nil
}

// StaticTierPolicy is an in-memory tier-to-tools policy adapter.
type StaticTierPolicy struct {
	Allow map[Tier]map[string]struct{}
}

// IsAllowed returns true when tool access is granted for a tier.
func (p StaticTierPolicy) IsAllowed(tier Tier, toolName string) bool {
	tools, ok := p.Allow[tier]
	if !ok {
		return false
	}

	_, ok = tools[toolName]

	return ok
}
