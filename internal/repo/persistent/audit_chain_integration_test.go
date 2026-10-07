//go:build postgres_integration

package persistent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/stretchr/testify/require"
)

// An audit row that can be edited by whoever owns the database is not evidence.
// The rows are hash-chained per tenant, and these tests state what that buys:
// an intact chain verifies, and an edit — even one made with the database's own
// tools, with the append-only trigger out of the way — is found.

// auditChainFixture is a plan whose audit trail the tests write to: an audit
// row belongs to a plan, so the plan has to exist before the chain can.
type auditChainFixture struct {
	planID    string
	accountID string
	projectID string
}

func newAuditChainFixture(t *testing.T, ctx context.Context, planRepo *AgentOSPlanRepo, suffix string) auditChainFixture {
	t.Helper()

	spec := postgresIntegrationPlanSpec("chain-plan-"+suffix, "chain-start-"+suffix)
	status := agentosplan.NewState(&spec, time.Now().UTC()).Status

	if _, _, err := planRepo.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("create plan: %v", err)
	}

	return auditChainFixture{planID: spec.PlanID, accountID: spec.AccountID, projectID: spec.ProjectID}
}

func recordAuditChain(t *testing.T, ctx context.Context, planRepo *AgentOSPlanRepo, suffix string, entries int) (string, string, []agentosplan.AuditRecord) {
	t.Helper()

	fixture := newAuditChainFixture(t, ctx, planRepo, suffix)
	record := &agentosplan.AuditRecord{
		PlanID:         fixture.planID,
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "chain-request-" + suffix,
		Payload:        map[string]any{"operation": "cancel", "sequence": 1},
	}

	created := make([]agentosplan.AuditRecord, 0, entries)

	for i := range entries {
		next := *record
		next.IdempotencyKey = fmt.Sprintf("%s-%d", record.IdempotencyKey, i)
		next.Payload = map[string]any{"operation": "cancel", "sequence": i}

		stored, isNew, err := planRepo.RecordAudit(ctx, &next)
		if err != nil {
			t.Fatalf("record audit %d: %v", i, err)
		}

		if !isNew {
			t.Fatalf("audit %d was not treated as new", i)
		}

		created = append(created, stored)
	}

	return fixture.accountID, fixture.projectID, created
}

func TestAuditChainVerifiesIntactRows(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)

	planRepo := NewAgentOSPlanRepo(pg)
	accountID, projectID, _ := recordAuditChain(t, ctx, planRepo, suffix, 3)

	verification, err := planRepo.VerifyAuditChain(ctx, accountID, projectID)
	require.NoError(t, err)

	require.True(t, verification.Verified(), "chain reported broken: %+v", verification)
	require.Equal(t, 3, verification.Rows)
	require.Equal(t, 3, verification.Chained)
	require.Zero(t, verification.UnchainedPrefix)
}

// The chain is per tenant: another tenant's rows do not continue this one's.
func TestAuditChainIsScopedToItsTenant(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)

	planRepo := NewAgentOSPlanRepo(pg)
	accountID, _, _ := recordAuditChain(t, ctx, planRepo, suffix, 2)

	verification, err := planRepo.VerifyAuditChain(ctx, accountID, "some-other-project-"+suffix)
	require.NoError(t, err)

	require.Zero(t, verification.Rows)
	require.True(t, verification.Verified(), "an empty chain is not a broken one")
}

// Tampering with a row is what the chain exists to catch. The test disables the
// append-only trigger first, because that is the act the trigger exists to
// block and the chain exists to survive: a database owner can drop the trigger,
// and the rows must still be able to say they were changed.
func TestAuditChainDetectsATamperedRow(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)

	planRepo := NewAgentOSPlanRepo(pg)
	accountID, projectID, created := recordAuditChain(t, ctx, planRepo, suffix, 3)

	verification, err := planRepo.VerifyAuditChain(ctx, accountID, projectID)
	require.NoError(t, err)
	require.True(t, verification.Verified(), "the chain must start intact")
	require.Len(t, created, 3)

	tampered := created[1].AuditID

	if _, err := pg.Pool.Exec(ctx, `ALTER TABLE audit_logs DISABLE TRIGGER audit_logs_append_only`); err != nil {
		t.Fatalf("disable append-only trigger: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `
		UPDATE audit_logs SET payload_json = '{"operation":"approved"}' WHERE audit_id = $1`, tampered); err != nil {
		t.Fatalf("tamper with the row: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `ALTER TABLE audit_logs ENABLE TRIGGER audit_logs_append_only`); err != nil {
		t.Fatalf("re-enable append-only trigger: %v", err)
	}

	verification, err = planRepo.VerifyAuditChain(ctx, accountID, projectID)
	require.NoError(t, err)

	require.Equal(t, tampered, verification.BrokenAt, "the edit must be reported at the row that was edited")
	require.Equal(t, "row contents do not match its hash", verification.Reason)
	require.False(t, verification.Verified())
}

// Removing a row is equally visible: the rows after it no longer continue from
// the hash their predecessor stored.
func TestAuditChainDetectsARemovedRow(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)

	planRepo := NewAgentOSPlanRepo(pg)
	accountID, projectID, created := recordAuditChain(t, ctx, planRepo, suffix, 3)

	if _, err := pg.Pool.Exec(ctx, `ALTER TABLE audit_logs DISABLE TRIGGER audit_logs_append_only`); err != nil {
		t.Fatalf("disable append-only trigger: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `DELETE FROM audit_logs WHERE audit_id = $1`, created[1].AuditID); err != nil {
		t.Fatalf("remove the row: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `ALTER TABLE audit_logs ENABLE TRIGGER audit_logs_append_only`); err != nil {
		t.Fatalf("re-enable append-only trigger: %v", err)
	}

	verification, err := planRepo.VerifyAuditChain(ctx, accountID, projectID)
	require.NoError(t, err)

	require.Equal(t, created[2].AuditID, verification.BrokenAt)
	require.Equal(t, "row does not continue from its predecessor", verification.Reason)
}

// Rows written before the chain existed are reported, not presented as
// verified: a chain that silently covered rows it never hashed would be worse
// than an honest gap.
func TestAuditChainReportsUnchainedPrefix(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)

	planRepo := NewAgentOSPlanRepo(pg)
	accountID, projectID, created := recordAuditChain(t, ctx, planRepo, suffix, 2)

	stripAuditChain(t, ctx, pg, created[0].AuditID)

	verification, err := planRepo.VerifyAuditChain(ctx, accountID, projectID)
	require.NoError(t, err)

	// The oldest row is counted as pre-chain history, and the row after it can
	// no longer continue from it: the gap is what the walk reports.
	require.Equal(t, 1, verification.UnchainedPrefix)
	require.Equal(t, created[1].AuditID, verification.BrokenAt)
	require.Equal(t, "row does not continue from its predecessor", verification.Reason)
}

// A row that loses its hash in the middle of the chain is not pre-chain
// history: the chain had begun, and the gap is reported as a break at that row.
func TestAuditChainRefusesAGapInsideTheChain(t *testing.T) {
	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)

	planRepo := NewAgentOSPlanRepo(pg)
	accountID, projectID, created := recordAuditChain(t, ctx, planRepo, suffix, 3)

	stripAuditChain(t, ctx, pg, created[1].AuditID)

	verification, err := planRepo.VerifyAuditChain(ctx, accountID, projectID)
	require.NoError(t, err)

	require.Equal(t, created[1].AuditID, verification.BrokenAt)
	require.Equal(t, "row carries no hash after the chain began", verification.Reason)
	require.Zero(t, verification.UnchainedPrefix)
}

// stripAuditChain removes a row's chain hashes, with the append-only trigger
// out of the way: that is the edit the trigger exists to block and the chain
// exists to survive.
func stripAuditChain(t *testing.T, ctx context.Context, pg *postgres.Postgres, auditID string) {
	t.Helper()

	if _, err := pg.Pool.Exec(ctx, `ALTER TABLE audit_logs DISABLE TRIGGER audit_logs_append_only`); err != nil {
		t.Fatalf("disable append-only trigger: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `
		UPDATE audit_logs SET prev_hash = '', row_hash = '' WHERE audit_id = $1`, auditID); err != nil {
		t.Fatalf("strip the chain from %s: %v", auditID, err)
	}

	if _, err := pg.Pool.Exec(ctx, `ALTER TABLE audit_logs ENABLE TRIGGER audit_logs_append_only`); err != nil {
		t.Fatalf("re-enable append-only trigger: %v", err)
	}
}
