package persistent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

// Audit rows are chained so that an edit is visible. The application computes
// the hashes — see the migration's comment for why SQL cannot — and this file is
// the single definition of the canonical form, shared by the writer and the
// verifier. A verifier that encoded a row differently from the writer would
// report every row as tampered, which is why both call auditRowHash.

// auditChainRow is one audit row in canonical form.
type auditChainRow struct {
	AuditID        string
	PlanID         string
	AccountID      string
	ProjectID      string
	RunID          string
	NodeID         string
	ActorID        string
	Action         string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}

// auditRowHash computes a row's chain hash from its predecessor's hash.
//
// Fields are length-prefixed before hashing: concatenating them with a
// separator would let a crafted value shift a boundary and produce the same
// hash for different rows, which is exactly the property the chain must not
// have.
func auditRowHash(prevHash string, row *auditChainRow) string {
	digest := sha256.New()
	writeField(digest, prevHash)

	for _, field := range []string{
		row.AuditID, row.PlanID, row.AccountID, row.ProjectID, row.RunID, row.NodeID,
		row.ActorID, row.Action, row.IdempotencyKey,
	} {
		writeField(digest, field)
	}

	writeField(digest, string(row.Payload))
	writeField(digest, auditChainTime(row.CreatedAt))

	return hex.EncodeToString(digest.Sum(nil))
}

// auditChainTime is the instant a row hashes over: UTC, truncated to the
// microsecond precision PostgreSQL stores. Hashing the nanosecond value a Go
// caller passed would make every row look tampered the moment it came back
// from the database, because the read value is the truncated one.
func auditChainTime(createdAt time.Time) string {
	return strconv.FormatInt(createdAt.UTC().Truncate(time.Microsecond).UnixMicro(), 10)
}

// canonicalAuditPayload puts a payload into the form both the writer and the
// verifier hash.
//
// The stored form cannot be hashed directly: jsonb reorders keys and reprints
// whitespace, so the bytes read back differ from the bytes written and every
// row would look tampered. The canonical form is Go's own encoding of the
// decoded value — map keys sorted, no spaces — and numbers are carried as
// json.Number so a large integer keeps its digits instead of being rounded
// through a float.
func canonicalAuditPayload(payload map[string]any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("audit chain - encode payload: %w", err)
	}

	return canonicalizeAuditJSON(raw)
}

func canonicalizeAuditJSON(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var decoded any

	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("audit chain - decode payload: %w", err)
	}

	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf("audit chain - canonicalize payload: %w", err)
	}

	return canonical, nil
}

func writeField(digest hash.Hash, value string) {
	digest.Write([]byte(strconv.Itoa(len(value))))
	digest.Write([]byte{':'})
	digest.Write([]byte(value))
}

// auditChainLockKey is the advisory lock key for a tenant's chain. It is
// derived in Go rather than in SQL so the key is stable across PostgreSQL
// versions and identical for the writer and any maintenance path.
func auditChainLockKey(accountID, projectID string) int64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(accountID))
	hasher.Write([]byte{0})
	hasher.Write([]byte(projectID))

	// The key is a hash, not an arithmetic value: reinterpreting its bits as a
	// signed integer is the whole point, and any 64-bit value names a lock.
	return int64(hasher.Sum64()) //nolint:gosec // hash bits, not a value
}

// insertAuditRecordChained appends one audit row to its tenant's chain.
//
// The whole operation runs in one transaction that first takes the tenant's
// advisory lock: reading the predecessor's hash and writing the successor has
// to be atomic, or two concurrent rows would both chain to the same
// predecessor and the verifier would report a break that no one caused.
func (r *AgentOSPlanRepo) insertAuditRecordChained(ctx context.Context, record *agentosplan.AuditRecord, payloadJSON []byte) error {
	return r.runInTx(ctx, func(tx pgx.Tx) error {
		queries := r.queries.WithTx(tx)

		// The row stores the instant it hashed over, not the caller's
		// nanosecond value the database would truncate.
		createdAt := record.CreatedAt.UTC().Truncate(time.Microsecond)

		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, auditChainLockKey(record.AccountID, record.ProjectID)); err != nil {
			return fmt.Errorf("audit chain - lock tenant chain: %w", err)
		}

		prevHash, err := queries.LatestAuditRowHash(ctx, sqlcgen.LatestAuditRowHashParams{
			AccountID: record.AccountID,
			ProjectID: record.ProjectID,
		})
		if err != nil {
			return fmt.Errorf("audit chain - read predecessor: %w", err)
		}

		rowHash := auditRowHash(prevHash, &auditChainRow{
			AuditID:        record.AuditID,
			PlanID:         record.PlanID,
			AccountID:      record.AccountID,
			ProjectID:      record.ProjectID,
			RunID:          record.RunID,
			NodeID:         record.NodeID,
			ActorID:        record.ActorID,
			Action:         string(record.Action),
			IdempotencyKey: record.IdempotencyKey,
			Payload:        payloadJSON,
			CreatedAt:      createdAt,
		})

		return queries.InsertAuditRecord(ctx, sqlcgen.InsertAuditRecordParams{
			AuditID:        record.AuditID,
			PlanID:         record.PlanID,
			AccountID:      record.AccountID,
			ProjectID:      record.ProjectID,
			RunID:          nullableText(record.RunID),
			NodeID:         nullableText(record.NodeID),
			ActorID:        record.ActorID,
			Action:         string(record.Action),
			IdempotencyKey: record.IdempotencyKey,
			PayloadJson:    payloadJSON,
			CreatedAt:      createdAt,
			PrevHash:       prevHash,
			RowHash:        rowHash,
		})
	})
}

// AuditChainVerification is the result of verifying one tenant's audit chain.
type AuditChainVerification struct {
	AccountID string
	ProjectID string
	// Rows is how many audit rows the tenant has.
	Rows int
	// Chained is how many of them carry a hash.
	Chained int
	// UnchainedPrefix counts rows written before the chain existed. They are
	// reported rather than presented as verified.
	UnchainedPrefix int
	// BrokenAt names the first row whose hash does not match its contents or
	// its predecessor. Empty when the chain verifies.
	BrokenAt string
	// Reason explains the break in one line.
	Reason string
}

// Verified reports whether the chain proved intact.
func (v *AuditChainVerification) Verified() bool {
	return v.BrokenAt == "" && v.UnchainedPrefix == 0
}

// VerifyAuditChain walks a tenant's audit rows in chain order and recomputes
// every hash. The first row that does not match stops the walk: after a break,
// every later row is untrustworthy by construction, and reporting them all
// would bury the one that matters.
func (r *AgentOSPlanRepo) VerifyAuditChain(ctx context.Context, accountID, projectID string) (AuditChainVerification, error) {
	rows, err := r.queries.ListAuditChain(ctx, sqlcgen.ListAuditChainParams{
		AccountID: accountID,
		ProjectID: projectID,
	})
	if err != nil {
		return AuditChainVerification{}, fmt.Errorf("audit chain - list rows: %w", err)
	}

	verification := AuditChainVerification{AccountID: accountID, ProjectID: projectID, Rows: len(rows)}

	walkAuditChain(&rows, &verification)

	return verification, nil
}

// walkAuditChain verifies rows in chain order, stopping at the first break.
func walkAuditChain(rows *[]sqlcgen.ListAuditChainRow, verification *AuditChainVerification) {
	prevHash := ""

	for i := range *rows {
		row := &(*rows)[i]

		if row.RowHash == "" {
			if verification.Chained > 0 {
				verification.BrokenAt = row.AuditID
				verification.Reason = "row carries no hash after the chain began"

				return
			}

			// Pre-chain history: counted, not verified.
			verification.UnchainedPrefix++

			continue
		}

		verification.Chained++

		if row.PrevHash != prevHash {
			verification.BrokenAt = row.AuditID
			verification.Reason = "row does not continue from its predecessor"

			return
		}

		canonicalPayload, err := canonicalizeAuditJSON(row.PayloadJson)
		if err != nil {
			verification.BrokenAt = row.AuditID
			verification.Reason = "row payload cannot be read back"

			return
		}

		expected := auditRowHash(prevHash, &auditChainRow{
			AuditID:        row.AuditID,
			PlanID:         row.PlanID,
			AccountID:      row.AccountID,
			ProjectID:      row.ProjectID,
			RunID:          row.RunID,
			NodeID:         row.NodeID,
			ActorID:        row.ActorID,
			Action:         row.Action,
			IdempotencyKey: row.IdempotencyKey,
			Payload:        canonicalPayload,
			CreatedAt:      row.CreatedAt,
		})

		if expected != row.RowHash {
			verification.BrokenAt = row.AuditID
			verification.Reason = "row contents do not match its hash"

			return
		}

		prevHash = row.RowHash
	}
}

// runInTx runs fn in one transaction, rolling back on any error.
func (r *AgentOSPlanRepo) runInTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("audit chain - begin transaction: %w", err)
	}

	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("audit chain - rollback: %w", rollbackErr))
		}

		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("audit chain - commit: %w", err)
	}

	return nil
}
