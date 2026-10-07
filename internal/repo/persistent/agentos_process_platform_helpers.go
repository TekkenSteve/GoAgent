package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// What the platform projections (worksets, governed actions) share once their
// SQL lives in queries/*.sql: JSON document codecs for the spec/status
// projection pattern, the protocol generics the create/update flows are
// shaped around, and small binding helpers for nullable values. The
// statements themselves are per-domain and generated.

func marshalProcessPlatformJSON(name string, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%s - marshal: %w", name, err)
	}

	return data, nil
}

func unmarshalProcessPlatformJSON[T any](name string, data []byte) (T, error) {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("%s - decode: %w", name, err)
	}

	return value, nil
}

// decodeProcessPlatformJSON is unmarshalProcessPlatformJSON under the name
// the generated-binding call sites read naturally.
func decodeProcessPlatformJSON[T any](name string, data []byte) (T, error) {
	return unmarshalProcessPlatformJSON[T](name, data)
}

// The optional* helpers are the shared binding vocabulary for absence: the
// generated binding reads SQL NULL, and NULL means "not supplied" to the
// statements in queries/*.sql — an omitted partial-update field, an absent
// filter, or LIMIT ALL.

// optionalTimestamptz binds a zero time as an absent optional timestamp.
func optionalTimestamptz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts, Valid: !ts.IsZero()}
}

// optionalInt8 binds a non-positive limit as NULL — LIMIT ALL.
func optionalInt8(value int) pgtype.Int8 {
	if value <= 0 {
		return pgtype.Int8{}
	}

	return pgtype.Int8{Int64: int64(value), Valid: true}
}

// optionalText binds a nil pointer as NULL, which is how a partial update
// omits a field.
func optionalText(value *string) pgtype.Text {
	if value == nil {
		return pgtype.Text{}
	}

	return pgtype.Text{String: *value, Valid: true}
}

// optionalBool binds a nil pointer as NULL, which is how a partial update
// omits a field.
func optionalBool(value *bool) pgtype.Bool {
	if value == nil {
		return pgtype.Bool{}
	}

	return pgtype.Bool{Bool: *value, Valid: true}
}

// optionalStrings binds a nil pointer as NULL, which is how a partial update
// omits an array field.
func optionalStrings(value *[]string) []string {
	if value == nil {
		return nil
	}

	return *value
}

// decodeRecordJSON decodes one JSON column into the destination field a
// generated-row mapper is filling. The column name is part of the error so a
// malformed document names what failed.
func decodeRecordJSON(name, column string, data []byte, target any) error {
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("%s - unmarshal %s: %w", name, column, err)
	}

	return nil
}

// getJSONRecord resolves one JSON-document lookup: a missing row is the
// caller's not-found case, and a present one decodes into the domain record.
func getJSONRecord[T any](name string, data []byte, queryErr error) (record T, exists bool, err error) {
	var zero T

	if missingRow(queryErr) {
		return zero, false, nil
	}

	if queryErr != nil {
		return zero, false, fmt.Errorf("%s - query: %w", name, queryErr)
	}

	decoded, err := decodeProcessPlatformJSON[T](name, data)
	if err != nil {
		return zero, false, err
	}

	return decoded, true, nil
}

// getRecord resolves one generated :one lookup: a missing row is the
// caller's not-found case, a query failure carries the caller's operation
// name, and a present row is mapped by the caller.
func getRecord[Row, Record any](operation string, row Row, queryErr error, mapRow func(*Row) (Record, error)) (record Record, exists bool, err error) {
	var zero Record

	if missingRow(queryErr) {
		return zero, false, nil
	}

	if queryErr != nil {
		return zero, false, fmt.Errorf("%s: %w", operation, queryErr)
	}

	mapped, err := mapRow(&row)
	if err != nil {
		return zero, false, err
	}

	return mapped, true, nil
}

// listRecords runs a generated list query and maps every row into the domain
// record: the one shape every list call site in this package follows.
func listRecords[Row, Record any](name string, rows []Row, err error, mapRow func(*Row) (Record, error)) ([]Record, error) {
	if err != nil {
		return nil, fmt.Errorf("%s - query: %w", name, err)
	}

	records := make([]Record, 0, len(rows))

	for i := range rows {
		record, err := mapRow(&rows[i])
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

// platformTenantMismatch reports a record or status whose tenant does not match
// the scope's, naming the domain's own entity and subject in the message.
func platformTenantMismatch(sentinel error, entity, subject, accountID, projectID string, scope platformScope) error {
	if accountID == scope.AccountID && projectID == scope.ProjectID {
		return nil
	}

	return fmt.Errorf("%w: %s %q %s", sentinel, entity, scope.RecordID, subject)
}

// platformScope identifies one tenant-scoped platform record: which record,
// which tenant, and which idempotency key scopes the write.
type platformScope struct {
	RecordID       string
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

// platformTable is one platform projection table's generated-SQL surface. An
// adapter implements it over that table's generated bindings and does nothing
// else; marshaling, decoding, idempotent replay, conflict resolution and
// transaction handling all live once in the protocols below, so a domain adds
// no protocol code of its own.
type platformTable[Spec, Status any] interface {
	// withTx returns the same table bound to a transaction.
	withTx(tx pgx.Tx) platformTable[Spec, Status]

	// selectRecordByKey and selectRecordByID return the stored spec and status
	// documents; pgx.ErrNoRows means the record does not exist.
	selectRecordByKey(ctx context.Context, scope platformScope) (specJSON, statusJSON []byte, queryErr error)
	selectRecordByID(ctx context.Context, scope platformScope) (specJSON, statusJSON []byte, queryErr error)

	// insertRecord stores a fresh record and returns the status document the
	// database kept.
	insertRecord(ctx context.Context, spec *Spec, status *Status, specJSON, statusJSON []byte) (storedStatusJSON []byte, queryErr error)

	// updateRecordStatus writes a status update's projection and reports how
	// many rows it touched; zero means the record is gone.
	updateRecordStatus(ctx context.Context, scope platformScope, status *Status, statusJSON []byte) (updated int64, queryErr error)

	// claimStatusKey inserts the write's idempotency key and returns the status
	// document it stored; pgx.ErrNoRows means the key was already claimed.
	claimStatusKey(ctx context.Context, scope platformScope, statusJSON []byte) (claimedJSON []byte, queryErr error)

	// readStatusKey reads the status document a prior write stored under the
	// scope's key.
	readStatusKey(ctx context.Context, scope platformScope) (storedJSON []byte, queryErr error)

	// The remaining methods are the domain's own decisions, kept out of the
	// protocols so the shared flow carries no per-domain closures.

	// recordScope and statusScope describe the tenant and write key a record or
	// a status update is scoped by.
	recordScope(spec *Spec) platformScope
	statusScope(status *Status, idempotencyKey string) platformScope

	// normalizeStatus folds the record's identity into an incoming status.
	normalizeStatus(spec *Spec, status *Status) *Status

	// notFound, alreadyExists and the two tenant mismatches report the domain's
	// own errors on the protocols' failure branches.
	notFound(scope platformScope) error
	alreadyExists(scope platformScope) error
	tenantMismatch(spec *Spec, scope platformScope) error
	statusTenantMismatch(spec *Spec, scope platformScope) error
}

// createPlatformRecord is the create-idempotency protocol every platform
// projection follows: an existing idempotency-key row is a replay validated
// against the incoming spec, an existing record ID is validated the same way,
// and only a fresh key and ID reach the insert. A unique-violation race is
// resolved by re-reading the key, which is why the insert is not the answer.
func createPlatformRecord[Spec, Status any](
	ctx context.Context,
	table platformTable[Spec, Status],
	name string,
	spec *Spec,
	status *Status,
	validateStart func(existing, incoming *Spec) error,
) (createdStatus Status, created bool, err error) {
	scope := table.recordScope(spec)
	status = table.normalizeStatus(spec, status)

	replayedStatus, replayed, err := platformReplay(ctx, table, name, spec, scope, validateStart)
	if err != nil || replayed {
		return replayedStatus, false, err
	}

	specJSON, err := marshalProcessPlatformJSON(name+" spec", spec)
	if err != nil {
		var zero Status

		return zero, false, err
	}

	statusJSON, err := marshalProcessPlatformJSON(name+" status", status)
	if err != nil {
		var zero Status

		return zero, false, err
	}

	storedStatusJSON, err := table.insertRecord(ctx, spec, status, specJSON, statusJSON)
	if err != nil {
		replayedStatus, err := resolvePlatformInsertConflict(ctx, table, name, scope, err)

		return replayedStatus, false, err
	}

	createdStatus, err = decodeProcessPlatformJSON[Status](name, storedStatusJSON)
	if err != nil {
		var zero Status

		return zero, false, err
	}

	return createdStatus, true, nil
}

// selectPlatformRecord decodes one stored spec/status pair through a table
// lookup, turning the missing row into exists-false.
func selectPlatformRecord[Spec, Status any](
	ctx context.Context,
	name string,
	lookup func(context.Context, platformScope) ([]byte, []byte, error),
	scope platformScope,
) (spec Spec, status Status, exists bool, err error) {
	specJSON, statusJSON, err := lookup(ctx, scope)
	if missingRow(err) {
		var zeroSpec Spec

		var zeroStatus Status

		return zeroSpec, zeroStatus, false, nil
	}

	if err != nil {
		var zeroSpec Spec

		var zeroStatus Status

		return zeroSpec, zeroStatus, false, fmt.Errorf("%s - query: %w", name, err)
	}

	spec, err = decodeProcessPlatformJSON[Spec](name+" spec", specJSON)
	if err != nil {
		var zeroSpec Spec

		var zeroStatus Status

		return zeroSpec, zeroStatus, false, err
	}

	status, err = decodeProcessPlatformJSON[Status](name+" status", statusJSON)
	if err != nil {
		var zeroSpec Spec

		var zeroStatus Status

		return zeroSpec, zeroStatus, false, err
	}

	return spec, status, true, nil
}

// platformReplay answers the create protocol's two identity lookups: the
// idempotency key first, because a hit is a replay, then the record ID, because
// a hit under a fresh key is an identity conflict. Both hits validate the same
// way and neither counts as newly created.
func platformReplay[Spec, Status any](
	ctx context.Context,
	table platformTable[Spec, Status],
	name string,
	spec *Spec,
	scope platformScope,
	validateStart func(existing, incoming *Spec) error,
) (replayedStatus Status, replayed bool, err error) {
	lookups := []func(context.Context, platformScope) ([]byte, []byte, error){
		table.selectRecordByKey,
		table.selectRecordByID,
	}

	for _, lookup := range lookups {
		existingSpec, existingStatus, exists, err := selectPlatformRecord[Spec, Status](ctx, name, lookup, scope)
		if err != nil {
			var zero Status

			return zero, false, err
		}

		if !exists {
			continue
		}

		return existingStatus, true, validateStart(&existingSpec, spec)
	}

	var zero Status

	return zero, false, nil
}

// resolvePlatformInsertConflict maps a failed insert to the replay answer: only
// a unique violation with a key that still resolves to a row is a replay, and
// anything else is the insert failure or the caller's own identity conflict.
func resolvePlatformInsertConflict[Spec, Status any](
	ctx context.Context,
	table platformTable[Spec, Status],
	name string,
	scope platformScope,
	insertErr error,
) (existingStatus Status, err error) {
	var zero Status

	if !isPostgresUniqueViolation(insertErr) {
		return zero, fmt.Errorf("%s - insert: %w", name, insertErr)
	}

	storedJSON, err := table.readStatusKey(ctx, scope)
	if missingRow(err) {
		return zero, table.alreadyExists(scope)
	}

	if err != nil {
		return zero, fmt.Errorf("%s - existing key: %w", name, err)
	}

	existing, err := decodeProcessPlatformJSON[Status](name, storedJSON)
	if err != nil {
		return zero, err
	}

	return existing, nil
}

// getPlatformRecord is the read-every platform projection follows: a missing
// record is exists-false, and a present one is checked against the caller's
// tenant before it is handed back.
func getPlatformRecord[Spec, Status any](
	ctx context.Context,
	table platformTable[Spec, Status],
	name string,
	scope platformScope,
) (spec Spec, status Status, exists bool, err error) {
	spec, status, exists, err = selectPlatformRecord[Spec, Status](ctx, name, table.selectRecordByID, scope)
	if err != nil || !exists {
		return spec, status, exists, err
	}

	if err := table.tenantMismatch(&spec, scope); err != nil {
		var zeroSpec Spec

		var zeroStatus Status

		return zeroSpec, zeroStatus, false, err
	}

	return spec, status, true, nil
}

// updatePlatformStatus is the status-update entry point every platform
// projection follows: a replay of the write key answers with the stored status,
// a missing record is the domain's not-found error, and a fresh write is
// normalized against the stored spec and applied under the claim transaction.
func updatePlatformStatus[Spec, Status any](
	ctx context.Context,
	pool *postgres.Postgres,
	table platformTable[Spec, Status],
	name string,
	status *Status,
	idempotencyKey string,
) (finalStatus Status, err error) {
	scope := table.statusScope(status, idempotencyKey)

	existing, exists, err := storedPlatformStatus[Spec, Status](ctx, table, name+" key", scope)
	if err != nil {
		var zero Status

		return zero, err
	}

	if exists {
		return existing, nil
	}

	spec, _, exists, err := selectPlatformRecord[Spec, Status](ctx, name, table.selectRecordByID, platformScope{RecordID: scope.RecordID})
	if err != nil {
		var zero Status

		return zero, err
	}

	if !exists {
		var zero Status

		return zero, table.notFound(scope)
	}

	if err := table.statusTenantMismatch(&spec, scope); err != nil {
		var zero Status

		return zero, err
	}

	return applyPlatformStatus(ctx, pool, name, table, scope, table.normalizeStatus(&spec, status))
}

// applyPlatformClaim is the claim-then-project transaction in its general form:
// the claim is whatever the write's key needs, and the projection write is the
// caller's own. Everything happens under one transaction, and a claim that
// returns no row is a replay whose stored answer wins.
func applyPlatformClaim[Status any](
	ctx context.Context,
	pool *postgres.Postgres,
	name string,
	claim func(ctx context.Context, tx pgx.Tx) (bool, Status, error),
	update func(ctx context.Context, tx pgx.Tx) error,
) (finalStatus Status, err error) {
	tx, err := pool.Pool.Begin(ctx)
	if err != nil {
		var zero Status

		return zero, fmt.Errorf("%s - begin: %w", name, err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	claimed, status, err := claim(ctx, tx)
	if err != nil {
		var zero Status

		return zero, err
	}

	if !claimed {
		return status, nil
	}

	if err := update(ctx, tx); err != nil {
		var zero Status

		return zero, err
	}

	if err := tx.Commit(ctx); err != nil {
		var zero Status

		return zero, fmt.Errorf("%s - commit: %w", name, err)
	}

	return status, nil
}

// applyPlatformStatus is the status-update protocol every platform projection
// runs: claim the idempotency key and write the projection under one
// transaction, so a replay can never advance the record.
func applyPlatformStatus[Spec, Status any](
	ctx context.Context,
	pool *postgres.Postgres,
	name string,
	table platformTable[Spec, Status],
	scope platformScope,
	status *Status,
) (finalStatus Status, err error) {
	statusJSON, err := marshalProcessPlatformJSON(name+" status", status)
	if err != nil {
		var zero Status

		return zero, err
	}

	return applyPlatformClaim(ctx, pool, name,
		func(ctx context.Context, tx pgx.Tx) (bool, Status, error) {
			return claimPlatformStatus(ctx, table.withTx(tx), name, scope, statusJSON)
		},
		func(ctx context.Context, tx pgx.Tx) error {
			updated, err := table.withTx(tx).updateRecordStatus(ctx, scope, status, statusJSON)
			if err != nil {
				return err
			}

			if updated == 0 {
				return table.notFound(scope)
			}

			return nil
		},
	)
}

// claimPlatformStatus is the claim-or-read half of a status write: a claimed
// key returns the document the claim stored, and an already-claimed key returns
// the document the previous write stored, read through the same transaction.
func claimPlatformStatus[Spec, Status any](
	ctx context.Context,
	table platformTable[Spec, Status],
	name string,
	scope platformScope,
	statusJSON []byte,
) (claimed bool, claimedStatus Status, err error) {
	return claimPlatformDocument[Status](ctx, name,
		func(ctx context.Context) ([]byte, error) {
			return table.claimStatusKey(ctx, scope, statusJSON)
		},
		func(ctx context.Context) ([]byte, error) {
			return table.readStatusKey(ctx, scope)
		},
	)
}

// claimPlatformDocument is the claim-or-read protocol for writes whose key
// carries more than a status document: a claimed key returns the document the
// claim stored, and an already-claimed key returns the document a previous
// write stored, read through the same transaction.
func claimPlatformDocument[Status any](
	ctx context.Context,
	name string,
	claim func(context.Context) ([]byte, error),
	readExisting func(context.Context) ([]byte, error),
) (claimed bool, claimedStatus Status, err error) {
	claimedJSON, err := claim(ctx)
	if missingRow(err) {
		storedJSON, err := readExisting(ctx)
		if err != nil {
			var zero Status

			return false, zero, fmt.Errorf("%s - existing key: %w", name, err)
		}

		existing, err := decodeProcessPlatformJSON[Status](name, storedJSON)
		if err != nil {
			var zero Status

			return false, zero, err
		}

		return false, existing, nil
	}

	if err != nil {
		var zero Status

		return false, zero, fmt.Errorf("%s - claim: %w", name, err)
	}

	claimedDocument, err := decodeProcessPlatformJSON[Status](name, claimedJSON)
	if err != nil {
		var zero Status

		return false, zero, err
	}

	return true, claimedDocument, nil
}

// storedPlatformStatus reads the status a prior idempotent write stored under a
// scope: an absent key is exists-false, a present one decodes through the
// domain type.
func storedPlatformStatus[Spec, Status any](
	ctx context.Context,
	table platformTable[Spec, Status],
	name string,
	scope platformScope,
) (storedStatus Status, exists bool, err error) {
	storedJSON, err := table.readStatusKey(ctx, scope)

	return getJSONRecord[Status](name, storedJSON, err)
}

// missingRow reports whether a generated :one query came back empty — the
// exists-false half of the (record, exists, err) contract.
func missingRow(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
