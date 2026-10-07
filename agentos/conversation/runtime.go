// Package conversation implements a durable Postgres-backed conversation
// runtime for AgentOS agents. Live delivery to subscribers is pure Postgres
// polling; there is no separate streaming transport.
package conversation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInvalidConversation reports a malformed conversation request or configuration.
	ErrInvalidConversation = errors.New("agentos conversation: invalid request")
	// ErrThreadNotFound reports an unknown conversation thread.
	ErrThreadNotFound = errors.New("agentos conversation: thread not found")
	// ErrRunNotFound reports an unknown conversation run.
	ErrRunNotFound = errors.New("agentos conversation: run not found")
	// ErrTenantMismatch reports that the conversation belongs to a different tenant.
	ErrTenantMismatch = errors.New("agentos conversation: tenant mismatch")
	// ErrRunAlreadyActive reports that a run is already active on the thread.
	ErrRunAlreadyActive = errors.New("agentos conversation: a run is already active")
	// ErrInterruptRequired reports that an unresolved interrupt must be resumed before continuing.
	ErrInterruptRequired = errors.New("agentos conversation: unresolved interrupt requires resume")
	// ErrInterruptMismatch reports that the interrupt does not match the active one.
	ErrInterruptMismatch = errors.New("agentos conversation: interrupt does not match")
	// ErrOutOfOrderSourceEvent reports that a source event arrived out of order.
	ErrOutOfOrderSourceEvent = errors.New("agentos conversation: source event is out of order")
	// ErrInvalidTransition reports an invalid lifecycle transition for the current state.
	ErrInvalidTransition = errors.New("agentos conversation: invalid lifecycle transition")
)

// Config holds Postgres connection settings plus the polling cadence for the durable conversation runtime.
type Config struct {
	PostgresURL  string
	MaxConns     int32
	PollInterval time.Duration
	// EventOutbox records every persisted event in the conversation event
	// outbox, in the same transaction as the event itself, so a backbone
	// drainer can publish it to the event log afterwards.
	//
	// It stays opt-in because the outbox is a queue with exactly one reader: a
	// deployment that never starts the drainer would accumulate rows nothing
	// consumes. Turn it on together with the drainer, never before.
	EventOutbox bool

	// OnSerializationRetry observes a replayed transaction: which attempt it
	// was, and the serialization failure that caused the replay. Replays are
	// normal under concurrency — they are how a serializable runtime keeps its
	// promises — but a rate that climbs is the first sign of contention, so a
	// deployment counts them. Optional; nil observes nothing.
	OnSerializationRetry func(attempt int, err error)

	// RunLease is how long a running run may go without producing a fact
	// before the sweeper abandons it. Zero takes the default; it exists as a
	// setting because "long enough" depends on the deployment's slowest tool.
	RunLease time.Duration
	// LeaseSweepInterval is how often the sweeper looks for expired runs and
	// parked projections. Zero takes the default.
	LeaseSweepInterval time.Duration

	// OnSweepError observes a sweep that could not run. The sweeper retries on
	// its next tick, so this is for the operator's log rather than for control
	// flow. Optional; nil observes nothing.
	OnSweepError func(err error)
}

// Runtime is the durable Postgres-backed conversation runtime. Subscribers
// catch up by polling the durable event stream; there is no live transport.
type Runtime struct {
	pool                 *pgxpool.Pool
	pollInterval         time.Duration
	eventOutbox          bool
	onSerializationRetry func(attempt int, err error)
	runLease             time.Duration
	leaseSweepInterval   time.Duration
	onSweepError         func(err error)
	closeCtx             context.Context
	closeCancel          context.CancelFunc
	workers              sync.WaitGroup
	closeOnce            sync.Once
}

const (
	defaultPollInterval       = 5 * time.Second
	conversationEventBatch    = 256
	conversationEventBuffer   = 256
	conversationSubscriberBuf = 64
)

// NewRuntime connects Postgres and returns a conversation Runtime.
func NewRuntime(ctx context.Context, config Config) (agentos.ConversationRuntime, error) {
	return newRuntime(ctx, config)
}

// newRuntime builds the concrete runtime. It is kept separate from
// NewRuntime's interface return because gosec G118 cannot trace the cancel
// call in Close through an interface return; returning *Runtime directly lets
// the cancel-propagation analysis see it.
func newRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if strings.TrimSpace(config.PostgresURL) == "" {
		return nil, fmt.Errorf("%w: postgres URL is required", ErrInvalidConversation)
	}

	poolConfig, err := pgxpool.ParseConfig(config.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: parse postgres URL: %w", err)
	}

	if config.MaxConns > 0 {
		poolConfig.MaxConns = config.MaxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: connect postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("agentos conversation: ping postgres: %w", err)
	}

	pollInterval := config.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	runLease := config.RunLease
	if runLease <= 0 {
		runLease = defaultRunLease
	}

	sweepInterval := config.LeaseSweepInterval
	if sweepInterval <= 0 {
		sweepInterval = defaultLeaseSweepInterval
	}

	r := &Runtime{
		pool:                 pool,
		pollInterval:         pollInterval,
		eventOutbox:          config.EventOutbox,
		onSerializationRetry: config.OnSerializationRetry,
		runLease:             runLease,
		leaseSweepInterval:   sweepInterval,
		onSweepError:         config.OnSweepError,
	}
	r.closeCtx, r.closeCancel = context.WithCancel(context.Background())
	r.startLeaseSweeper()

	return r, nil
}

// startLeaseSweeper runs the loop that abandons dead runs and starts the turns
// that were waiting for them. It owns a goroutine for the runtime's lifetime,
// and stops with the runtime's context.
func (r *Runtime) startLeaseSweeper() {
	r.workers.Add(1)

	go func() {
		defer r.workers.Done()

		ticker := time.NewTicker(r.leaseSweepInterval)
		defer ticker.Stop()

		for {
			select {
			case <-r.closeCtx.Done():
				return
			case <-ticker.C:
				// A sweep failure is logged, not fatal: the next tick retries,
				// and a runtime that stopped serving conversations because one
				// sweep could not run would be worse than the condition it
				// guards against.
				_, _, sweepErr := r.sweepOnce(r.closeCtx)
				if sweepErr != nil && r.closeCtx.Err() == nil && r.onSweepError != nil {
					r.onSweepError(sweepErr)
				}
			}
		}
	}()
}

// Close cancels background workers and closes the Postgres pool. Close is
// idempotent: the once-guard drains workers and pool exactly once. The drain
// lives in a separate method so the receiver is not captured by a closure —
// gosec G118 can then trace the cancel call and see the context is not leaked.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}

	r.closeOnce.Do(r.shutdown)

	return nil
}

// shutdown is the once-guarded body of Close: it cancels background workers,
// waits for them to finish, then closes the Postgres pool.
func (r *Runtime) shutdown() {
	if r.closeCancel != nil {
		r.closeCancel()
	}

	r.workers.Wait()

	if r.pool != nil {
		r.pool.Close()
	}
}

// StartRun atomically persists a new conversation run with its initial user message and stream events.
func (r *Runtime) StartRun(ctx context.Context, spec *agentos.StartConversationRunSpec) (agentos.ConversationRun, error) {
	prepared, err := prepareStartSpec(spec)
	if err != nil {
		return agentos.ConversationRun{}, err
	}

	return persistConversationChange(ctx, r, func(tx pgx.Tx) (agentos.ConversationRun, error) {
		return r.persistStartRun(ctx, tx, &prepared)
	})
}

func (r *Runtime) persistStartRun(ctx context.Context, tx pgx.Tx, prepared *agentos.StartConversationRunSpec) (agentos.ConversationRun, error) {
	existing, found, err := prepareNewRun(ctx, tx, prepared)
	if err != nil || found {
		return existing, err
	}

	inserted, err := insertConversationRun(ctx, tx, prepared)
	if err != nil {
		return agentos.ConversationRun{}, err
	}

	if !inserted {
		// A concurrent admission under the same idempotency key won the insert;
		// the run it created is this request's run.
		existing, found, err := getRunByIdempotency(ctx, tx, prepared.ThreadID, prepared.IdempotencyKey)
		if err != nil {
			return agentos.ConversationRun{}, err
		}

		if !found {
			return agentos.ConversationRun{}, fmt.Errorf("%w: run %q is neither inserted nor readable", ErrInvalidConversation, prepared.RunID)
		}

		return existing, nil
	}

	if err := insertUserMessage(ctx, tx, prepared); err != nil {
		return agentos.ConversationRun{}, err
	}

	if err := r.appendUserMessageEvents(ctx, tx, prepared); err != nil {
		return agentos.ConversationRun{}, err
	}

	return agentos.ConversationRun{
		RunID: prepared.RunID, ThreadID: prepared.ThreadID, ProcessID: prepared.ProcessID,
		AccountID: prepared.AccountID, ProjectID: prepared.ProjectID,
		Status: agentos.ConversationRunPending, CreatedAt: prepared.RequestedAt,
	}, nil
}

func prepareNewRun(ctx context.Context, tx pgx.Tx, spec *agentos.StartConversationRunSpec) (agentos.ConversationRun, bool, error) {
	if err := ensureThread(ctx, tx, spec.ThreadID, spec.AccountID, spec.ProjectID, spec.RequestedAt); err != nil {
		return agentos.ConversationRun{}, false, err
	}

	existing, found, err := getRunByIdempotency(ctx, tx, spec.ThreadID, spec.IdempotencyKey)
	if err != nil || found {
		return existing, found, err
	}

	if err := validateResume(ctx, tx, spec); err != nil {
		return agentos.ConversationRun{}, false, err
	}

	return agentos.ConversationRun{}, false, nil
}

// insertConversationRun admits a run as pending and reports whether this call
// created it. The idempotency key is the admission identity: a concurrent
// duplicate yields false instead of an error so the caller can return the run
// that request already produced. A run already streaming is not a conflict
// here — idx_agentos_conversation_runs_running guards RUN_STARTED instead.
func insertConversationRun(ctx context.Context, tx pgx.Tx, spec *agentos.StartConversationRunSpec) (bool, error) {
	metadata, err := marshalJSON(spec.RunMetadata)
	if err != nil {
		return false, fmt.Errorf("%w: run metadata: %w", ErrInvalidConversation, err)
	}

	resumeID := ""
	if spec.Resume != nil {
		resumeID = spec.Resume.InterruptID
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO agentos_conversation_runs (
			run_id, thread_id, process_id, account_id, project_id, status,
			idempotency_key, resume_interrupt_id, metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (thread_id, idempotency_key) DO NOTHING`,
		spec.RunID, spec.ThreadID, spec.ProcessID, spec.AccountID, spec.ProjectID,
		agentos.ConversationRunPending, spec.IdempotencyKey, resumeID, metadata, spec.RequestedAt)
	if err != nil {
		return false, fmt.Errorf("agentos conversation: insert run: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

func insertUserMessage(ctx context.Context, tx pgx.Tx, spec *agentos.StartConversationRunSpec) error {
	attachments, err := marshalJSON(spec.Attachments)
	if err != nil {
		return fmt.Errorf("%w: attachments: %w", ErrInvalidConversation, err)
	}

	metadata, err := marshalJSON(spec.MessageMetadata)
	if err != nil {
		return fmt.Errorf("%w: message metadata: %w", ErrInvalidConversation, err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agentos_messages (
			message_id, thread_id, run_id, process_id, role, content, status,
			attachments, metadata, created_at, completed_at
		) VALUES ($1, $2, $3, $4, 'user', $5, 'completed', $6, $7, $8, $8)`,
		spec.MessageID, spec.ThreadID, spec.RunID, spec.ProcessID,
		spec.UserMessage, attachments, metadata, spec.RequestedAt)
	if err != nil {
		return fmt.Errorf("agentos conversation: insert user message: %w", err)
	}

	return nil
}

func (r *Runtime) appendUserMessageEvents(ctx context.Context, tx pgx.Tx, spec *agentos.StartConversationRunSpec) error {
	events := []struct {
		typeName core.EventType
		payload  map[string]any
	}{
		{agentos.ConversationEventTextMessageStart, map[string]any{"message_id": spec.MessageID, "role": "user"}},
		{agentos.ConversationEventTextMessageContent, map[string]any{"message_id": spec.MessageID, "delta": spec.UserMessage}},
		{agentos.ConversationEventTextMessageEnd, map[string]any{"message_id": spec.MessageID, "role": "user", "content": spec.UserMessage}},
	}
	for _, event := range events {
		if _, err := r.appendEvent(ctx, tx, spec.ThreadID, spec.RunID, spec.ProcessID, "", 0, event.typeName, spec.RequestedAt, event.payload); err != nil {
			return err
		}
	}

	return nil
}

// IngestEvent appends an externally produced event to the run's durable event stream, enforcing source ordering and idempotency.
func (r *Runtime) IngestEvent(ctx context.Context, incoming *agentos.ExternalConversationEvent) (agentos.ConversationEvent, error) {
	event, err := prepareExternalEvent(incoming)
	if err != nil {
		return agentos.ConversationEvent{}, err
	}

	return persistConversationChange(ctx, r, func(tx pgx.Tx) (agentos.ConversationEvent, error) {
		return r.persistEvent(ctx, tx, &event)
	})
}

// conversationTxRetries bounds how many times a serializable transaction is
// replayed after Postgres aborts it. Serializable isolation is what lets the
// runtime promise ordered, once-only ingestion under concurrent writers, and a
// serialization failure is that promise being kept: the transaction is rolled
// back whole, so replaying it cannot duplicate anything, and the durable
// guards (source event id, run sequence) make the replay produce the same
// state. Surfacing the abort instead would hand a caller a 500 for a
// concurrency level the runtime is designed to handle.
const conversationTxRetries = 3

// conversationTxRetryBackoff is the pause before the first replay; each
// further attempt doubles it, because the cause is contention and an instant
// retry is the least likely to succeed.
const conversationTxRetryBackoff = 5 * time.Millisecond

func persistConversationChange[T any](ctx context.Context, runtime *Runtime, operation func(pgx.Tx) (T, error)) (T, error) {
	options := pgx.TxOptions{IsoLevel: pgx.Serializable}

	return retrySerializable(ctx, runtime.onSerializationRetry, func() (T, error) {
		return withConversationTx(ctx, runtime.pool, &options, operation)
	})
}

// retrySerializable runs a transaction body, replaying it while Postgres aborts
// it for a reason a replay can resolve. The runner is a parameter so the
// policy can be exercised without a database.
func retrySerializable[T any](ctx context.Context, onRetry func(attempt int, err error), run func() (T, error)) (T, error) {
	var zero T

	for attempt := 0; ; attempt++ {
		result, err := run()
		if err == nil {
			return result, nil
		}

		if !isRetryableTxFailure(err) || attempt >= conversationTxRetries {
			return zero, err
		}

		if onRetry != nil {
			onRetry(attempt+1, err)
		}

		if waitErr := waitForRetry(ctx, attempt); waitErr != nil {
			// The caller's context ended while waiting: report the failure
			// that started the replay, not the wait.
			return zero, err
		}
	}
}

// waitForRetry pauses before the next attempt, honoring the caller's context.
func waitForRetry(ctx context.Context, attempt int) error {
	delay := conversationTxRetryBackoff << attempt

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// isRetryableTxFailure reports whether Postgres aborted the transaction for a
// reason a replay can resolve:
//
//   - a serialization failure (40001) under serializable isolation, or a
//     deadlock (40P01): the abort is the isolation level doing its job;
//   - a unique violation (23505) on the source-event id: ingestion reads for
//     an existing event and then inserts, and two deliveries of the same
//     upstream event can both find nothing. The database adjudicates that
//     race, and the replay finds the winner's row and returns it — which is
//     what the source event id promises in the first place.
//
// A violation that is not a race fails the same way on every attempt, so the
// bound is what keeps a genuine constraint violation visible to the caller.
func isRetryableTxFailure(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	switch pgErr.Code {
	case pgSerializationFailure, pgDeadlockDetected, pgUniqueViolation:
		return true
	default:
		return false
	}
}

const (
	// pgSerializationFailure is SQLSTATE 40001.
	pgSerializationFailure = "40001"
	// pgDeadlockDetected is SQLSTATE 40P01.
	pgDeadlockDetected = "40P01"
	// pgUniqueViolation is SQLSTATE 23505.
	pgUniqueViolation = "23505"
)

func (r *Runtime) persistEvent(ctx context.Context, tx pgx.Tx, event *agentos.ExternalConversationEvent) (agentos.ConversationEvent, error) {
	run, replayed, err := admitEvent(ctx, tx, event)
	if err != nil {
		return agentos.ConversationEvent{}, err
	}

	if replayed != nil {
		return *replayed, nil
	}

	parked, projectionErr := projectionIsParked(event, applyEventProjectionIsolated(ctx, tx, &run, event))
	if projectionErr != nil && !parked {
		return agentos.ConversationEvent{}, projectionErr
	}

	stored, err := r.appendEvent(ctx, tx, event.ThreadID, event.RunID, run.ProcessID, event.SourceEventID, event.SourceSequence, event.EventType, event.OccurredAt, event.Payload)
	if err != nil {
		return agentos.ConversationEvent{}, err
	}

	// The fact is durable either way; only its projection can be waiting.
	// Before, the caller was told the turn failed and the fact was lost with
	// it, which is how a steer turn disappeared silently.
	if parked {
		if err := deferProjection(ctx, tx, event.ThreadID, stored.Sequence, event.RunID, string(event.EventType)); err != nil {
			return agentos.ConversationEvent{}, err
		}
	}

	if err := advanceRunCursor(ctx, tx, event); err != nil {
		return agentos.ConversationEvent{}, err
	}

	return stored, nil
}

// admitEvent runs the guards every incoming fact passes before it is stored:
// the run exists and is this producer's, the delivery is new, and it arrives in
// order. A delivery already stored comes back as the fact to answer with, which
// is what makes ingestion idempotent on the producer's event id.
func admitEvent(ctx context.Context, tx pgx.Tx, event *agentos.ExternalConversationEvent) (agentos.ConversationRun, *agentos.ConversationEvent, error) {
	run, lastSourceSequence, err := lockRun(ctx, tx, event.RunID)
	if err != nil {
		return agentos.ConversationRun{}, nil, err
	}

	if err := validateEventRunScope(&run, event); err != nil {
		return agentos.ConversationRun{}, nil, err
	}

	existing, found, err := eventBySource(ctx, tx, event.ThreadID, event.SourceEventID)
	if err != nil {
		return agentos.ConversationRun{}, nil, err
	}

	if found {
		return run, &existing, nil
	}

	if event.SourceSequence <= lastSourceSequence {
		return agentos.ConversationRun{}, nil, fmt.Errorf("%w: got %d after %d", ErrOutOfOrderSourceEvent, event.SourceSequence, lastSourceSequence)
	}

	return run, nil, nil
}

// projectionIsParked reports whether a projection failure means "wait for the
// thread" rather than "reject this event": only a run's start waits, and only
// because another run holds the thread.
func projectionIsParked(event *agentos.ExternalConversationEvent, projectionErr error) (bool, error) {
	parked := event.EventType == agentos.ConversationEventRunStarted && errors.Is(projectionErr, ErrRunAlreadyActive)

	return parked, projectionErr
}

// advanceRunCursor records how far the run's source stream has been read, and
// when it last produced a fact — the lease the sweeper reads.
func advanceRunCursor(ctx context.Context, tx pgx.Tx, event *agentos.ExternalConversationEvent) error {
	_, err := tx.Exec(ctx, `
		UPDATE agentos_conversation_runs
		SET last_source_sequence = $2, last_activity_at = $3
		WHERE run_id = $1`, event.RunID, event.SourceSequence, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("agentos conversation: update source cursor: %w", err)
	}

	return nil
}

// applyEventProjectionIsolated applies a projection so that a failure leaves the
// surrounding transaction usable.
//
// It matters for exactly one case: taking the thread's turn is a unique-index
// violation when another run already holds it, and Postgres aborts the whole
// transaction on that error — every later statement in it would fail with
// "current transaction is aborted". The projection therefore runs in a
// savepoint: a refusal rolls back to it, and the fact that could not take the
// turn is still recorded, queued, and applied later.
func applyEventProjectionIsolated(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	nested, err := tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("agentos conversation: projection savepoint: %w", err)
	}

	if projectionErr := applyEventProjection(ctx, nested, run, event); projectionErr != nil {
		if rollbackErr := nested.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return errors.Join(projectionErr, fmt.Errorf("agentos conversation: projection rollback: %w", rollbackErr))
		}

		return projectionErr
	}

	if err := nested.Commit(ctx); err != nil {
		return fmt.Errorf("agentos conversation: projection savepoint commit: %w", err)
	}

	return nil
}

// deferProjection queues an event whose projection cannot be applied yet.
func deferProjection(ctx context.Context, tx pgx.Tx, threadID string, sequence int64, runID, eventType string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO agentos_conversation_deferred_projections (thread_id, sequence, run_id, event_type)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (thread_id, sequence) DO NOTHING`, threadID, sequence, runID, eventType)
	if err != nil {
		return fmt.Errorf("agentos conversation: defer projection: %w", err)
	}

	return nil
}

func validateEventRunScope(run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	if run.ThreadID != event.ThreadID || run.AccountID != event.AccountID || run.ProjectID != event.ProjectID {
		return ErrTenantMismatch
	}

	if event.ProcessID != "" && run.ProcessID != "" && event.ProcessID != run.ProcessID {
		return fmt.Errorf("%w: process ID does not match", ErrInvalidConversation)
	}

	return nil
}

// GetThreadSnapshot reads a consistent read-only snapshot of a thread's messages, runs, and recent events.
func (r *Runtime) GetThreadSnapshot(ctx context.Context, scope agentos.ThreadScope) (agentos.ThreadSnapshot, error) {
	if err := validateScope(scope.ThreadID, scope.AccountID, scope.ProjectID); err != nil {
		return agentos.ThreadSnapshot{}, err
	}

	options := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

	return withConversationTx(ctx, r.pool, &options, func(tx pgx.Tx) (agentos.ThreadSnapshot, error) {
		return loadThreadSnapshot(ctx, tx, scope)
	})
}

func loadThreadSnapshot(ctx context.Context, tx pgx.Tx, scope agentos.ThreadScope) (agentos.ThreadSnapshot, error) {
	var (
		cursor    int64
		updatedAt time.Time
	)

	err := tx.QueryRow(ctx, `
		SELECT next_sequence, updated_at FROM agentos_threads
		WHERE thread_id = $1 AND account_id = $2 AND project_id = $3`,
		scope.ThreadID, scope.AccountID, scope.ProjectID,
	).Scan(&cursor, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentos.ThreadSnapshot{}, ErrThreadNotFound
	}

	if err != nil {
		return agentos.ThreadSnapshot{}, fmt.Errorf("agentos conversation: read thread: %w", err)
	}

	messages, err := listMessages(ctx, tx, scope.ThreadID)
	if err != nil {
		return agentos.ThreadSnapshot{}, err
	}

	runs, err := listRuns(ctx, tx, scope.ThreadID)
	if err != nil {
		return agentos.ThreadSnapshot{}, err
	}

	limit := scope.EventLimit
	if limit <= 0 || limit > 1000 {
		limit = 200
	}

	events, err := listRecentEvents(ctx, tx, scope.ThreadID, limit)
	if err != nil {
		return agentos.ThreadSnapshot{}, err
	}

	return agentos.ThreadSnapshot{
		SchemaVersion: agentos.ConversationSchemaVersion,
		ThreadID:      scope.ThreadID, AccountID: scope.AccountID, ProjectID: scope.ProjectID,
		Messages: messages, Runs: runs, Events: events, Cursor: cursor, UpdatedAt: updatedAt,
	}, nil
}

func withConversationTx[T any](ctx context.Context, pool *pgxpool.Pool, options *pgx.TxOptions, operation func(pgx.Tx) (T, error)) (T, error) {
	var zero T

	tx, err := pool.BeginTx(ctx, *options)
	if err != nil {
		return zero, fmt.Errorf("agentos conversation: begin transaction: %w", err)
	}

	result, operationErr := operation(tx)
	if operationErr != nil {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return zero, errors.Join(operationErr, fmt.Errorf("agentos conversation: rollback transaction: %w", rollbackErr))
		}

		return zero, operationErr
	}

	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("agentos conversation: commit transaction: %w", err)
	}

	return result, nil
}

// SubscribeThread streams a thread's events by replaying persisted history and
// polling the durable event stream for new ones.
func (r *Runtime) SubscribeThread(ctx context.Context, scope agentos.ThreadStreamScope) (core.Subscription, error) {
	if err := validateScope(scope.ThreadID, scope.AccountID, scope.ProjectID); err != nil {
		return nil, err
	}

	var exists bool

	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM agentos_threads WHERE thread_id = $1 AND account_id = $2 AND project_id = $3
		)`, scope.ThreadID, scope.AccountID, scope.ProjectID).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: authorize subscription: %w", err)
	}

	if !exists {
		return nil, ErrThreadNotFound
	}

	subCtx, cancel := context.WithCancel(ctx)
	stopRuntimeCancel := context.AfterFunc(r.closeCtx, cancel)
	sub := &subscription{events: make(chan core.Event, conversationSubscriberBuf), cancel: func() {
		stopRuntimeCancel()
		cancel()
	}}

	go func() {
		defer stopRuntimeCancel()

		r.streamSubscription(subCtx, sub, scope)
	}()

	return sub, nil
}

func (r *Runtime) streamSubscription(ctx context.Context, sub *subscription, scope agentos.ThreadStreamScope) {
	defer close(sub.events)

	cursor := scope.AfterSequence

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	if !r.catchUpConversationEvents(ctx, sub, scope.ThreadID, &cursor) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.catchUpConversationEvents(ctx, sub, scope.ThreadID, &cursor) {
				return
			}
		}
	}
}

func (r *Runtime) catchUpConversationEvents(ctx context.Context, sub *subscription, threadID string, cursor *int64) bool {
	for {
		events, err := r.listEventsAfter(ctx, threadID, *cursor, conversationEventBatch)
		if err != nil {
			return ctx.Err() == nil
		}

		for i := range events {
			event := &events[i]
			if event.Sequence <= *cursor {
				continue
			}

			if !sendConversationEvent(ctx, sub, cursor, event) {
				return false
			}
		}

		if len(events) < conversationEventBatch {
			return true
		}
	}
}

func sendConversationEvent(ctx context.Context, sub *subscription, cursor *int64, event *agentos.ConversationEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case sub.events <- toCoreEvent(event):
		*cursor = event.Sequence

		return true
	}
}

func (r *Runtime) listEventsAfter(ctx context.Context, threadID string, after int64, limit int) ([]agentos.ConversationEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT event_id, thread_id, run_id, process_id, sequence, source_event_id,
		       source_sequence, event_type, occurred_at, payload
		FROM agentos_conversation_events
		WHERE thread_id = $1 AND sequence > $2
		ORDER BY sequence ASC LIMIT $3`, threadID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanEvents(rows)
}

type subscription struct {
	events    chan core.Event
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func (s *subscription) Events() <-chan core.Event { return s.events }

func (s *subscription) Close() error {
	if s != nil {
		s.closeOnce.Do(s.cancel)
	}

	return nil
}

func toCoreEvent(event *agentos.ConversationEvent) core.Event {
	payload := cloneMap(event.Payload)
	payload["schema_version"] = event.SchemaVersion
	payload["source_event_id"] = event.SourceEventID
	payload["source_sequence"] = event.SourceSequence

	return core.Event{
		EventID: event.EventID, EventType: event.EventType, RunID: event.RunID,
		ProcessID: event.ProcessID, ThreadID: event.ThreadID, Sequence: event.Sequence,
		Timestamp: event.OccurredAt, Source: "agentos.conversation", Payload: payload,
	}
}
