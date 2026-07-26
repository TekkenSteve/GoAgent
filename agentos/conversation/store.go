package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type rowQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func prepareStartSpec(spec *agentos.StartConversationRunSpec) (agentos.StartConversationRunSpec, error) {
	if spec == nil {
		return agentos.StartConversationRunSpec{}, fmt.Errorf("%w: start spec is required", ErrInvalidConversation)
	}

	prepared := *spec
	normalizeStartSpec(&prepared)

	if err := validateStartSpec(&prepared); err != nil {
		return agentos.StartConversationRunSpec{}, err
	}

	return prepared, nil
}

func normalizeStartSpec(prepared *agentos.StartConversationRunSpec) {
	prepared.RunID = strings.TrimSpace(prepared.RunID)
	prepared.ThreadID = strings.TrimSpace(prepared.ThreadID)
	prepared.ProcessID = strings.TrimSpace(prepared.ProcessID)
	prepared.AccountID = strings.TrimSpace(prepared.AccountID)
	prepared.ProjectID = strings.TrimSpace(prepared.ProjectID)
	prepared.MessageID = strings.TrimSpace(prepared.MessageID)

	prepared.IdempotencyKey = strings.TrimSpace(prepared.IdempotencyKey)
	if prepared.RunID == "" {
		prepared.RunID = uuid.NewString()
	}

	if prepared.MessageID == "" {
		prepared.MessageID = uuid.NewString()
	}

	if prepared.RequestedAt.IsZero() {
		prepared.RequestedAt = time.Now().UTC()
	} else {
		prepared.RequestedAt = prepared.RequestedAt.UTC()
	}

	if prepared.Attachments == nil {
		prepared.Attachments = []agentos.Attachment{}
	}

	if prepared.MessageMetadata == nil {
		prepared.MessageMetadata = map[string]any{}
	}

	if prepared.RunMetadata == nil {
		prepared.RunMetadata = map[string]any{}
	}

	if prepared.Resume != nil {
		resume := *prepared.Resume
		resume.InterruptID = strings.TrimSpace(resume.InterruptID)
		prepared.Resume = &resume
	}
}

func validateStartSpec(prepared *agentos.StartConversationRunSpec) error {
	if prepared.ThreadID == "" || prepared.AccountID == "" || prepared.ProjectID == "" || prepared.IdempotencyKey == "" {
		return fmt.Errorf("%w: thread, tenant, and idempotency key are required", ErrInvalidConversation)
	}

	if strings.TrimSpace(prepared.UserMessage) == "" && len(prepared.Attachments) == 0 {
		return fmt.Errorf("%w: user message or attachment is required", ErrInvalidConversation)
	}

	if prepared.Resume != nil && prepared.Resume.InterruptID == "" {
		return fmt.Errorf("%w: resume interrupt ID is required", ErrInvalidConversation)
	}

	return nil
}

func prepareExternalEvent(event *agentos.ExternalConversationEvent) (agentos.ExternalConversationEvent, error) {
	if event == nil {
		return agentos.ExternalConversationEvent{}, fmt.Errorf("%w: event is required", ErrInvalidConversation)
	}

	prepared := *event
	prepared.ThreadID = strings.TrimSpace(prepared.ThreadID)
	prepared.RunID = strings.TrimSpace(prepared.RunID)
	prepared.ProcessID = strings.TrimSpace(prepared.ProcessID)
	prepared.AccountID = strings.TrimSpace(prepared.AccountID)
	prepared.ProjectID = strings.TrimSpace(prepared.ProjectID)
	prepared.SourceEventID = strings.TrimSpace(prepared.SourceEventID)

	prepared.EventType = core.EventType(strings.TrimSpace(string(prepared.EventType)))

	if prepared.OccurredAt.IsZero() {
		prepared.OccurredAt = time.Now().UTC()
	} else {
		prepared.OccurredAt = prepared.OccurredAt.UTC()
	}

	if prepared.Payload == nil {
		prepared.Payload = map[string]any{}
	}

	if err := validateExternalConversationEvent(&prepared); err != nil {
		return agentos.ExternalConversationEvent{}, err
	}

	return prepared, nil
}

func validateExternalConversationEvent(event *agentos.ExternalConversationEvent) error {
	if event.ThreadID == "" || event.RunID == "" || event.AccountID == "" || event.ProjectID == "" || event.SourceEventID == "" || event.EventType == "" {
		return fmt.Errorf("%w: event identity, tenant, and type are required", ErrInvalidConversation)
	}

	if event.SourceSequence <= 0 {
		return fmt.Errorf("%w: source sequence must be positive", ErrInvalidConversation)
	}

	return nil
}

func validateScope(threadID, accountID, projectID string) error {
	if strings.TrimSpace(threadID) == "" || strings.TrimSpace(accountID) == "" || strings.TrimSpace(projectID) == "" {
		return fmt.Errorf("%w: thread and tenant scope are required", ErrInvalidConversation)
	}

	return nil
}

func ensureThread(ctx context.Context, tx pgx.Tx, threadID, accountID, projectID string, now time.Time) error {
	var storedAccount, storedProject string

	err := tx.QueryRow(ctx, `SELECT account_id, project_id FROM agentos_threads WHERE thread_id = $1 FOR UPDATE`, threadID).Scan(&storedAccount, &storedProject)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `
			INSERT INTO agentos_threads (thread_id, account_id, project_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $4)`, threadID, accountID, projectID, now)
		if err != nil {
			return fmt.Errorf("agentos conversation: insert thread: %w", err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("agentos conversation: lock thread: %w", err)
	}

	if storedAccount != accountID || storedProject != projectID {
		return ErrTenantMismatch
	}

	return nil
}

func validateResume(ctx context.Context, tx pgx.Tx, spec *agentos.StartConversationRunSpec) error {
	var interruptJSON []byte

	err := tx.QueryRow(ctx, `
		SELECT parent.interrupt FROM agentos_conversation_runs parent
		WHERE parent.thread_id = $1 AND parent.status = 'interrupted'
		  AND NOT EXISTS (
			SELECT 1 FROM agentos_conversation_runs resumed
			WHERE resumed.thread_id = parent.thread_id
			  AND resumed.resume_interrupt_id = parent.interrupt->>'interrupt_id'
		  )
		ORDER BY completed_at DESC, created_at DESC LIMIT 1`, spec.ThreadID).Scan(&interruptJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		if spec.Resume != nil {
			return ErrInterruptMismatch
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("agentos conversation: read interrupt: %w", err)
	}

	var interrupt agentos.ConversationInterrupt
	if err := json.Unmarshal(interruptJSON, &interrupt); err != nil {
		return fmt.Errorf("agentos conversation: decode interrupt: %w", err)
	}

	if spec.Resume == nil {
		return ErrInterruptRequired
	}

	if interrupt.InterruptID != spec.Resume.InterruptID {
		return ErrInterruptMismatch
	}

	return nil
}

func getRunByIdempotency(ctx context.Context, tx pgx.Tx, threadID, key string) (agentos.ConversationRun, bool, error) {
	row := tx.QueryRow(ctx, runSelect+` WHERE thread_id = $1 AND idempotency_key = $2`, threadID, key)

	run, err := scanRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentos.ConversationRun{}, false, nil
	}

	if err != nil {
		return agentos.ConversationRun{}, false, fmt.Errorf("agentos conversation: read idempotent run: %w", err)
	}

	return run, true, nil
}

func lockRun(ctx context.Context, tx pgx.Tx, runID string) (agentos.ConversationRun, int64, error) {
	row := tx.QueryRow(ctx, runSelect+` WHERE run_id = $1 FOR UPDATE`, runID)

	var (
		run                    agentos.ConversationRun
		interruptJSON          []byte
		startedAt, completedAt *time.Time
		lastSourceSequence     int64
	)

	err := row.Scan(
		&run.RunID, &run.ThreadID, &run.ProcessID, &run.AccountID, &run.ProjectID,
		&run.Status, &run.Outcome, &interruptJSON, &run.ErrorCode, &run.Error,
		&run.CreatedAt, &startedAt, &completedAt, &lastSourceSequence,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentos.ConversationRun{}, 0, ErrRunNotFound
	}

	if err != nil {
		return agentos.ConversationRun{}, 0, fmt.Errorf("agentos conversation: lock run: %w", err)
	}

	finishRunScan(&run, interruptJSON, startedAt, completedAt)

	return run, lastSourceSequence, nil
}

const runSelect = `
	SELECT run_id, thread_id, process_id, account_id, project_id, status, outcome,
	       interrupt, error_code, error_message, created_at, started_at, completed_at,
	       last_source_sequence
	FROM agentos_conversation_runs`

func scanRun(row pgx.Row) (agentos.ConversationRun, error) {
	var (
		run                    agentos.ConversationRun
		interruptJSON          []byte
		startedAt, completedAt *time.Time
		ignoredSourceSequence  int64
	)

	err := row.Scan(
		&run.RunID, &run.ThreadID, &run.ProcessID, &run.AccountID, &run.ProjectID,
		&run.Status, &run.Outcome, &interruptJSON, &run.ErrorCode, &run.Error,
		&run.CreatedAt, &startedAt, &completedAt, &ignoredSourceSequence,
	)
	if err != nil {
		return agentos.ConversationRun{}, err
	}

	finishRunScan(&run, interruptJSON, startedAt, completedAt)

	return run, nil
}

func finishRunScan(run *agentos.ConversationRun, interruptJSON []byte, startedAt, completedAt *time.Time) {
	if len(interruptJSON) > 0 && string(interruptJSON) != "null" {
		var interrupt agentos.ConversationInterrupt
		if json.Unmarshal(interruptJSON, &interrupt) == nil {
			run.Interrupt = &interrupt
		}
	}

	if startedAt != nil {
		run.StartedAt = *startedAt
	}

	if completedAt != nil {
		run.CompletedAt = *completedAt
	}
}

func applyEventProjection(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	if event.EventType == agentos.ConversationEventRunStarted {
		if err := requireRunStatus(run, event, agentos.ConversationRunPending); err != nil {
			return err
		}

		_, err := tx.Exec(ctx, `UPDATE agentos_conversation_runs SET status = 'running', started_at = $2 WHERE run_id = $1`, run.RunID, event.OccurredAt)

		return wrapProjectionError(err)
	}

	if event.EventType == agentos.ConversationEventRunError {
		if run.Status != agentos.ConversationRunPending && run.Status != agentos.ConversationRunRunning {
			return transitionError(run.Status, string(event.EventType))
		}

		_, err := tx.Exec(ctx, `
			UPDATE agentos_conversation_runs
			SET status = 'error', error_code = $2, error_message = $3, completed_at = $4
			WHERE run_id = $1`, run.RunID, stringPayload(event.Payload, "code"), stringPayload(event.Payload, "message"), event.OccurredAt)

		return wrapProjectionError(err)
	}

	handlers := map[core.EventType]func(context.Context, pgx.Tx, *agentos.ConversationRun, *agentos.ExternalConversationEvent) error{
		agentos.ConversationEventTextMessageStart:   startMessage,
		agentos.ConversationEventTextMessageContent: appendMessageContent,
		agentos.ConversationEventTextMessageEnd:     endMessage,
		agentos.ConversationEventRunFinished:        finishRun,
	}

	handler, projected := handlers[event.EventType]
	if !projected {
		return nil
	}

	if err := requireRunStatus(run, event, agentos.ConversationRunRunning); err != nil {
		return err
	}

	return handler(ctx, tx, run, event)
}

func requireRunStatus(run *agentos.ConversationRun, event *agentos.ExternalConversationEvent, status string) error {
	if run.Status != status {
		return transitionError(run.Status, string(event.EventType))
	}

	return nil
}

func startMessage(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	messageID := stringPayload(event.Payload, "message_id")

	role := stringPayload(event.Payload, "role")
	if messageID == "" || (role != "assistant" && role != "system" && role != "tool") {
		return fmt.Errorf("%w: message start requires ID and valid role", ErrInvalidConversation)
	}

	metadata, err := marshalJSON(mapPayload(event.Payload, "metadata"))
	if err != nil {
		return fmt.Errorf("%w: message metadata", ErrInvalidConversation)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agentos_messages (
			message_id, thread_id, run_id, process_id, role, status, metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, 'streaming', $6, $7)`,
		messageID, run.ThreadID, run.RunID, run.ProcessID, role, metadata, event.OccurredAt)
	if isUniqueViolation(err) {
		return fmt.Errorf("%w: duplicate message start", ErrInvalidTransition)
	}

	return wrapProjectionError(err)
}

func appendMessageContent(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	messageID := stringPayload(event.Payload, "message_id")

	delta := stringPayload(event.Payload, "delta")
	if messageID == "" || delta == "" {
		return fmt.Errorf("%w: message content requires ID and delta", ErrInvalidConversation)
	}

	result, err := tx.Exec(ctx, `
		UPDATE agentos_messages SET content = content || $2
		WHERE message_id = $1 AND run_id = $3 AND status = 'streaming'`, messageID, delta, run.RunID)
	if err != nil {
		return wrapProjectionError(err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: message content without active message", ErrInvalidTransition)
	}

	return nil
}

func endMessage(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	messageID := stringPayload(event.Payload, "message_id")
	if messageID == "" {
		return fmt.Errorf("%w: message end requires ID", ErrInvalidConversation)
	}

	content, hasContent := event.Payload["content"].(string)

	var (
		result pgconn.CommandTag
		err    error
	)

	if hasContent {
		result, err = tx.Exec(ctx, `
			UPDATE agentos_messages SET content = $2, status = 'completed', completed_at = $3
			WHERE message_id = $1 AND run_id = $4 AND status = 'streaming'`, messageID, content, event.OccurredAt, run.RunID)
	} else {
		result, err = tx.Exec(ctx, `
			UPDATE agentos_messages SET status = 'completed', completed_at = $2
			WHERE message_id = $1 AND run_id = $3 AND status = 'streaming'`, messageID, event.OccurredAt, run.RunID)
	}

	if err != nil {
		return wrapProjectionError(err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: message end without active message", ErrInvalidTransition)
	}

	return nil
}

func finishRun(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun, event *agentos.ExternalConversationEvent) error {
	outcome := stringPayload(event.Payload, "outcome")
	status := ""

	var interruptJSON any

	switch outcome {
	case agentos.ConversationOutcomeNormal:
		status = agentos.ConversationRunCompleted
	case agentos.ConversationOutcomeCancelled:
		status = agentos.ConversationRunCancelled
	case agentos.ConversationOutcomeInterrupt:
		status = agentos.ConversationRunInterrupted

		encoded, err := prepareInterrupt(ctx, tx, run.RunID, event.Payload)
		if err != nil {
			return err
		}

		interruptJSON = encoded
	default:
		return fmt.Errorf("%w: unsupported run outcome %q", ErrInvalidConversation, outcome)
	}

	_, err := tx.Exec(ctx, `
		UPDATE agentos_conversation_runs SET status = $2, outcome = $3, interrupt = $4, completed_at = $5
		WHERE run_id = $1`, run.RunID, status, outcome, interruptJSON, event.OccurredAt)

	return wrapProjectionError(err)
}

func prepareInterrupt(ctx context.Context, tx pgx.Tx, runID string, payload map[string]any) ([]byte, error) {
	interruptMap := mapPayload(payload, "interrupt")

	interrupt := agentos.ConversationInterrupt{
		InterruptID: stringValue(interruptMap["interrupt_id"]),
		Type:        stringValue(interruptMap["type"]),
		Prompt:      stringValue(interruptMap["prompt"]),
		InputSchema: mapValue(interruptMap["input_schema"]),
		Metadata:    mapValue(interruptMap["metadata"]),
	}
	if interrupt.InterruptID == "" || interrupt.Type == "" || interrupt.Prompt == "" {
		return nil, fmt.Errorf("%w: interrupt identity, type, and prompt are required", ErrInvalidConversation)
	}

	var completedAssistant bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM agentos_messages WHERE run_id = $1 AND role = 'assistant' AND status = 'completed')`, runID).Scan(&completedAssistant); err != nil {
		return nil, wrapProjectionError(err)
	}

	if !completedAssistant {
		return nil, fmt.Errorf("%w: interrupt requires a completed assistant message", ErrInvalidTransition)
	}

	encoded, err := marshalJSON(interrupt)
	if err != nil {
		return nil, fmt.Errorf("%w: interrupt payload", ErrInvalidConversation)
	}

	return encoded, nil
}

func appendEvent(ctx context.Context, tx pgx.Tx, threadID, runID, processID, sourceEventID string, sourceSequence int64, eventType core.EventType, occurredAt time.Time, payload map[string]any, enqueueForStream bool) (agentos.ConversationEvent, error) {
	var sequence int64

	err := tx.QueryRow(ctx, `
		UPDATE agentos_threads SET next_sequence = next_sequence + 1, updated_at = $2
		WHERE thread_id = $1 RETURNING next_sequence`, threadID, occurredAt).Scan(&sequence)
	if err != nil {
		return agentos.ConversationEvent{}, fmt.Errorf("agentos conversation: allocate sequence: %w", err)
	}

	eventID := uuid.NewString()

	encoded, err := marshalJSON(payload)
	if err != nil {
		return agentos.ConversationEvent{}, fmt.Errorf("%w: event payload: %w", ErrInvalidConversation, err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agentos_conversation_events (
			thread_id, sequence, event_id, run_id, process_id, source_event_id,
			source_sequence, event_type, occurred_at, payload
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		threadID, sequence, eventID, runID, processID, sourceEventID,
		sourceSequence, eventType, occurredAt, encoded)
	if err != nil {
		return agentos.ConversationEvent{}, fmt.Errorf("agentos conversation: insert event: %w", err)
	}

	if enqueueForStream {
		_, err = tx.Exec(ctx, `
			INSERT INTO agentos_conversation_event_outbox (thread_id, sequence, available_at)
			VALUES ($1, $2, NOW()) ON CONFLICT (thread_id, sequence) DO NOTHING`, threadID, sequence)
		if err != nil {
			return agentos.ConversationEvent{}, fmt.Errorf("agentos conversation: enqueue event: %w", err)
		}
	}

	return agentos.ConversationEvent{
		SchemaVersion: agentos.ConversationSchemaVersion,
		EventID:       eventID, ThreadID: threadID, RunID: runID, ProcessID: processID,
		Sequence: sequence, SourceEventID: sourceEventID, SourceSequence: sourceSequence,
		EventType: eventType, OccurredAt: occurredAt, Payload: cloneMap(payload),
	}, nil
}

func eventBySource(ctx context.Context, tx pgx.Tx, threadID, sourceEventID string) (agentos.ConversationEvent, bool, error) {
	row := tx.QueryRow(ctx, `
		SELECT event_id, thread_id, run_id, process_id, sequence, source_event_id,
		       source_sequence, event_type, occurred_at, payload
		FROM agentos_conversation_events WHERE thread_id = $1 AND source_event_id = $2`, threadID, sourceEventID)

	event, err := scanEvent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentos.ConversationEvent{}, false, nil
	}

	if err != nil {
		return agentos.ConversationEvent{}, false, fmt.Errorf("agentos conversation: read source event: %w", err)
	}

	return event, true, nil
}

func listMessages(ctx context.Context, query rowQuerier, threadID string) ([]agentos.ConversationMessage, error) {
	rows, err := query.Query(ctx, `
		SELECT message_id, thread_id, run_id, process_id, role, content, status,
		       attachments, metadata, created_at, completed_at
		FROM agentos_messages WHERE thread_id = $1 ORDER BY created_at ASC, message_id ASC`, threadID)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: list messages: %w", err)
	}
	defer rows.Close()

	messages := make([]agentos.ConversationMessage, 0)

	for rows.Next() {
		var (
			message                       agentos.ConversationMessage
			attachmentsJSON, metadataJSON []byte
			completedAt                   *time.Time
		)

		if err := rows.Scan(
			&message.MessageID, &message.ThreadID, &message.RunID, &message.ProcessID,
			&message.Role, &message.Content, &message.Status, &attachmentsJSON,
			&metadataJSON, &message.CreatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("agentos conversation: scan message: %w", err)
		}

		if err := json.Unmarshal(attachmentsJSON, &message.Attachments); err != nil {
			return nil, fmt.Errorf("agentos conversation: decode message attachments: %w", err)
		}

		if err := json.Unmarshal(metadataJSON, &message.Metadata); err != nil {
			return nil, fmt.Errorf("agentos conversation: decode message metadata: %w", err)
		}

		if completedAt != nil {
			message.CompletedAt = *completedAt
		}

		messages = append(messages, message)
	}

	return messages, rows.Err()
}

func listRuns(ctx context.Context, query rowQuerier, threadID string) ([]agentos.ConversationRun, error) {
	rows, err := query.Query(ctx, runSelect+` WHERE thread_id = $1 ORDER BY created_at ASC, run_id ASC`, threadID)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: list runs: %w", err)
	}
	defer rows.Close()

	runs := make([]agentos.ConversationRun, 0)

	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("agentos conversation: scan run: %w", err)
		}

		runs = append(runs, run)
	}

	return runs, rows.Err()
}

func listRecentEvents(ctx context.Context, query rowQuerier, threadID string, limit int) ([]agentos.ConversationEvent, error) {
	rows, err := query.Query(ctx, `
		SELECT event_id, thread_id, run_id, process_id, sequence, source_event_id,
		       source_sequence, event_type, occurred_at, payload
		FROM (
			SELECT event_id, thread_id, run_id, process_id, sequence, source_event_id,
			       source_sequence, event_type, occurred_at, payload
			FROM agentos_conversation_events WHERE thread_id = $1
			ORDER BY sequence DESC LIMIT $2
		) recent ORDER BY sequence ASC`, threadID, limit)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: list events: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

func scanEvents(rows pgx.Rows) ([]agentos.ConversationEvent, error) {
	events := make([]agentos.ConversationEvent, 0)

	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, rows.Err()
}

func scanEvent(row pgx.Row) (agentos.ConversationEvent, error) {
	var (
		event       agentos.ConversationEvent
		payloadJSON []byte
	)

	err := row.Scan(
		&event.EventID, &event.ThreadID, &event.RunID, &event.ProcessID,
		&event.Sequence, &event.SourceEventID, &event.SourceSequence,
		&event.EventType, &event.OccurredAt, &payloadJSON,
	)
	if err != nil {
		return agentos.ConversationEvent{}, err
	}

	event.SchemaVersion = agentos.ConversationSchemaVersion
	if err := json.Unmarshal(payloadJSON, &event.Payload); err != nil {
		return agentos.ConversationEvent{}, fmt.Errorf("agentos conversation: decode event payload: %w", err)
	}

	return event, nil
}

func marshalJSON(value any) ([]byte, error) {
	if value == nil {
		value = map[string]any{}
	}

	return json.Marshal(value)
}

func stringPayload(payload map[string]any, key string) string { return stringValue(payload[key]) }

func stringValue(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(text)
}

func mapPayload(payload map[string]any, key string) map[string]any { return mapValue(payload[key]) }

func mapValue(value any) map[string]any {
	record, ok := value.(map[string]any)
	if !ok {
		return nil
	}

	return record
}

func cloneMap(source map[string]any) map[string]any {
	const expectedProjectionFields = 3

	cloned := make(map[string]any, len(source)+expectedProjectionFields)
	maps.Copy(cloned, source)

	return cloned
}

func transitionError(status, eventType string) error {
	return fmt.Errorf("%w: %s cannot accept %s", ErrInvalidTransition, status, eventType)
}

func wrapProjectionError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("agentos conversation: project event: %w", err)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
