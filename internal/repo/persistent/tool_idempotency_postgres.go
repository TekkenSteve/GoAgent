package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	agenttool "github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
)

var (
	// ErrToolIdempotencyUnavailable reports a store without a database, which
	// is a wiring error rather than a lookup miss.
	ErrToolIdempotencyUnavailable = errors.New("tool idempotency store: no database")
	// ErrToolIdempotencyResultRequired reports a recorded result without a value.
	ErrToolIdempotencyResultRequired = errors.New("tool idempotency store: result is required")
)

// ToolIdempotencyRetention is how long a tool call's result stays resolvable.
// A platform retry of an activity happens within its retry window, so a day is
// generous; the record exists to answer a repeat, not to be history.
const ToolIdempotencyRetention = 24 * time.Hour

// ToolIdempotencyStore is the durable half of tool-call idempotency: the first
// attempt's result, keyed by the call's own id, so a retry answers with it
// instead of running the tool again.
//
// It is durable rather than in-process on purpose: activities run on whichever
// worker picks them up, so a per-process cache would miss exactly the retry it
// exists to catch.
type ToolIdempotencyStore struct {
	pg        *postgres.Postgres
	retention time.Duration
}

// NewToolIdempotencyStore creates the store over the given database.
func NewToolIdempotencyStore(pg *postgres.Postgres) *ToolIdempotencyStore {
	return &ToolIdempotencyStore{pg: pg, retention: ToolIdempotencyRetention}
}

// WithRetention overrides how long a record stays resolvable.
func (s *ToolIdempotencyStore) WithRetention(retention time.Duration) *ToolIdempotencyStore {
	if retention > 0 {
		s.retention = retention
	}

	return s
}

// Get returns the recorded result for a key, or false when there is none
// within the retention window.
func (s *ToolIdempotencyStore) Get(ctx context.Context, key string) (agenttool.Result, bool, error) {
	if s.pg == nil {
		return agenttool.Result{}, false, ErrToolIdempotencyUnavailable
	}

	const query = `
		SELECT run_id, tool_call_id, tool_name, output, attempts
		FROM agentos_tool_idempotency
		WHERE key = $1 AND created_at > now() - $2::interval`

	var (
		result   agenttool.Result
		output   []byte
		attempts int
	)

	row := s.pg.Pool.QueryRow(ctx, query, key, s.retention.String())
	if err := row.Scan(&result.RunID, &result.ToolCallID, &result.ToolName, &output, &attempts); err != nil {
		if missingRow(err) {
			return agenttool.Result{}, false, nil
		}

		return agenttool.Result{}, false, fmt.Errorf("tool idempotency store - get %q: %w", key, err)
	}

	if err := json.Unmarshal(output, &result.Output); err != nil {
		return agenttool.Result{}, false, fmt.Errorf("tool idempotency store - decode result for %q: %w", key, err)
	}

	result.Attempts = attempts

	return result, true, nil
}

// Put records the first result for a key. A later write for the same key is
// ignored: the first attempt is the one that had the side effect, so its
// result is the answer every retry must see.
func (s *ToolIdempotencyStore) Put(ctx context.Context, key string, result *agenttool.Result) error {
	if s.pg == nil {
		return ErrToolIdempotencyUnavailable
	}

	if result == nil {
		return ErrToolIdempotencyResultRequired
	}

	output, err := json.Marshal(result.Output)
	if err != nil {
		return fmt.Errorf("tool idempotency store - encode result for %q: %w", key, err)
	}

	const insert = `
		INSERT INTO agentos_tool_idempotency (key, run_id, tool_call_id, tool_name, output, attempts)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (key) DO NOTHING`

	if _, err := s.pg.Pool.Exec(ctx, insert, key, result.RunID, result.ToolCallID, result.ToolName, output, result.Attempts); err != nil {
		return fmt.Errorf("tool idempotency store - put %q: %w", key, err)
	}

	// Pruning on write keeps the table bounded without a background job: the
	// write path is the only place rows are added.
	const prune = `DELETE FROM agentos_tool_idempotency WHERE created_at <= now() - $1::interval`

	if _, err := s.pg.Pool.Exec(ctx, prune, s.retention.String()); err != nil {
		return fmt.Errorf("tool idempotency store - prune: %w", err)
	}

	return nil
}

// Ensure the store satisfies the pipeline's port at compile time.
var _ agenttool.IdempotencyStore = (*ToolIdempotencyStore)(nil)
