package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
)

const (
	// DefaultEventStoreMaxLen is the default maximum number of entries per stream.
	DefaultEventStoreMaxLen = 2000
	// DefaultEventStoreTTL is the TTL for event store streams.
	// After this period, the entire stream is evicted by Redis.
	DefaultEventStoreTTL = 2 * time.Hour
	// DefaultSnapshotTTL is the TTL for snapshot keys.
	DefaultSnapshotTTL = 2 * time.Hour
)

// EventStore is the Event Sourcing interface for agent run events.
// Events are append-only, immutable, and ordered by sequence number per session.
type EventStore interface {
	// Append appends an event to the session's event log.
	// sessionID/runID are injected into the event's BaseEvent metadata during storage.
	// Returns the assigned sequence number.
	Append(ctx context.Context, sessionID, runID string, event entity.StreamEvent) (sequence int64, err error)

	// Replay replays events after a given sequence number.
	// afterSequence=0 replays from the beginning.
	// Returns up to limit events, ordered by sequence ascending.
	Replay(ctx context.Context, sessionID string, afterSequence int64, limit int) ([]StoredEvent, error)

	// SaveSnapshot persists a snapshot of the session state at a given sequence.
	SaveSnapshot(ctx context.Context, sessionID string, sequence int64, state map[string]any) error

	// GetSnapshot retrieves the latest snapshot for a session.
	// Returns (0, nil, nil) if no snapshot exists.
	GetSnapshot(ctx context.Context, sessionID string) (sequence int64, state map[string]any, err error)
}

// StoredEvent represents a single event retrieved from the store.
type StoredEvent struct {
	Event    entity.StreamEvent
	Sequence int64
	StoredAt time.Time
}

// RedisEventStore implements EventStore backed by Redis Stream.
//
// Key design:
//   - Events are stored in Redis Stream `agent:events:<sessionID>`
//   - Stream entry IDs use the format `<sequence>-0` for O(1) range queries
//     by sequence number via XRANGE
//   - A Sequencer provides atomic sequence number generation via Redis INCR
//   - Snapshots are stored in `agent:snapshot:<sessionID>` as JSON
type RedisEventStore struct {
	rdb       *redis.Redis
	sequencer Sequencer
}

// NewRedisEventStore creates a RedisEventStore.
func NewRedisEventStore(rdb *redis.Redis, sequencer Sequencer) *RedisEventStore {
	return &RedisEventStore{
		rdb:       rdb,
		sequencer: sequencer,
	}
}

// Append assigns a sequence number, injects sessionID/runID into the event,
// and writes it to the Redis Stream. Returns the assigned sequence number.
func (s *RedisEventStore) Append(ctx context.Context, sessionID, runID string, event entity.StreamEvent) (int64, error) {
	seq, err := s.sequencer.Next(ctx, sessionID)
	if err != nil {
		return 0, fmt.Errorf("event_store: sequencer: %w", err)
	}

	// Marshal then inject sessionID/runID into JSON payload.
	data, err := entity.MarshalEvent(event)
	if err != nil {
		return 0, fmt.Errorf("event_store: marshal: %w", err)
	}

	// Inject sessionID and runID into the serialized JSON.
	payload := injectEventMetadata(string(data), sessionID, runID)

	streamKey := streamKeyForSession(sessionID)
	entryID := fmt.Sprintf("%d-0", seq)

	_, err = s.rdb.StreamAddWithID(ctx, streamKey, entryID, map[string]any{
		"data":       payload,
		"event_type": event.EventType(),
		"sequence":   seq,
	}, DefaultEventStoreMaxLen)
	if err != nil {
		return 0, fmt.Errorf("event_store: append: %w", err)
	}

	// Best-effort TTL refresh so streams of finished runs eventually expire.
	if _, err := s.rdb.Expire(ctx, streamKey, DefaultEventStoreTTL); err != nil {
		_ = err
	}

	return seq, nil
}

// Replay returns events after a given sequence number, up to limit.
func (s *RedisEventStore) Replay(ctx context.Context, sessionID string, afterSequence int64, limit int) ([]StoredEvent, error) {
	streamKey := streamKeyForSession(sessionID)

	// XRANGE from (afterSequence+1)-0 to +, limited by count
	start := fmt.Sprintf("%d", afterSequence+1)

	entries, err := s.rdb.StreamRange(ctx, streamKey, start, "+", limit)
	if err != nil {
		return nil, fmt.Errorf("event_store: replay: %w", err)
	}

	stored := make([]StoredEvent, 0, len(entries))
	for _, entry := range entries {
		rawData, ok := entry.Values["data"]
		if !ok {
			continue
		}

		dataStr, ok := rawData.(string)
		if !ok {
			continue
		}

		if dataStr == "" {
			continue // skip corrupt entries
		}

		event, err := entity.UnmarshalEvent([]byte(dataStr))
		if err != nil {
			continue // skip corrupt events
		}

		seq := parseSequenceFromID(entry.ID)
		stored = append(stored, StoredEvent{
			Event:    event,
			Sequence: seq,
		})
	}

	return stored, nil
}

// SaveSnapshot persists a snapshot of the session state.
func (s *RedisEventStore) SaveSnapshot(ctx context.Context, sessionID string, sequence int64, state map[string]any) error {
	snapshot := snapshotData{
		Sequence: sequence,
		State:    state,
		SavedAt:  time.Now().UTC(),
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("event_store: snapshot marshal: %w", err)
	}

	key := snapshotKeyForSession(sessionID)
	if err := s.rdb.Set(ctx, key, string(data), DefaultSnapshotTTL); err != nil {
		return fmt.Errorf("event_store: snapshot save: %w", err)
	}

	return nil
}

// GetSnapshot retrieves the latest snapshot for a session.
func (s *RedisEventStore) GetSnapshot(ctx context.Context, sessionID string) (sequence int64, state map[string]any, err error) {
	key := snapshotKeyForSession(sessionID)

	data, err := s.rdb.Get(ctx, key)
	if err != nil {
		return 0, nil, nil // no snapshot
	}

	var snapshot snapshotData
	if err := json.Unmarshal([]byte(data), &snapshot); err != nil {
		return 0, nil, fmt.Errorf("event_store: snapshot unmarshal: %w", err)
	}

	return snapshot.Sequence, snapshot.State, nil
}

// -- internal helpers --

// entryIDPartsCount is the number of parts expected when splitting a stream entry ID.
const entryIDPartsCount = 2

type snapshotData struct {
	Sequence int64          `json:"sequence"`
	State    map[string]any `json:"state"`
	SavedAt  time.Time      `json:"saved_at"`
}

func streamKeyForSession(sessionID string) string {
	return fmt.Sprintf("agent:events:%s", sessionID)
}

func snapshotKeyForSession(sessionID string) string {
	return fmt.Sprintf("agent:snapshot:%s", sessionID)
}

func parseSequenceFromID(entryID string) int64 {
	// Entry ID format: "<sequence>-0"
	parts := strings.SplitN(entryID, "-", entryIDPartsCount)
	if len(parts) < 1 {
		return 0
	}

	seq, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}

	return seq
}

// injectEventMetadata injects sessionID and runID into a serialized StreamEvent JSON.
// The event constructors don't populate these fields — they are injected at the
// Event Store layer as the single authority for these identifiers.
func injectEventMetadata(jsonData, sessionID, runID string) string {
	if len(jsonData) < len("{}") {
		return jsonData
	}

	return jsonData[:len(jsonData)-1] +
		`,"session_id":"` + sessionID +
		`","run_id":"` + runID + `"}`
}
