// Package redis implements dual-pool Redis client:
//   - General pool: for non-blocking ops (GET, SET, XADD)
//   - Stream pool: for blocking ops (XREAD, XREADGROUP), prevents connection starvation
//   - Timeout protection: all operations wrapped with per-operation timeout
//   - Metrics: operation count, timeout count, error count
package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	_defaultGeneralPoolSize   = 200
	_defaultStreamPoolSize    = 50
	_defaultOpTimeout         = 5 * time.Second
	_defaultStreamTimeout     = 10 * time.Second
	_defaultSocketTimeout     = 10 * time.Second
	_defaultConnectTimeout    = 5 * time.Second
	_defaultConnAttempts      = 10
	_defaultConnRetryInterval = time.Second
	_defaultMaxRetries        = 3
	_defaultPoolTimeout       = 2 * time.Minute
	_defaultConnMaxLifetime   = 30 * time.Minute
	_defaultConnMaxIdleTime   = 10 * time.Minute
	_defaultMinRetryBackoff   = 100 * time.Millisecond
	_defaultMaxRetryBackoff   = 2 * time.Second
)

// Config is the configuration for the Redis client.
type Config struct {
	URL             string
	GeneralPoolSize int
	StreamPoolSize  int
	OpTimeout       time.Duration
	StreamTimeout   time.Duration
	SocketTimeout   time.Duration
	ConnectTimeout  time.Duration
	MaxRetries      int
}

// Redis is a dual-connection-pool Redis client.
// GeneralClient and StreamClient use isolated pools so blocking XREAD
// on the stream pool cannot starve the general pool.
//
// All exported methods are safe for concurrent use.
type Redis struct {
	GeneralClient *goredis.Client
	StreamClient  *goredis.Client

	hub        *StreamHub
	initTime   time.Time
	opCount    atomic.Int64
	timeoutCnt atomic.Int64
	errorCnt   atomic.Int64
}

// PoolStats contains connection pool statistics.
type PoolStats struct {
	Hits      int32 `json:"hits"`
	Misses    int32 `json:"misses"`
	Timeouts  int32 `json:"timeouts"`
	TotalConn int32 `json:"total_conn"`
	IdleConn  int32 `json:"idle_conn"`
	StaleConn int32 `json:"stale_conn"`
}

// HealthReport contains the full health check result.
type HealthReport struct {
	Status         string         `json:"status"` // "healthy", "degraded", "unhealthy"
	GeneralPing    bool           `json:"general_ping"`
	StreamPing     bool           `json:"stream_ping"`
	GeneralLatency int64          `json:"general_latency_ms"`
	GeneralPool    PoolStats      `json:"general_pool"`
	StreamPool     PoolStats      `json:"stream_pool"`
	Hub            map[string]any `json:"hub,omitempty"`
	OpCount        int64          `json:"op_count"`
	TimeoutCount   int64          `json:"timeout_count"`
	ErrorCount     int64          `json:"error_count"`
	UptimeSeconds  int            `json:"uptime_seconds"`
}

// ClientStats contains current client metrics.
type ClientStats struct {
	OpCount      int64 `json:"op_count"`
	TimeoutCount int64 `json:"timeout_count"`
	ErrorCount   int64 `json:"error_count"`
}

// New creates a Redis client with dual connection pools (general + stream).
func New(ctx context.Context, url string, opts ...Option) (*Redis, error) {
	cfg := &Config{
		URL:             url,
		GeneralPoolSize: _defaultGeneralPoolSize,
		StreamPoolSize:  _defaultStreamPoolSize,
		OpTimeout:       _defaultOpTimeout,
		StreamTimeout:   _defaultStreamTimeout,
		SocketTimeout:   _defaultSocketTimeout,
		ConnectTimeout:  _defaultConnectTimeout,
		MaxRetries:      _defaultMaxRetries,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	optURL, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("redis - New - ParseURL: %w", err)
	}

	addr := optURL.Addr
	username := optURL.Username
	password := optURL.Password

	// General pool — for non-blocking ops
	generalOpts := &goredis.Options{
		Addr:            addr,
		Username:        username,
		Password:        password,
		DB:              0,
		PoolSize:        cfg.GeneralPoolSize,
		DialTimeout:     cfg.ConnectTimeout,
		ReadTimeout:     cfg.SocketTimeout,
		WriteTimeout:    cfg.SocketTimeout,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: _defaultMinRetryBackoff,
		MaxRetryBackoff: _defaultMaxRetryBackoff,
		PoolTimeout:     _defaultPoolTimeout,
		ConnMaxLifetime: _defaultConnMaxLifetime,
		ConnMaxIdleTime: _defaultConnMaxIdleTime,
	}

	// Stream pool — isolated to prevent blocking XREAD from starving general pool
	streamOpts := &goredis.Options{
		Addr:            addr,
		Username:        username,
		Password:        password,
		DB:              0,
		PoolSize:        cfg.StreamPoolSize,
		DialTimeout:     cfg.ConnectTimeout,
		ReadTimeout:     cfg.SocketTimeout,
		WriteTimeout:    cfg.SocketTimeout,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: _defaultMinRetryBackoff,
		MaxRetryBackoff: _defaultMaxRetryBackoff,
		PoolTimeout:     _defaultPoolTimeout,
		ConnMaxLifetime: _defaultConnMaxLifetime,
		ConnMaxIdleTime: _defaultConnMaxIdleTime,
	}

	rdb := &Redis{}

	rdb.GeneralClient, err = connectWithRetry(ctx, generalOpts)
	if err != nil {
		return nil, fmt.Errorf("redis - New - general pool: %w", err)
	}

	rdb.StreamClient, err = connectWithRetry(ctx, streamOpts)
	if err != nil {
		rdb.GeneralClient.Close()
		return nil, fmt.Errorf("redis - New - stream pool: %w", err)
	}

	rdb.hub = NewStreamHub(rdb.StreamClient)
	rdb.initTime = time.Now()

	return rdb, nil
}

func connectWithRetry(ctx context.Context, opts *goredis.Options) (*goredis.Client, error) {
	var client *goredis.Client
	for attempt := range _defaultConnAttempts {
		client = goredis.NewClient(opts)
		pingCtx, cancel := context.WithTimeout(ctx, _defaultConnectTimeout)
		err := client.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return client, nil
		}
		client.Close()
		if attempt < _defaultConnAttempts-1 {
			time.Sleep(_defaultConnRetryInterval)
		}
	}
	return nil, fmt.Errorf("redis - connectWithRetry - exhausted %d attempts", _defaultConnAttempts)
}

// Close closes both connection pools and the hub. Safe to call multiple times.
func (r *Redis) Close() error {
	var errs []error
	if r.hub != nil {
		r.hub.Close()
	}
	if r.GeneralClient != nil {
		if err := r.GeneralClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("general client: %w", err))
		}
	}
	if r.StreamClient != nil {
		if err := r.StreamClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("stream client: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Hub returns the StreamHub for SSE fan-out.
func (r *Redis) Hub() *StreamHub {
	return r.hub
}

// =============================================================================
// Timeout-protected operation wrapper
// =============================================================================

// exec is a timeout wrapper.
// go-redis respects context cancellation natively, so no goroutine is needed.
func (r *Redis) exec(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	r.opCount.Add(1)
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := fn(timeoutCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.timeoutCnt.Add(1)
		} else {
			r.errorCnt.Add(1)
		}
	}
	return err
}

// execVal is a typed version of exec.
func execVal[T any](r *Redis, ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	r.opCount.Add(1)
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	val, err := fn(timeoutCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.timeoutCnt.Add(1)
		} else {
			r.errorCnt.Add(1)
		}
	}
	return val, err
}

// =============================================================================
// Basic operations with timeout protection
// =============================================================================

// Get returns the value of a key.
func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (string, error) {
		return r.GeneralClient.Get(ctx, key).Result()
	})
}

// Set sets a key with optional expiration.
func (r *Redis) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return r.exec(ctx, _defaultOpTimeout, func(ctx context.Context) error {
		return r.GeneralClient.Set(ctx, key, value, expiration).Err()
	})
}

// SetNX sets a key only if it does not exist.
func (r *Redis) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (bool, error) {
		return r.GeneralClient.SetNX(ctx, key, value, expiration).Result()
	})
}

// Del deletes one or more keys.
func (r *Redis) Del(ctx context.Context, keys ...string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.Del(ctx, keys...).Result()
	})
}

// DelMultiple deletes multiple keys using pipelining, falling back to individual deletes if the
// pipeline fails.
func (r *Redis) DelMultiple(ctx context.Context, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		pipe := r.GeneralClient.Pipeline()
		for _, key := range keys {
			pipe.Del(ctx, key)
		}
		cmds, err := pipe.Exec(ctx)
		if err != nil {
			// Fallback to individual deletes
			var total int64
			for _, key := range keys {
				n, e := r.GeneralClient.Del(ctx, key).Result()
				if e == nil {
					total += n
				}
			}
			return total, nil
		}
		var total int64
		for _, cmd := range cmds {
			if c, ok := cmd.(*goredis.IntCmd); ok {
				total += c.Val()
			}
		}
		return total, nil
	})
}

// Exists checks if a key exists.
func (r *Redis) Exists(ctx context.Context, keys ...string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.Exists(ctx, keys...).Result()
	})
}

// Expire sets a key's expiration.
func (r *Redis) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (bool, error) {
		return r.GeneralClient.Expire(ctx, key, expiration).Result()
	})
}

// TTL returns the remaining TTL of a key.
func (r *Redis) TTL(ctx context.Context, key string) (time.Duration, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (time.Duration, error) {
		return r.GeneralClient.TTL(ctx, key).Result()
	})
}

// Incr atomically increments a key.
func (r *Redis) Incr(ctx context.Context, key string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.Incr(ctx, key).Result()
	})
}

// Decr atomically decrements a key.
func (r *Redis) Decr(ctx context.Context, key string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.Decr(ctx, key).Result()
	})
}

// SAdd adds members to a set.
func (r *Redis) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.SAdd(ctx, key, members...).Result()
	})
}

// SRem removes members from a set.
func (r *Redis) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.SRem(ctx, key, members...).Result()
	})
}

// SMembers returns all members of a set.
func (r *Redis) SMembers(ctx context.Context, key string) ([]string, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) ([]string, error) {
		return r.GeneralClient.SMembers(ctx, key).Result()
	})
}

// SCard returns the cardinality of a set.
func (r *Redis) SCard(ctx context.Context, key string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.SCard(ctx, key).Result()
	})
}

// ZScore returns the score of a member in a sorted set.
func (r *Redis) ZScore(ctx context.Context, key, member string) (float64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (float64, error) {
		return r.GeneralClient.ZScore(ctx, key, member).Result()
	})
}

// ZRangeByScore returns members in a sorted set within a score range.
func (r *Redis) ZRangeByScore(ctx context.Context, key string, min, max string) ([]string, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) ([]string, error) {
		return r.GeneralClient.ZRangeByScore(ctx, key, &goredis.ZRangeBy{Min: min, Max: max}).Result()
	})
}

// ScanKeys scans keys matching a pattern with timeout protection.
// Returns up to 1000 keys to avoid unbounded scans.
func (r *Redis) ScanKeys(ctx context.Context, pattern string, count int) ([]string, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) ([]string, error) {
		var keys []string
		iter := r.GeneralClient.Scan(ctx, 0, pattern, int64(count)).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
			if len(keys) >= 1000 {
				break
			}
		}
		return keys, iter.Err()
	})
}

// LLen returns the length of a list.
func (r *Redis) LLen(ctx context.Context, key string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.LLen(ctx, key).Result()
	})
}

// LPush prepends values to a list.
func (r *Redis) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.LPush(ctx, key, values...).Result()
	})
}

// LRange returns a range of elements from a list.
func (r *Redis) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) ([]string, error) {
		return r.GeneralClient.LRange(ctx, key, start, stop).Result()
	})
}

// LTrim trims a list to the specified range.
func (r *Redis) LTrim(ctx context.Context, key string, start, stop int64) error {
	return r.exec(ctx, _defaultOpTimeout, func(ctx context.Context) error {
		return r.GeneralClient.LTrim(ctx, key, start, stop).Err()
	})
}

// =============================================================================
// Stream operations with timeout protection
// =============================================================================

// StreamAdd appends a message to a stream. Uses the GeneralClient (non-blocking).
func (r *Redis) StreamAdd(ctx context.Context, stream string, values map[string]any, maxLen int) (string, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) (string, error) {
		args := &goredis.XAddArgs{
			Stream: stream,
			Values: values,
		}
		if maxLen > 0 {
			args.MaxLen = int64(maxLen)
			args.Approx = true
		}
		return r.GeneralClient.XAdd(ctx, args).Result()
	})
}

// StreamRead reads from a stream. Uses StreamClient if blocking (prevents starvation),
// GeneralClient otherwise.
func (r *Redis) StreamRead(ctx context.Context, stream, lastID string, count int, block time.Duration) ([]goredis.XStream, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) ([]goredis.XStream, error) {
		args := &goredis.XReadArgs{
			Streams: []string{stream, lastID},
			Count:   int64(count),
			Block:   block,
		}

		if block > 0 {
			return r.StreamClient.XRead(ctx, args).Result()
		}
		return r.GeneralClient.XRead(ctx, args).Result()
	})
}

// StreamRange returns a range of entries from a stream.
func (r *Redis) StreamRange(ctx context.Context, stream, start, end string, count int) ([]goredis.XMessage, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) ([]goredis.XMessage, error) {
		return r.GeneralClient.XRange(ctx, stream, start, end).Result()
	})
}

// StreamLen returns the length of a stream.
func (r *Redis) StreamLen(ctx context.Context, stream string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.XLen(ctx, stream).Result()
	})
}

// StreamTrim trims a stream to the given max length.
func (r *Redis) StreamTrim(ctx context.Context, stream string, maxLen int64) (int64, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.XTrimMaxLen(ctx, stream, maxLen).Result()
	})
}

// StreamTrimMinID trims a stream keeping entries with IDs >= minID.
func (r *Redis) StreamTrimMinID(ctx context.Context, stream, minID string) (int64, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.XTrimMinID(ctx, stream, minID).Result()
	})
}

// StreamDelete deletes entries from a stream by ID.
func (r *Redis) StreamDelete(ctx context.Context, stream string, ids ...string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.XDel(ctx, stream, ids...).Result()
	})
}

// XReadGroup reads from a consumer group. Uses StreamClient if blocking.
func (r *Redis) XReadGroup(ctx context.Context, group, consumer string, streams []string, count int, block time.Duration) ([]goredis.XStream, error) {
	return execVal(r, ctx, _defaultStreamTimeout, func(ctx context.Context) ([]goredis.XStream, error) {
		args := &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  streams,
			Count:    int64(count),
			Block:    block,
		}
		if block > 0 {
			return r.StreamClient.XReadGroup(ctx, args).Result()
		}
		return r.GeneralClient.XReadGroup(ctx, args).Result()
	})
}

// XAck acknowledges a message in a consumer group.
func (r *Redis) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	return execVal(r, ctx, _defaultOpTimeout, func(ctx context.Context) (int64, error) {
		return r.GeneralClient.XAck(ctx, stream, group, ids...).Result()
	})
}

// VerifyStreamWritable verifies that a stream key is writable by writing a test entry
// and deleting it immediately.
func (r *Redis) VerifyStreamWritable(ctx context.Context, streamKey string) error {
	id, err := r.StreamAdd(ctx, streamKey, map[string]any{"_health_check": "true"}, 1)
	if err != nil {
		return fmt.Errorf("redis - VerifyStreamWritable - write: %w", err)
	}
	if id == "" {
		return fmt.Errorf("redis - VerifyStreamWritable - stream %s returned empty id", streamKey)
	}
	if _, err := r.StreamDelete(ctx, streamKey, id); err != nil {
		return fmt.Errorf("redis - VerifyStreamWritable - cleanup: %w", err)
	}
	return nil
}

// =============================================================================
// Agent run stop signals
// =============================================================================

const _stopSignalTTL = 300 * time.Second

// SetStopSignal marks an agent run for cancellation.
func (r *Redis) SetStopSignal(ctx context.Context, runID string) error {
	key := fmt.Sprintf("agent_run:%s:stop", runID)
	return r.Set(ctx, key, "1", _stopSignalTTL)
}

// CheckStopSignal returns true if the agent run has been cancelled.
func (r *Redis) CheckStopSignal(ctx context.Context, runID string) (bool, error) {
	key := fmt.Sprintf("agent_run:%s:stop", runID)
	val, err := r.Get(ctx, key)
	if err != nil {
		return false, nil
	}
	return val == "1", nil
}

// ClearStopSignal removes the stop signal for an agent run.
func (r *Redis) ClearStopSignal(ctx context.Context, runID string) error {
	_, err := r.Del(ctx, fmt.Sprintf("agent_run:%s:stop", runID))
	return err
}

// =============================================================================
// Health check
// =============================================================================

// HealthCheck pings both pools and returns diagnostics.
func (r *Redis) HealthCheck(ctx context.Context) HealthReport {
	start := time.Now()
	generalErr := r.GeneralClient.Ping(ctx).Err()
	generalLatency := time.Since(start).Milliseconds()

	streamErr := r.StreamClient.Ping(ctx).Err()

	status := "healthy"
	if generalErr != nil || streamErr != nil {
		status = "degraded"
	}

	var hubStats map[string]any
	if r.hub != nil {
		hubStats = r.hub.Stats()
	}

	return HealthReport{
		Status:         status,
		GeneralPing:    generalErr == nil,
		StreamPing:     streamErr == nil,
		GeneralLatency: generalLatency,
		GeneralPool:    poolStats(r.GeneralClient),
		StreamPool:     poolStats(r.StreamClient),
		Hub:            hubStats,
		OpCount:        r.opCount.Load(),
		TimeoutCount:   r.timeoutCnt.Load(),
		ErrorCount:     r.errorCnt.Load(),
		UptimeSeconds:  int(time.Since(r.initTime).Seconds()),
	}
}

func poolStats(client *goredis.Client) PoolStats {
	if client == nil {
		return PoolStats{}
	}
	pool := client.PoolStats()
	return PoolStats{
		Hits:      int32(pool.Hits),
		Misses:    int32(pool.Misses),
		Timeouts:  int32(pool.Timeouts),
		TotalConn: int32(pool.TotalConns),
		IdleConn:  int32(pool.IdleConns),
		StaleConn: int32(pool.StaleConns),
	}
}

// Stats returns the current client metrics.
func (r *Redis) Stats() ClientStats {
	return ClientStats{
		OpCount:      r.opCount.Load(),
		TimeoutCount: r.timeoutCnt.Load(),
		ErrorCount:   r.errorCnt.Load(),
	}
}
