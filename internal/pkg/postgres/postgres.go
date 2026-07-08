// Package postgres implements postgres connection.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errMaxPoolSizeExceedsMaxInt32 = errors.New("max pool size exceeds max int32")
	errMaxPoolSizeMustBePositive  = errors.New("max pool size must be positive")
)

const (
	_defaultMaxPoolSize  = 1
	_defaultConnAttempts = 10
	_defaultConnTimeout  = time.Second
)

// Postgres -.
type Postgres struct {
	maxPoolSize  int
	connAttempts int
	connTimeout  time.Duration

	Builder squirrel.StatementBuilderType
	Pool    *pgxpool.Pool
}

// New -.
func New(url string, opts ...Option) (*Postgres, error) {
	pg := &Postgres{
		maxPoolSize:  _defaultMaxPoolSize,
		connAttempts: _defaultConnAttempts,
		connTimeout:  _defaultConnTimeout,
	}

	// Custom options
	for _, opt := range opts {
		opt(pg)
	}

	pg.Builder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("postgres - NewPostgres - pgxpool.ParseConfig: %w", err)
	}

	maxConns, err := checkedMaxConns(pg.maxPoolSize)
	if err != nil {
		return nil, fmt.Errorf("postgres - NewPostgres: %w", err)
	}

	poolConfig.MaxConns = maxConns

	for pg.connAttempts > 0 {
		pg.Pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err == nil {
			break
		}

		log.Printf("Postgres is trying to connect, attempts left: %d", pg.connAttempts)

		time.Sleep(pg.connTimeout)

		pg.connAttempts--
	}

	if err != nil {
		return nil, fmt.Errorf("postgres - NewPostgres - connAttempts == 0: %w", err)
	}

	return pg, nil
}

func checkedMaxConns(maxPoolSize int) (int32, error) {
	if maxPoolSize <= 0 {
		return 0, fmt.Errorf("%w: %d", errMaxPoolSizeMustBePositive, maxPoolSize)
	}

	if maxPoolSize > math.MaxInt32 {
		return 0, fmt.Errorf("%w: %d", errMaxPoolSizeExceedsMaxInt32, maxPoolSize)
	}

	return int32(maxPoolSize), nil
}

// Close -.
func (p *Postgres) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}
