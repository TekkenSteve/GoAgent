package v1

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
	"github.com/go-playground/validator/v10"
)

// V1 -.
type V1 struct {
	t   usecase.AgentExecutor
	h   usecase.HistoryQuery
	s   usecase.StreamExecutor
	l   logger.Interface
	v   *validator.Validate
	rdb *redis.Redis

	// Event Sourcing components (Phase 2+)
	eventStore stream.EventStore
	subscriber stream.Subscriber
	gateway    stream.StatelessGateway
}
