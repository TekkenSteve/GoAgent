package conversation

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	internalredis "github.com/TekkenSteve/GoAgent/internal/pkg/redis"
)

const (
	defaultConversationStreamMaxLength  = 2000
	defaultConversationStreamExpiration = 24 * time.Hour
)

type redisConversationStream struct {
	rdb        *internalredis.Redis
	maxLength  int
	expiration time.Duration
}

func newRedisConversationStream(ctx context.Context, config Config) (*redisConversationStream, error) {
	redisURL := strings.TrimSpace(config.RedisURL)
	if redisURL == "" {
		return nil, nil
	}

	rdb, err := internalredis.New(ctx, redisURL)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: connect redis stream: %w", err)
	}

	maxLength := config.RedisStreamMaxLength
	if maxLength <= 0 {
		maxLength = defaultConversationStreamMaxLength
	}

	expiration := config.RedisStreamExpiration
	if expiration <= 0 {
		expiration = defaultConversationStreamExpiration
	}

	return &redisConversationStream{rdb: rdb, maxLength: maxLength, expiration: expiration}, nil
}

func (s *redisConversationStream) Publish(ctx context.Context, event *agentos.ConversationEvent, accountID, projectID string) error {
	key := conversationStreamKey(accountID, projectID, event.ThreadID)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("agentos conversation: encode redis event: %w", err)
	}

	_, err = s.rdb.StreamAdd(ctx, key, map[string]any{
		"data":     string(data),
		"event_id": event.EventID,
		"sequence": event.Sequence,
	}, s.maxLength)
	if err != nil {
		return fmt.Errorf("agentos conversation: publish redis event: %w", err)
	}

	if _, err := s.rdb.Expire(ctx, key, s.expiration); err != nil {
		return fmt.Errorf("agentos conversation: expire redis stream: %w", err)
	}

	return nil
}

func (s *redisConversationStream) Subscribe(ctx context.Context, scope agentos.ThreadStreamScope) (*conversationLiveSubscription, error) {
	key := conversationStreamKey(scope.AccountID, scope.ProjectID, scope.ThreadID)
	startID := "0-0"

	tailCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	entries, err := s.rdb.GeneralClient.XRevRangeN(tailCtx, key, "+", "-", 1).Result()
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: read redis stream tail: %w", err)
	}

	if len(entries) > 0 {
		startID = entries[0].ID
	}

	hubSub := s.rdb.Hub().Subscribe(key, startID)
	events := make(chan agentos.ConversationEvent, conversationEventBuffer)

	go func() {
		defer close(events)

		for entry := range hubSub.C {
			data := entry.Values["data"]

			var event agentos.ConversationEvent
			if data == "" || json.Unmarshal([]byte(data), &event) != nil {
				continue
			}

			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &conversationLiveSubscription{events: events, close: hubSub.Close}, nil
}

func (s *redisConversationStream) Close() error { return s.rdb.Close() }

type conversationLiveSubscription struct {
	events <-chan agentos.ConversationEvent
	close  func()
}

func (s *conversationLiveSubscription) Events() <-chan agentos.ConversationEvent { return s.events }
func (s *conversationLiveSubscription) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

func conversationStreamKey(accountID, projectID, threadID string) string {
	return "agentos:conversation:events:" + streamScopeComponent(accountID) + ":" + streamScopeComponent(projectID) + ":" + streamScopeComponent(threadID)
}

func streamScopeComponent(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
