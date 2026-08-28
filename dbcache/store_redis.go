package dbcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// 协议头第一字节用于区分负缓存和正常字节。
// 这样 Redis 中存的是单条 Value,不需要再嵌一层 JSON。
const (
	redisFlagValue    byte = 0x00
	redisFlagNotFound byte = 0x01

	redisInvalidationVersion = 1
)

// redisInvalidationMessage 是 Redis Pub/Sub 上传输的失效协议。
// Keys 保留调用 Store.Delete 时的逻辑 key,订阅端可直接用它清理自己的 L1。
type redisInvalidationMessage struct {
	Version int      `json:"version"`
	Source  string   `json:"source"`
	Keys    []string `json:"keys"`
}

// redisStore 基于 go-redis 的 Store 实现。Codec 在 Cache 层,Store 只搬字节。
type redisStore struct {
	client              redis.UniversalClient
	keyPrefix           string
	invalidationChannel string
	instanceId          string
}

// RedisOption 调整 redisStore 行为。
type RedisOption func(*redisStore)

// WithRedisKeyPrefix 给所有 Redis key 加固定前缀(典型如 "app:cache:")。
// 多个 Store 共享同一 Redis 时务必区分前缀避免撞 key。
func WithRedisKeyPrefix(prefix string) RedisOption {
	return func(s *redisStore) {
		s.keyPrefix = prefix
	}
}

// WithRedisInvalidation 开启基于 Redis Pub/Sub 的删除广播。
// channel 必须在同一逻辑缓存的所有进程中保持一致;空白 channel 属于配置错误并 panic。
//
// Redis Pub/Sub 是 best-effort 通知:订阅端断线期间的消息不会补发,
// tiered Store 仍靠 L1 TTL 作为最终收敛兜底。
func WithRedisInvalidation(channel string) RedisOption {
	return func(s *redisStore) {
		channel = strings.TrimSpace(channel)
		if channel == "" {
			panic("dbcache: WithRedisInvalidation: empty channel")
		}
		s.invalidationChannel = channel
		s.instanceId = uuid.NewString()
	}
}

// NewRedisStore 创建 Redis Store。client 为 nil 直接返回 panic 等价的实现 ——
// 调用方不应在缺 client 时还要求 Store 工作,这是配置错误。
//
// 与 Memory Store 不同:Redis 必须配合 Codec 使用,Codec 由 Cache 持有,
// 这里只把 Cache 准备好的字节写入 Redis、读出来交给 Cache 反序列化。
func NewRedisStore(client redis.UniversalClient, opts ...RedisOption) Store {
	if client == nil {
		panic("dbcache: NewRedisStore: nil redis client")
	}
	s := &redisStore{client: client}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// k 拼接 keyPrefix。
func (s *redisStore) k(key string) string {
	if s.keyPrefix == "" {
		return key
	}
	return s.keyPrefix + key
}

// Get 读单条。redis.Nil 视为未命中。
func (s *redisStore) Get(ctx context.Context, key string) (Item, bool, error) {
	raw, err := s.client.Get(ctx, s.k(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Item{}, false, nil
	}
	if err != nil {
		return Item{}, false, err
	}
	return decodeRedisPayload(raw), true, nil
}

// Set 写单条。ttl<=0 视为不过期(KEEPTTL 行为不在此考虑)。
func (s *redisStore) Set(ctx context.Context, key string, item Item, ttl time.Duration) error {
	payload := encodeRedisPayload(item)
	if ttl <= 0 {
		return s.client.Set(ctx, s.k(key), payload, 0).Err()
	}
	return s.client.Set(ctx, s.k(key), payload, ttl).Err()
}

// Delete 批量 DEL。
func (s *redisStore) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = s.k(k)
	}
	if err := s.client.Del(ctx, full...).Err(); err != nil {
		return err
	}
	return s.publishInvalidation(ctx, keys)
}

// MGet 用 Pipeline 一次拿多条。未命中的 key 不出现在结果 map 中。
//
// 走 Pipeline 而非 MGET 命令的原因:cluster 部署下 MGET 要求 keys 同 slot,
// Pipeline 由 client 内部按 slot 分批。
func (s *redisStore) MGet(ctx context.Context, keys []string) (map[string]Item, error) {
	if len(keys) == 0 {
		return map[string]Item{}, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Get(ctx, s.k(k))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make(map[string]Item, len(keys))
	for i, c := range cmds {
		raw, err := c.Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[keys[i]] = decodeRedisPayload(raw)
	}
	return out, nil
}

// MSet 用 Pipeline 批量 SET,共享 ttl。空 items 直接返回。
func (s *redisStore) MSet(ctx context.Context, items map[string]Item, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for k, item := range items {
		payload := encodeRedisPayload(item)
		if ttl <= 0 {
			pipe.Set(ctx, s.k(k), payload, 0)
		} else {
			pipe.Set(ctx, s.k(k), payload, ttl)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Close redis 客户端的生命周期由 provider 管理,这里不主动 Close。
func (s *redisStore) Close() error {
	return nil
}

// publishInvalidation 在 L2 删除成功后广播逻辑 key。发布失败返回给调用方,
// 这样上层可以重试 Delete;没有订阅者不是错误,Redis Publish 仍返回 nil。
func (s *redisStore) publishInvalidation(ctx context.Context, keys []string) error {
	if s.invalidationChannel == "" || len(keys) == 0 {
		return nil
	}
	payload, err := json.Marshal(redisInvalidationMessage{
		Version: redisInvalidationVersion,
		Source:  s.instanceId,
		Keys:    keys,
	})
	if err != nil {
		return fmt.Errorf("dbcache: marshal invalidation: %w", err)
	}
	if err := s.client.Publish(ctx, s.invalidationChannel, payload).Err(); err != nil {
		return fmt.Errorf("dbcache: publish invalidation: %w", err)
	}
	return nil
}

// subscribeInvalidation 为一个 TieredStore 建立独立订阅并返回幂等停止函数。
// 未开启失效广播时返回 nil,让 NewTieredStore 保持原有零后台协程行为。
func (s *redisStore) subscribeInvalidation(handler func(context.Context, []string) error) func() error {
	if s.invalidationChannel == "" || handler == nil {
		return nil
	}

	pubsub := s.client.Subscribe(context.Background(), s.invalidationChannel)
	messages := pubsub.Channel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range messages {
			var event redisInvalidationMessage
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Warn().Err(err).
					Str("channel", s.invalidationChannel).
					Msg("dbcache: ignore malformed invalidation message")
				continue
			}
			if event.Version != redisInvalidationVersion || event.Source == "" || len(event.Keys) == 0 {
				log.Warn().
					Str("channel", s.invalidationChannel).
					Int("version", event.Version).
					Msg("dbcache: ignore invalid invalidation message")
				continue
			}
			if event.Source == s.instanceId {
				continue
			}
			if err := handler(context.Background(), event.Keys); err != nil {
				log.Error().Err(err).
					Str("channel", s.invalidationChannel).
					Int("keys", len(event.Keys)).
					Msg("dbcache: invalidate local cache failed")
			}
		}
	}()

	var (
		once    sync.Once
		stopErr error
	)
	return func() error {
		once.Do(func() {
			stopErr = pubsub.Close()
			<-done
		})
		return stopErr
	}
}

// encodeRedisPayload 把 Item 编为单条字节:首字节为标志位,余下为 Value。
func encodeRedisPayload(item Item) []byte {
	if item.NotFound {
		return []byte{redisFlagNotFound}
	}
	out := make([]byte, 0, len(item.Value)+1)
	out = append(out, redisFlagValue)
	out = append(out, item.Value...)
	return out
}

// decodeRedisPayload 解析 encodeRedisPayload 写入的字节;空字节或未知标志位按值类型处理。
func decodeRedisPayload(raw []byte) Item {
	if len(raw) == 0 {
		return Item{}
	}
	switch raw[0] {
	case redisFlagNotFound:
		return Item{NotFound: true}
	case redisFlagValue:
		return Item{Value: raw[1:]}
	default:
		// 老数据兼容:无标志位时整段视为 Value。
		return Item{Value: raw}
	}
}
