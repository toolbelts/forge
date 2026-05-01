package dbcache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// 协议头第一字节用于区分负缓存和正常字节。
// 这样 Redis 中存的是单条 Value,不需要再嵌一层 JSON。
const (
	redisFlagValue    byte = 0x00
	redisFlagNotFound byte = 0x01
)

// redisStore 基于 go-redis 的 Store 实现。Codec 在 Cache 层,Store 只搬字节。
type redisStore struct {
	client    redis.UniversalClient
	keyPrefix string
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
	return s.client.Del(ctx, full...).Err()
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
