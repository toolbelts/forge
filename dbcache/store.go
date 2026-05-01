package dbcache

import (
	"context"
	"time"
)

// Item 是 Store 中的存储单元。Cache 把序列化后的 V 字节放进 Value;
// 当 Loader 返回 ErrNotFound 时,Cache 写入 NotFound=true、Value 为空的 Item,
// 这样 Store 既能区分"key 不存在"与"已知 key 在数据源里也没有",从而支持负缓存。
type Item struct {
	Value    []byte
	NotFound bool
}

// Store 是字节级 KV 后端的统一抽象,Cache 不感知具体实现。
//
// 实现需保证并发安全;非线程安全的实现自行加锁。
//
// Get 的第二个返回值 hit 区分"未命中"和"命中但是空 Item"两种情况:
//   - hit=false: Store 中没有这个 key,Cache 应走 Loader 回源。
//   - hit=true:  Store 命中,返回的 Item 可能是真值或 NotFound 负缓存。
//
// Set/MSet 中的 ttl<=0 由实现决定:典型行为是采用 Store 自身默认或永不过期。
// MGet/MSet 默认实现可基于 Get/Set 循环完成,但建议有原生批量(Pipeline / 批量 LRU)的实现自行 override 以提速。
//
// Close 释放底层资源(如关闭后台 goroutine),被多次调用应安全幂等。
type Store interface {
	Get(ctx context.Context, key string) (item Item, hit bool, err error)
	Set(ctx context.Context, key string, item Item, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	MGet(ctx context.Context, keys []string) (map[string]Item, error)
	MSet(ctx context.Context, items map[string]Item, ttl time.Duration) error
	Close() error
}
