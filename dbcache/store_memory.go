package dbcache

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// memoryStore 进程内 LRU,基于 hashicorp/golang-lru/v2/expirable。
//
// expirable.LRU 的 ttl 是统一的"全局 TTL",和我们 Store 接口要求"按 Set 时传入的 ttl"
// 不完全一致。这里采用约定:在 Item 上记一个绝对过期时刻,Store 自己按时刻判定;
// LRU 的 ttl 设成 0(不过期)由我们手动驱逐。这样可以做"逐 key 不同 TTL",
// 同时保留 LRU 容量驱逐能力。
type memoryStore struct {
	lru *expirable.LRU[string, memoryEntry]
}

type memoryEntry struct {
	item      Item
	expiresAt time.Time // 零值 = 永不过期
}

// NewMemoryStore 创建容量为 size 的 LRU Store。size<=0 时按 100000 处理。
//
// Memory Store 不需要 Codec —— 它直接持有字节级 Item;但 cache 仍可能跨 Store 写,
// 序列化策略由 Cache 统一处理,Memory 只是把 Item 原样存起来。
func NewMemoryStore(size int) Store {
	if size <= 0 {
		size = defaultMemSize
	}
	return &memoryStore{
		lru: expirable.NewLRU[string, memoryEntry](size, nil, 0),
	}
}

// Get 命中时返回 Item;过期 entry 主动删除并返回 hit=false。
func (m *memoryStore) Get(ctx context.Context, key string) (Item, bool, error) {
	e, ok := m.lru.Get(key)
	if !ok {
		return Item{}, false, nil
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		m.lru.Remove(key)
		return Item{}, false, nil
	}
	return e.item, true, nil
}

// Set 写入 entry,ttl<=0 视为永不过期(默认场景仍由 Cache 传入有效 ttl)。
func (m *memoryStore) Set(ctx context.Context, key string, item Item, ttl time.Duration) error {
	e := memoryEntry{item: item}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	m.lru.Add(key, e)
	return nil
}

// Delete 批量删除。
func (m *memoryStore) Delete(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		m.lru.Remove(k)
	}
	return nil
}

// MGet 循环 Get;Memory 后端没有原生批量优势,逐条够用。
func (m *memoryStore) MGet(ctx context.Context, keys []string) (map[string]Item, error) {
	out := make(map[string]Item, len(keys))
	for _, k := range keys {
		if item, hit, _ := m.Get(ctx, k); hit {
			out[k] = item
		}
	}
	return out, nil
}

// MSet 循环 Set,共享同一 ttl。
func (m *memoryStore) MSet(ctx context.Context, items map[string]Item, ttl time.Duration) error {
	for k, v := range items {
		_ = m.Set(ctx, k, v, ttl)
	}
	return nil
}

// Close 清空 LRU。
func (m *memoryStore) Close() error {
	m.lru.Purge()
	return nil
}
