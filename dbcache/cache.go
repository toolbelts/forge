package dbcache

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

// LoaderFunc 单 key 回源:miss 时由 Cache 调用从数据源(典型 DB)拿值。
//
// 约定:
//   - 数据源中不存在该 key:返回 (zero, ErrNotFound),Cache 写负缓存(短 TTL)防穿透。
//   - 临时错误(连接断、超时等):返回 (zero, err),Cache 不写缓存,原样把错抛给调用方。
//   - 成功:返回 (v, nil),Cache 序列化后写入 Store。
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)

// BatchLoaderFunc 批量回源,MGet 调用。返回的 map 只包含命中的 key;
// 未在返回 map 中出现的 key 一律视为"数据源里也没有",Cache 写入对应负缓存。
type BatchLoaderFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// Cache 数据库缓存。生命周期:New / NewBun → Get / MGet / Set / Delete / Warm → Close。
//
// Cache 是并发安全的;singleflight 保证同 key 在 miss 期间只触发一次 Loader。
type Cache[K comparable, V any] struct {
	name        string // 类型名,用于 metrics 标签
	store       Store
	loader      LoaderFunc[K, V]
	batchLoader BatchLoaderFunc[K, V] // 可能为 nil,nil 时 MGet 走逐 key 并发 + singleflight
	ttl         time.Duration
	negTtl      time.Duration
	jitter      float64
	codec       Codec
	keyPrefix   string
	logger      zerolog.Logger
	metrics     Metrics

	sf     singleflight.Group
	closed atomic.Bool
}

// New 通用入口:自定义 Loader。loader 为 nil 直接 panic(配置错误)。
//
// MGet 优化:通过 WithBatchLoader[K, V] 注入 BatchLoaderFunc,可一次 SQL 拉多条。
// 不传时,MGet 在缺失多个 key 时退化为多 goroutine 并发 + singleflight 去重。
func New[K comparable, V any](loader LoaderFunc[K, V], opts ...Option) *Cache[K, V] {
	if loader == nil {
		panic(ErrNilLoader)
	}

	o := defaultOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.store == nil {
		o.store = NewMemoryStore(defaultMemSize)
	}

	var bl BatchLoaderFunc[K, V]
	if o.batchLoader != nil {
		typed, ok := o.batchLoader.(BatchLoaderFunc[K, V])
		if !ok {
			// 类型不匹配通常是用户调 WithBatchLoader 时类型参数和 New 不一致,
			// panic 比静默忽略好——这是配置错误,早暴露早修。
			panic(fmt.Sprintf("dbcache: batch loader type mismatch, want %T", bl))
		}
		bl = typed
	}

	var zero V
	name := reflect.TypeOf(zero).String()

	return &Cache[K, V]{
		name:        name,
		store:       o.store,
		loader:      loader,
		batchLoader: bl,
		ttl:         o.ttl,
		negTtl:      o.negativeTtl,
		jitter:      o.jitter,
		codec:       o.codec,
		keyPrefix:   o.keyPrefix,
		logger:      o.logger,
		metrics:     o.metrics,
	}
}

// Get 单 key 读取。命中(含负缓存)直接返回;miss 走 Loader 并写回 Store。
//
// 返回值:
//   - (v, nil):命中正向值或 Loader 取到值。
//   - (zero, ErrNotFound):命中负缓存,或 Loader 返回 ErrNotFound。
//   - (zero, err):Loader 返回其它错误,或 Cache 已 Close。
func (c *Cache[K, V]) Get(ctx context.Context, k K) (V, error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	sk := c.storeKey(k)

	if v, hit, fatal := c.tryStoreGet(ctx, sk); fatal == nil && hit {
		return v.value, v.err
	}
	return c.loadOne(ctx, k, sk)
}

// MGet 批量读取。返回 map 中只包含命中正向值的 key;
// 已知不存在(负缓存或 Loader 报 ErrNotFound)的 key 不出现在 map 中。
//
// 任意 Loader 报真错(非 ErrNotFound)会被聚合返回;此时已成功的 key 仍在 map 中。
func (c *Cache[K, V]) MGet(ctx context.Context, ks ...K) (map[K]V, error) {
	if len(ks) == 0 {
		return map[K]V{}, nil
	}
	if c.closed.Load() {
		return nil, ErrClosed
	}

	skList := make([]string, len(ks))
	for i, k := range ks {
		skList[i] = c.storeKey(k)
	}

	storeHit, err := c.store.MGet(ctx, skList)
	if err != nil {
		c.logger.Warn().Err(err).Msg("dbcache: store mget failed, fall back to per-key load")
		storeHit = map[string]Item{}
	}

	result := make(map[K]V, len(ks))
	var (
		missingKeys []K
		missingSKs  []string
	)
	for i, k := range ks {
		sk := skList[i]
		item, ok := storeHit[sk]
		if !ok {
			c.metrics.Miss(c.name)
			missingKeys = append(missingKeys, k)
			missingSKs = append(missingSKs, sk)
			continue
		}
		c.metrics.Hit(c.name)
		if item.NotFound {
			continue // 已知不存在,跳过
		}
		var v V
		if err := c.codec.Unmarshal(item.Value, &v); err != nil {
			c.logger.Warn().Err(err).Str("key", sk).Msg("dbcache: unmarshal cached item failed, refetching")
			_ = c.store.Delete(ctx, sk)
			missingKeys = append(missingKeys, k)
			missingSKs = append(missingSKs, sk)
			continue
		}
		result[k] = v
	}

	if len(missingKeys) == 0 {
		return result, nil
	}

	if c.batchLoader != nil {
		return c.loadBatch(ctx, missingKeys, missingSKs, result)
	}
	return c.loadEach(ctx, missingKeys, missingSKs, result)
}

// Set 主动写入(覆盖式),通常只在显式更新场景使用;
// 一般业务读路径靠 Get 自动回源即可,不需要手动 Set。
func (c *Cache[K, V]) Set(ctx context.Context, k K, v V) error {
	if c.closed.Load() {
		return ErrClosed
	}
	data, err := c.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("dbcache: marshal: %w", err)
	}
	return c.store.Set(ctx, c.storeKey(k), Item{Value: data}, c.jitterDuration(c.ttl))
}

// Delete 显式失效。同时清掉 singleflight 中飞行的 in-flight 项,
// 避免 DB 已更新但本进程仍读到旧 loader 结果。
func (c *Cache[K, V]) Delete(ctx context.Context, ks ...K) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if len(ks) == 0 {
		return nil
	}
	sks := make([]string, len(ks))
	for i, k := range ks {
		sks[i] = c.storeKey(k)
		c.sf.Forget(sks[i])
	}
	return c.store.Delete(ctx, sks...)
}

// Warm 预热:对一批 key 触发 MGet,把数据加进 Store。
// 不返回数据,仅返回错误用于诊断。
func (c *Cache[K, V]) Warm(ctx context.Context, ks []K) error {
	_, err := c.MGet(ctx, ks...)
	return err
}

// Close 关闭 Cache。后续读写返回 ErrClosed。多次调用安全幂等。
//
// 注意:Close 会调 store.Close;如果多个 Cache 共享同一 Store(典型 provider 场景),
// 不要在业务级 Cache 上调 Close,而是让 Store 由 provider Shutdown 统一管理。
func (c *Cache[K, V]) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	return c.store.Close()
}

// storeRead Get 读 Store 的中间结果(成功或负缓存)。
type storeRead[V any] struct {
	value V
	err   error // 命中负缓存时为 ErrNotFound
}

// tryStoreGet 尝试从 Store 取数据并反序列化。返回 (value, true, nil) 表示直接可返回上游;
// (_, false, _) 表示需要走 Loader;fatal 当前总是 nil(失败也走 loader)。
func (c *Cache[K, V]) tryStoreGet(ctx context.Context, sk string) (storeRead[V], bool, error) {
	var sr storeRead[V]

	item, hit, err := c.store.Get(ctx, sk)
	if err != nil {
		c.logger.Warn().Err(err).Str("key", sk).Msg("dbcache: store get failed, fall back to loader")
		c.metrics.Miss(c.name)
		return sr, false, nil
	}
	if !hit {
		c.metrics.Miss(c.name)
		return sr, false, nil
	}
	c.metrics.Hit(c.name)
	if item.NotFound {
		sr.err = ErrNotFound
		return sr, true, nil
	}

	var v V
	if err := c.codec.Unmarshal(item.Value, &v); err != nil {
		c.logger.Warn().Err(err).Str("key", sk).Msg("dbcache: unmarshal cached item failed, refetching")
		_ = c.store.Delete(ctx, sk)
		return sr, false, nil
	}
	sr.value = v
	return sr, true, nil
}

// loadOne 通过 singleflight 调 Loader,把结果写回 Store 并返回。
func (c *Cache[K, V]) loadOne(ctx context.Context, k K, sk string) (V, error) {
	var zero V
	val, err, _ := c.sf.Do(sk, func() (any, error) {
		start := time.Now()
		v, lerr := c.loader(ctx, k)
		c.metrics.LoadDuration(c.name, time.Since(start), lerr)

		if errors.Is(lerr, ErrNotFound) {
			if setErr := c.store.Set(ctx, sk, Item{NotFound: true}, c.jitterDuration(c.negTtl)); setErr != nil {
				c.logger.Warn().Err(setErr).Str("key", sk).Msg("dbcache: store set negative failed")
			}
			return zero, ErrNotFound
		}
		if lerr != nil {
			return zero, lerr
		}

		data, mErr := c.codec.Marshal(v)
		if mErr != nil {
			c.logger.Warn().Err(mErr).Str("key", sk).Msg("dbcache: marshal value failed, returning value without caching")
			return v, nil
		}
		if setErr := c.store.Set(ctx, sk, Item{Value: data}, c.jitterDuration(c.ttl)); setErr != nil {
			c.logger.Warn().Err(setErr).Str("key", sk).Msg("dbcache: store set failed")
		}
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	return val.(V), nil
}

// loadBatch 走 BatchLoader 一次性拉缺失 keys,把结果写入 into 并写回 Store。
func (c *Cache[K, V]) loadBatch(ctx context.Context, ks []K, sks []string, into map[K]V) (map[K]V, error) {
	start := time.Now()
	found, err := c.batchLoader(ctx, ks)
	c.metrics.LoadDuration(c.name, time.Since(start), err)
	if err != nil {
		return into, err
	}

	positive := make(map[string]Item, len(found))
	negative := make(map[string]Item, len(ks)-len(found))
	for i, k := range ks {
		sk := sks[i]
		if v, ok := found[k]; ok {
			data, mErr := c.codec.Marshal(v)
			if mErr != nil {
				c.logger.Warn().Err(mErr).Str("key", sk).Msg("dbcache: marshal value failed")
				continue
			}
			into[k] = v
			positive[sk] = Item{Value: data}
		} else {
			negative[sk] = Item{NotFound: true}
		}
	}

	if len(positive) > 0 {
		if setErr := c.store.MSet(ctx, positive, c.jitterDuration(c.ttl)); setErr != nil {
			c.logger.Warn().Err(setErr).Msg("dbcache: store mset positive failed")
		}
	}
	if len(negative) > 0 {
		if setErr := c.store.MSet(ctx, negative, c.jitterDuration(c.negTtl)); setErr != nil {
			c.logger.Warn().Err(setErr).Msg("dbcache: store mset negative failed")
		}
	}
	return into, nil
}

// loadEach 没 BatchLoader 时,逐 key 并发走 loadOne;singleflight 内部保证不会重复打 loader。
func (c *Cache[K, V]) loadEach(ctx context.Context, ks []K, sks []string, into map[K]V) (map[K]V, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
	)
	for i := range ks {
		wg.Add(1)
		go func(k K, sk string) {
			defer wg.Done()
			v, err := c.loadOne(ctx, k, sk)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					return
				}
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			into[k] = v
		}(ks[i], sks[i])
	}
	wg.Wait()
	return into, firstErr
}

// storeKey 把泛型 K 转成 Store 用的字符串 key,带 keyPrefix。
// 默认 fmt.Sprintf("%v") 即可:K 是 comparable,常见类型都有合理字符串表示。
// 复杂结构体作 K 时业务方应自行确保有稳定字符串表示(或加 String() 方法)。
func (c *Cache[K, V]) storeKey(k K) string {
	if c.keyPrefix == "" {
		return fmt.Sprintf("%v", k)
	}
	return c.keyPrefix + fmt.Sprintf("%v", k)
}

// jitterDuration 在 base 上叠加 ±jitter 比例的随机抖动。
// jitter==0 时直接返回 base。
func (c *Cache[K, V]) jitterDuration(base time.Duration) time.Duration {
	if c.jitter <= 0 || base <= 0 {
		return base
	}
	delta := float64(base) * c.jitter
	offset := delta * (rand.Float64()*2 - 1) // [-delta, +delta]
	return base + time.Duration(offset)
}
