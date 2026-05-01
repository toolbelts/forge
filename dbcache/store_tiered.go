package dbcache

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/rs/zerolog/log"
)

// tieredStore 由两个 Store 组合而成:l1 在前(进程内,典型 Memory),l2 在后(共享,典型 Redis)。
//
// 行为:
//   - Get: l1 命中直接返;l1 miss + l2 命中则回填 l1;都 miss 由上层 Cache 走 Loader。
//   - Set: 同时写 l1 + l2,l2 写失败仅 warn,本进程优先可用。
//   - Delete: 同时清 l1 + l2,任一错聚合返回。
//   - MGet/MSet: 同样的"l1 优先,l2 补漏"思路,只对 l1 漏的 key 去 l2 拉。
//
// 注意:不做跨进程广播 —— 其它进程的 l1 通过 TTL 自然收敛,
// l2 是共享的,所以其它进程下次访问 l2 会拿到新值或感知缺失。
type tieredStore struct {
	l1 Store
	l2 Store
}

// NewTieredStore 组合两层 Store。任一为 nil 直接 panic(配置错误)。
func NewTieredStore(l1, l2 Store) Store {
	if l1 == nil || l2 == nil {
		panic("dbcache: NewTieredStore: nil l1 or l2")
	}
	return &tieredStore{l1: l1, l2: l2}
}

// Get 先 l1 后 l2,l2 命中后回填 l1(用透传的 ttl 不可知,这里用一个保守的较短 ttl,
// 由上层 Cache.Set 的真实 ttl 才决定最终 TTL —— 此处只是临时让本进程后续读不再穿透 l2)。
//
// 选择"用 60s 兜底"而不是无 TTL 的原因:tiered 回填时不知道 l2 那条数据原本的剩余 TTL,
// 给本地一个短 TTL 保证最差情况下也能在 60s 内重新和 l2 对齐。
const tieredRefillTtl = 60 * time.Second

func (s *tieredStore) Get(ctx context.Context, key string) (Item, bool, error) {
	if item, hit, err := s.l1.Get(ctx, key); err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("key", key).Msg("dbcache: tiered l1 get failed, fall back to l2")
	} else if hit {
		return item, true, nil
	}

	item, hit, err := s.l2.Get(ctx, key)
	if err != nil {
		return Item{}, false, err
	}
	if !hit {
		return Item{}, false, nil
	}
	if setErr := s.l1.Set(ctx, key, item, tieredRefillTtl); setErr != nil {
		log.Ctx(ctx).Warn().Err(setErr).Str("key", key).Msg("dbcache: tiered l1 refill failed")
	}
	return item, true, nil
}

// Set 双写,l2 失败仅 warn 不阻塞 —— 本进程仍然可以从 l1 读到数据。
func (s *tieredStore) Set(ctx context.Context, key string, item Item, ttl time.Duration) error {
	if err := s.l1.Set(ctx, key, item, ttl); err != nil {
		// l1 写失败是反常的(典型 LRU 不会出错),直接返
		return err
	}
	if err := s.l2.Set(ctx, key, item, ttl); err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("key", key).Msg("dbcache: tiered l2 set failed (l1 ok)")
	}
	return nil
}

// Delete 双清,errors.Join 聚合返回。
func (s *tieredStore) Delete(ctx context.Context, keys ...string) error {
	err1 := s.l1.Delete(ctx, keys...)
	err2 := s.l2.Delete(ctx, keys...)
	return errors.Join(err1, err2)
}

// MGet 先批量读 l1,缺的去 l2 拉,拉到的回填 l1。
func (s *tieredStore) MGet(ctx context.Context, keys []string) (map[string]Item, error) {
	if len(keys) == 0 {
		return map[string]Item{}, nil
	}
	out, err := s.l1.MGet(ctx, keys)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("dbcache: tiered l1 mget failed, fall back to l2 only")
		out = map[string]Item{}
	}

	missing := make([]string, 0, len(keys)-len(out))
	for _, k := range keys {
		if _, ok := out[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return out, nil
	}

	l2Hit, err := s.l2.MGet(ctx, missing)
	if err != nil {
		return out, err
	}
	if len(l2Hit) > 0 {
		if setErr := s.l1.MSet(ctx, l2Hit, tieredRefillTtl); setErr != nil {
			log.Ctx(ctx).Warn().Err(setErr).Msg("dbcache: tiered l1 refill (mset) failed")
		}
		maps.Copy(out, l2Hit)
	}
	return out, nil
}

// MSet 双写,l2 失败仅 warn。
func (s *tieredStore) MSet(ctx context.Context, items map[string]Item, ttl time.Duration) error {
	if err := s.l1.MSet(ctx, items, ttl); err != nil {
		return err
	}
	if err := s.l2.MSet(ctx, items, ttl); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("dbcache: tiered l2 mset failed (l1 ok)")
	}
	return nil
}

// Close 关闭两层(顺序无关紧要)。
func (s *tieredStore) Close() error {
	return errors.Join(s.l1.Close(), s.l2.Close())
}
