package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// luaUnlock 校验持锁 token 后再 DEL,防止 TTL 过期后误删别人持有的锁。
const luaUnlock = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`

var unlockScript = redis.NewScript(luaUnlock)

// Manager 分布式锁工厂,持有 Redis 客户端与默认配置。
type Manager struct {
	rdb redis.UniversalClient
	opt options
}

// NewManager 创建分布式锁工厂,rdb 为 nil 或 ttl 非正时返回错误。
func NewManager(rdb redis.UniversalClient, opts ...Option) (*Manager, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.ttl <= 0 {
		return nil, fmt.Errorf("%w: ttl must be positive", ErrInvalidOption)
	}
	return &Manager{rdb: rdb, opt: o}, nil
}

// NewLocker 根据 key 创建一个绑定的 Locker。
// 同一 Locker 可重复 Lock/Unlock,每次 Lock 内部生成新 token。
// 注意:Locker 非并发安全,多 goroutine 应各自调用 NewLocker 拿独立实例。
func (m *Manager) NewLocker(key string) *Locker {
	return &Locker{
		rdb:           m.rdb,
		rawKey:        key,
		fullKey:       m.opt.prefix + ":" + key,
		ttl:           m.opt.ttl,
		retry:         m.opt.retry,
		retryInterval: m.opt.retryInterval,
	}
}

// Locker 绑定单个 key 的分布式锁柄,非并发安全。
type Locker struct {
	rdb           redis.UniversalClient
	rawKey        string
	fullKey       string
	ttl           time.Duration
	retry         int
	retryInterval time.Duration
	token         string
}

// TryLock 单次 SETNX 加锁,失败立即返回。
//
// 返回值:
//   - nil: 加锁成功
//   - ErrLocked: key 已被其他持有者占用
//   - ErrEmptyKey: key 为空
//   - 其他 error: 网络或 Redis 错误,此时不保证未拿到锁,
//     业务应将该 key 视为可能被持有,等 TTL 过期后再重试。
func (l *Locker) TryLock(ctx context.Context) error {
	if l.rawKey == "" {
		return ErrEmptyKey
	}
	token := uuid.NewString()
	ok, err := l.rdb.SetNX(ctx, l.fullKey, token, l.ttl).Result()
	if err != nil {
		return fmt.Errorf("lock: setnx failed: %w", err)
	}
	if !ok {
		return ErrLocked
	}
	l.token = token
	return nil
}

// Lock 加锁,被占时按 Manager 配置的重试策略 sleep 后重试。
// 网络/Redis 错误不重试(可能已写入),立即返回包装错误。
//
// 返回值:
//   - nil: 加锁成功(可能在重试后)
//   - ErrLocked: 重试用尽仍被占
//   - ErrEmptyKey: key 为空
//   - ctx.Err(): 等待期间 ctx 被取消或超时
//   - 其他 error: 网络或 Redis 错误,立即返回不重试。
func (l *Locker) Lock(ctx context.Context) error {
	err := l.TryLock(ctx)
	if !errors.Is(err, ErrLocked) || l.retry <= 0 {
		return err
	}
	timer := time.NewTimer(l.retryInterval)
	defer timer.Stop()
	for i := 0; i < l.retry; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		err = l.TryLock(ctx)
		if !errors.Is(err, ErrLocked) {
			return err
		}
		timer.Reset(l.retryInterval)
	}
	return err
}

// Unlock 校验 token 后释放锁。
// token 不匹配(锁已过期或本实例未持锁)返回 ErrNotHeld,业务通常忽略即可。
func (l *Locker) Unlock(ctx context.Context) error {
	if l.token == "" {
		return ErrNotHeld
	}
	n, err := unlockScript.Run(ctx, l.rdb, []string{l.fullKey}, l.token).Int64()
	if err != nil {
		return fmt.Errorf("lock: unlock script failed: %w", err)
	}
	l.token = ""
	if n == 0 {
		return ErrNotHeld
	}
	return nil
}

// Key 返回写入 Redis 的完整 key(含 prefix)。
func (l *Locker) Key() string {
	return l.fullKey
}
