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

const luaLock = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return redis.call('INCR', KEYS[2])
end
return 0
`

const luaRenew = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`

var (
	lockScript   = redis.NewScript(luaLock)
	unlockScript = redis.NewScript(luaUnlock)
	renewScript  = redis.NewScript(luaRenew)
)

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
		fenceKey:      m.opt.prefix + ":fence:" + key,
		ttl:           m.opt.ttl,
		retry:         m.opt.retry,
		retryInterval: m.opt.retryInterval,
	}
}

// Run 在持有分布式锁期间执行 fn，并按 ttl/3 自动续租。
// 续租失败时会取消传给 fn 的 context；fn 必须响应 ctx.Done() 才能及时退出。
// 返回值聚合 fn、续租与解锁错误。fencing token 通过 Locker.Fence() 读取。
// 即便 fn panic，defer 仍会取消续租 goroutine 并尝试解锁，让 panic 继续向上传播。
func (m *Manager) Run(ctx context.Context, key string, fn func(context.Context, *Locker) error) (err error) {
	if fn == nil {
		return fmt.Errorf("%w: run fn is nil", ErrInvalidOption)
	}

	locker := m.NewLocker(key)
	if err := locker.Lock(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	renewDone := make(chan error, 1)
	go locker.renewLoop(runCtx, cancel, renewDone)

	defer func() {
		cancel()
		renewErr := <-renewDone
		unlockErr := locker.Unlock(context.WithoutCancel(ctx))
		err = errors.Join(err, renewErr, unlockErr)
	}()

	return fn(runCtx, locker)
}

// Locker 绑定单个 key 的分布式锁柄,非并发安全。
type Locker struct {
	rdb           redis.UniversalClient
	rawKey        string
	fullKey       string
	fenceKey      string
	ttl           time.Duration
	retry         int
	retryInterval time.Duration
	token         string
	fence         int64
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
	fence, err := lockScript.Run(ctx, l.rdb, []string{l.fullKey, l.fenceKey}, token, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("lock: acquire script failed: %w", err)
	}
	if fence == 0 {
		return ErrLocked
	}
	l.token = token
	l.fence = fence
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
	for range l.retry {
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
	l.fence = 0
	if n == 0 {
		return ErrNotHeld
	}
	return nil
}

// Renew 校验持锁 token 后刷新 TTL。锁已过期或不再属于当前 Locker 时返回 ErrNotHeld。
func (l *Locker) Renew(ctx context.Context) error {
	if l.token == "" {
		return ErrNotHeld
	}
	n, err := renewScript.Run(ctx, l.rdb, []string{l.fullKey}, l.token, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("lock: renew script failed: %w", err)
	}
	if n == 0 {
		return ErrNotHeld
	}
	return nil
}

// Fence 返回本次成功加锁获得的 fencing token。未持锁时返回 0。
func (l *Locker) Fence() int64 {
	return l.fence
}

// Key 返回写入 Redis 的完整 key(含 prefix)。
func (l *Locker) Key() string {
	return l.fullKey
}

func (l *Locker) renewLoop(ctx context.Context, cancel context.CancelFunc, done chan<- error) {
	interval := l.ttl / 3
	if interval <= 0 {
		interval = l.ttl
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := l.Renew(ctx); err != nil {
				if ctx.Err() != nil {
					done <- nil
					return
				}
				cancel()
				done <- err
				return
			}
		}
	}
}
