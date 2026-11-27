package provider

import (
	"cmp"
	"context"

	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/lock"
)

// lockDefaultRedisName 是分布式锁默认使用的 Redis 实例名,可通过 lock.redis 覆盖。
const lockDefaultRedisName = "default"

// LockProvider 基于 Redis 的分布式锁工厂。
//
// 依赖 RedisProvider:取客户端时会先把它拉起,两者的注册顺序不限。
// 不挂拦截器,只向容器注入 lock.Manager 供业务 handler 通过 MustGetLockManager 获取。
type LockProvider struct{}

// Setup 取出 Redis 客户端并构造 lock.Manager,把 Manager 注入容器。lock.enabled=false 时跳过。
func (p *LockProvider) Setup(ctx context.Context) error {
	v := MustGetViper(ctx)
	if !v.GetBool("lock.enabled") {
		log.Ctx(ctx).Info().Str("provider", "lock").Msg("lock disabled, skip")
		return nil
	}

	redisName := cmp.Or(v.GetString("lock.redis"), lockDefaultRedisName)

	rdb, err := GetRedis(ctx, redisName)
	if err != nil {
		return err
	}

	opts := make([]lock.Option, 0, 4)
	if prefix := v.GetString("lock.prefix"); prefix != "" {
		opts = append(opts, lock.WithPrefix(prefix))
	}
	if d := v.GetDuration("lock.ttl"); d > 0 {
		opts = append(opts, lock.WithTtl(d))
	}
	// GetInt 在键缺失时返回 0,会误关默认开启的重试,所以必须 IsSet 守卫。
	if v.IsSet("lock.retry") {
		opts = append(opts, lock.WithRetry(v.GetInt("lock.retry")))
	}
	if d := v.GetDuration("lock.retry_interval"); d > 0 {
		opts = append(opts, lock.WithRetryInterval(d))
	}

	lm, err := lock.NewManager(rdb, opts...)
	if err != nil {
		return err
	}
	ioc.FromContext(ctx).Instance(lm)

	log.Ctx(ctx).Info().
		Str("provider", "lock").
		Str("redis", redisName).
		Msg("lock manager registered")
	return nil
}

// MustGetLockManager 从容器获取分布式锁工厂,缺失时 panic。
// 业务方在需要互斥的 handler 中调 NewLocker(key) 拿到 *Locker 后再 Lock/Unlock。
func MustGetLockManager(ctx context.Context) lock.Manager {
	return ioc.FromContext(ctx).MustGetFrom[lock.Manager, *LockProvider]()
}

// GetLockManager 从容器获取分布式锁工厂。
func GetLockManager(ctx context.Context) (lock.Manager, error) {
	return ioc.FromContext(ctx).GetFrom[lock.Manager, *LockProvider]()
}
