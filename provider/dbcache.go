package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/dbcache"
	"github.com/toolbelts/forge/ioc"
)

// dbcacheDefaultRedisName 是 redis/tiered 后端默认引用的 redis 实例名,
// 与 LockProvider/JobQueueProvider 等保持一致(沿用 redis.default)。
const dbcacheDefaultRedisName = "default"

// DbcacheProvider 数据库缓存提供者。
//
// 编排约定:依赖 RedisProvider(redis / tiered 后端时);排在 RedisProvider 之后、业务 Provider 之前。
//
// 行为约定:
//   - 扫描 dbcache.<name> 配置,为每个 name 构造对应类型的 Store 并按 name 注入容器。
//     存储类型由 dbcache.<name>.store 决定:memory(默认) / redis / tiered。
//   - 不向容器注册 *Cache,Cache 是泛型类型由业务方在自己代码里 dbcache.NewBun 等构造,
//     这里只提供 Store 编排能力。
//   - 可观测性默认是 noop,业务想接 OTel 显式 dbcache.WithMetrics(dbcache.NewOTelMetrics())
//     / dbcache.WithTracer(dbcache.NewOTelTracer())。两个工厂内部走全局
//     otel.MeterProvider / TracerProvider,与 metrics.enabled / trace.enabled 联动,
//     未启用时是 noop。
type DbcacheProvider struct {
	stores map[string]dbcache.Store
}

// Register 扫 dbcache.* 配置,为每个 name 构造 Store 并以 dbcache.Store 类型注入容器。
func (p *DbcacheProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	cfgMap := v.GetStringMap("dbcache")
	p.stores = make(map[string]dbcache.Store, len(cfgMap))

	for name := range cfgMap {
		prefix := "dbcache." + name
		storeType := v.GetString(prefix + ".store")
		if storeType == "" {
			storeType = "memory"
		}

		store, err := buildDbcacheStore(ctx, v, prefix, storeType)
		if err != nil {
			return fmt.Errorf("dbcache [%s]: %w", name, err)
		}

		ioc.MustInstanceNamed(ctx, name, store)
		p.stores[name] = store

		log.Ctx(ctx).Info().
			Str("provider", "dbcache").
			Str("name", name).
			Str("store", storeType).
			Msg("dbcache store registered")
	}

	return nil
}

// Setup 无操作。Cache 由业务方自行构造。
func (p *DbcacheProvider) Setup(ctx context.Context) error {
	return nil
}

// Shutdown 关闭所有 Store(memory 仅清空,redis 不主动 Close —— Redis 客户端由 RedisProvider 管理)。
func (p *DbcacheProvider) Shutdown(ctx context.Context) error {
	var errs []error
	for name, s := range p.stores {
		if err := s.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("name", name).Msg("dbcache store close failed")
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// buildDbcacheStore 按配置类型构造对应的 Store。
func buildDbcacheStore(ctx context.Context, v *viper.Viper, prefix, storeType string) (dbcache.Store, error) {
	switch storeType {
	case "memory":
		return dbcache.NewMemoryStore(v.GetInt(prefix + ".size")), nil

	case "redis":
		client, keyPrefix, err := resolveDbcacheRedis(ctx, v, prefix)
		if err != nil {
			return nil, err
		}
		return dbcache.NewRedisStore(client, dbcache.WithRedisKeyPrefix(keyPrefix)), nil

	case "tiered":
		client, keyPrefix, err := resolveDbcacheRedis(ctx, v, prefix)
		if err != nil {
			return nil, err
		}
		l1 := dbcache.NewMemoryStore(v.GetInt(prefix + ".size"))
		l2 := dbcache.NewRedisStore(client, dbcache.WithRedisKeyPrefix(keyPrefix))
		return dbcache.NewTieredStore(l1, l2), nil

	default:
		return nil, fmt.Errorf("unknown store type %q", storeType)
	}
}

// resolveDbcacheRedis 抽出 redis/tiered 共用的"按 redis 名取 client + 读 key_prefix"逻辑。
func resolveDbcacheRedis(ctx context.Context, v *viper.Viper, prefix string) (redis.UniversalClient, string, error) {
	redisName := v.GetString(prefix + ".redis")
	if redisName == "" {
		redisName = dbcacheDefaultRedisName
	}
	rdb, err := GetRedis(ctx, redisName)
	if err != nil {
		return nil, "", fmt.Errorf("resolve redis %q: %w", redisName, err)
	}
	return rdb, v.GetString(prefix + ".key_prefix"), nil
}

// GetDbcacheStore 从容器获取指定名称的 dbcache Store。
func GetDbcacheStore(ctx context.Context, name string) (dbcache.Store, error) {
	return ioc.GetNamed[dbcache.Store](ctx, name)
}

// MustGetDbcacheStore 从容器获取指定名称的 dbcache Store,缺失时 panic。
func MustGetDbcacheStore(ctx context.Context, name string) dbcache.Store {
	return ioc.MustGetNamed[dbcache.Store](ctx, name)
}
