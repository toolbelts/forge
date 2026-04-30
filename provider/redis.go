package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// RedisProvider Redis 提供者，支持从配置加载多个 Redis 实例。
type RedisProvider struct {
	clients map[string]*redis.Client
}

// Register 扫描 redis.* 配置创建多个客户端，连通性验证后绑定到容器。
func (p *RedisProvider) Register(ctx context.Context) error {
	v := ioc.MustGet[*viper.Viper](ctx)
	redisMap := v.GetStringMap("redis")
	p.clients = make(map[string]*redis.Client, len(redisMap))

	for name := range redisMap {
		prefix := "redis." + name
		opts := &redis.Options{
			Addr:     v.GetString(prefix + ".addr"),
			Password: v.GetString(prefix + ".password"),
			DB:       v.GetInt(prefix + ".db"),
		}
		if n := v.GetInt(prefix + ".pool_size"); n > 0 {
			opts.PoolSize = n
		}
		if n := v.GetInt(prefix + ".min_idle_conns"); n > 0 {
			opts.MinIdleConns = n
		}
		if n := v.GetInt(prefix + ".max_idle_conns"); n > 0 {
			opts.MaxIdleConns = n
		}
		if d := v.GetDuration(prefix + ".dial_timeout"); d > 0 {
			opts.DialTimeout = d
		}
		if d := v.GetDuration(prefix + ".read_timeout"); d > 0 {
			opts.ReadTimeout = d
		}
		if d := v.GetDuration(prefix + ".write_timeout"); d > 0 {
			opts.WriteTimeout = d
		}
		if d := v.GetDuration(prefix + ".pool_timeout"); d > 0 {
			opts.PoolTimeout = d
		}

		client := redis.NewClient(opts)
		if err := redisotel.InstrumentTracing(client); err != nil {
			return fmt.Errorf("redis [%s] instrument tracing failed: %w", name, err)
		}
		if err := redisotel.InstrumentMetrics(client); err != nil {
			return fmt.Errorf("redis [%s] instrument metrics failed: %w", name, err)
		}
		if err := client.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis [%s] ping failed: %w", name, err)
		}

		ioc.MustInstanceNamed(ctx, name, client)
		p.clients[name] = client
		log.Ctx(ctx).Info().
			Str("redis_name", name).
			Str("addr", opts.Addr).
			Msg("redis connected")
	}

	return nil
}

// Setup 无操作。
func (p *RedisProvider) Setup(ctx context.Context) error {
	return nil
}

// Shutdown 关闭所有 Redis 客户端，错误用 errors.Join 聚合返回。
func (p *RedisProvider) Shutdown(ctx context.Context) error {
	var errs []error
	for name, client := range p.clients {
		if err := client.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("redis_name", name).Msg("redis close failed")
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// GetRedis 从容器获取指定名称的 Redis 客户端。
func GetRedis(ctx context.Context, name string) (*redis.Client, error) {
	return ioc.GetNamed[*redis.Client](ctx, name)
}

// MustGetRedis 从容器获取指定名称的 Redis 客户端，缺失时 panic。
func MustGetRedis(ctx context.Context, name string) *redis.Client {
	return ioc.MustGetNamed[*redis.Client](ctx, name)
}
