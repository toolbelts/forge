package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

func TestRedisProviderRegistersOnlyUniversalClient(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := ioc.New().WithContext(context.Background())
	config := viper.New()
	config.Set("redis.default.addr", server.Addr())
	config.Set("trace.instrumentation.redis", true)
	config.Set("metrics.instrumentation.redis", true)
	ioc.FromContext(ctx).Instance(config)

	redisProvider := &RedisProvider{}
	if err := redisProvider.Setup(ctx); err != nil {
		t.Fatalf("setup redis provider: %v", err)
	}
	t.Cleanup(func() {
		if err := redisProvider.Shutdown(ctx); err != nil {
			t.Fatalf("shutdown redis provider: %v", err)
		}
	})

	client, err := GetRedis(ctx, "default")
	if err != nil {
		t.Fatalf("get universal client: %v", err)
	}
	if err := client.Set(ctx, "provider:test", "ok", 0).Err(); err != nil {
		t.Fatalf("use universal client: %v", err)
	}
	if _, err := ioc.FromContext(ctx).Get[*redis.Client]("default"); !errors.Is(err, ioc.ErrNotFound) {
		t.Fatalf("concrete redis client lookup error = %v", err)
	}
}

func TestReliableQueueProviderUsesNamedUniversalClient(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	ctx := ioc.New().WithContext(context.Background())
	config := viper.New()
	config.Set("reliablequeue.enabled", true)
	config.Set("reliablequeue.redis", "reliable")
	config.Set("reliablequeue.key_prefix", "provider:test:")
	ioc.FromContext(ctx).Instance(config)
	ioc.FromContext(ctx).Instance[redis.UniversalClient](client, "reliable")

	queueProvider := &ReliableQueueProvider{}
	if err := queueProvider.Setup(ctx); err != nil {
		t.Fatalf("setup reliablequeue provider: %v", err)
	}
	if _, err := GetReliableQueue(ctx); err != nil {
		t.Fatalf("get reliablequeue: %v", err)
	}
	select {
	case <-queueProvider.queue.Stop():
	case <-ctx.Done():
		t.Fatal("stop reliablequeue provider timed out")
	}
}
