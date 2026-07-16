package provider

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/reliablequeue"
)

const reliableQueueDefaultRedisName = "default"

// ReliableQueueProvider 从配置构造 Redis Streams 至少一次队列并管理其生命周期。
type ReliableQueueProvider struct {
	enabled         bool
	queue           *reliablequeue.Queue
	shutdownTimeout time.Duration
}

// Register 读取 reliablequeue 配置并注册 Queue；启用时依赖更早的 RedisProvider。
func (provider *ReliableQueueProvider) Register(ctx context.Context) error {
	viper := MustGetViper(ctx)
	provider.enabled = viper.GetBool("reliablequeue.enabled")
	if !provider.enabled {
		log.Ctx(ctx).Info().Str("provider", "reliablequeue").Msg("reliablequeue disabled, skip register")
		return nil
	}
	redisName := viper.GetString("reliablequeue.redis")
	if redisName == "" {
		redisName = reliableQueueDefaultRedisName
	}
	client := MustGetRedis(ctx, redisName)
	options := []reliablequeue.QueueOption{
		reliablequeue.WithBlockTimeout(viper.GetDuration("reliablequeue.block_timeout")),
		reliablequeue.WithClaimIdle(viper.GetDuration("reliablequeue.claim_idle")),
		reliablequeue.WithRecoveryInterval(viper.GetDuration("reliablequeue.recovery_interval")),
		reliablequeue.WithBatchSize(viper.GetInt("reliablequeue.batch_size")),
		reliablequeue.WithWaitReplicas(
			viper.GetInt("reliablequeue.wait_replicas"),
			viper.GetDuration("reliablequeue.wait_timeout"),
		),
		reliablequeue.WithDlqTopic(viper.GetString("reliablequeue.dlq_topic")),
	}
	if metricsInstrumentationEnabled(viper, observabilityComponentReliableQueue) {
		options = append(options, reliablequeue.WithMetrics(reliablequeue.NewOTelMetrics()))
	}
	queue, err := reliablequeue.New(client, viper.GetString("reliablequeue.key_prefix"), options...)
	if err != nil {
		return err
	}
	provider.queue = queue
	provider.shutdownTimeout = viper.GetDuration("reliablequeue.shutdown_timeout")
	ioc.MustInstance(ctx, queue)
	log.Ctx(ctx).Info().
		Str("provider", "reliablequeue").
		Str("redis", redisName).
		Str("key_prefix", viper.GetString("reliablequeue.key_prefix")).
		Int("wait_replicas", viper.GetInt("reliablequeue.wait_replicas")).
		Msg("reliablequeue registered")
	return nil
}

// Setup 不注册业务订阅；业务 Provider 在自己的 Setup 中调用 Queue.Subscribe。
func (*ReliableQueueProvider) Setup(context.Context) error { return nil }

// Serve 启动 Queue 并阻塞到应用上下文取消。
func (provider *ReliableQueueProvider) Serve(ctx context.Context) error {
	if !provider.enabled {
		<-ctx.Done()
		return nil
	}
	if err := provider.queue.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

// Shutdown 等待可靠队列停止；超时只记录警告，未确认消息继续保留在 Redis PEL。
func (provider *ReliableQueueProvider) Shutdown(ctx context.Context) error {
	if !provider.enabled {
		return nil
	}
	timeout := provider.shutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-provider.queue.Stop():
		log.Ctx(ctx).Info().Str("provider", "reliablequeue").Msg("reliablequeue stopped cleanly")
	case <-timer.C:
		log.Ctx(ctx).Warn().
			Str("provider", "reliablequeue").
			Dur("timeout", timeout).
			Msg("reliablequeue shutdown timed out")
	}
	return nil
}

// GetReliableQueue 从容器获取启用的 ReliableQueue。
func GetReliableQueue(ctx context.Context) (*reliablequeue.Queue, error) {
	return ioc.Get[*reliablequeue.Queue](ctx)
}

// MustGetReliableQueue 从容器获取 ReliableQueue，缺失时 panic。
func MustGetReliableQueue(ctx context.Context) *reliablequeue.Queue {
	return ioc.MustGet[*reliablequeue.Queue](ctx)
}
