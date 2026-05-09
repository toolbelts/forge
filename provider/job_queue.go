package provider

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/jobqueue"
)

const jobqueueDefaultRedisName = "default"

// JobQueueProvider 任务队列提供者:Register 阶段构造 *jobqueue.Queue 并注入容器,
// 业务方在自己的 Setup 中通过 MustGetJobQueue(ctx) 拿到队列再 Subscribe。
//
// 行为约定:
//   - jobqueue.enabled=false 时静默跳过(不向容器注入实例),业务方调用 MustGetJobQueue 会 panic,
//     这是有意为之 —— 关闭队列时不应允许业务方依赖它
//   - 依赖 RedisProvider,从容器按 jobqueue.redis (默认 "default") 取 redis 客户端
//   - jobqueue.max_len 设全局 LIST 长度上限 (0 = 不限);超限时 Publish 自动 LTRIM 丢最老消息,
//     被丢数量通过 OTel 指标 jobqueue.publish.dropped 上报。需要 per-topic 覆盖请业务方
//     自行 jobqueue.New(... WithTopicMaxLen(...))。
//   - 默认接 jobqueue.NewOTelMetrics();metrics.enabled=false 时全局 MeterProvider 是 noop,零开销
//   - Serve 仅在 enabled 时启动 worker 并阻塞到 ctx 取消
//   - Shutdown 通过 queue.Stop() 等待 worker 收尾,超时阈值 jobqueue.shutdown_timeout
type JobQueueProvider struct {
	enabled         bool
	queue           *jobqueue.Queue
	shutdownTimeout time.Duration
}

// Register 读 jobqueue.* 配置,构造 *jobqueue.Queue 并注入容器。enabled=false 静默跳过。
func (p *JobQueueProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("jobqueue.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "jobqueue").Msg("jobqueue disabled, skip register")
		return nil
	}

	redisName := v.GetString("jobqueue.redis")
	if redisName == "" {
		redisName = jobqueueDefaultRedisName
	}
	client := MustGetRedis(ctx, redisName)

	keyPrefix := v.GetString("jobqueue.key_prefix")
	p.shutdownTimeout = v.GetDuration("jobqueue.shutdown_timeout")
	maxLen := v.GetInt("jobqueue.max_len")

	// 默认接 OTel metrics:与全局 MeterProvider 联动,metrics.enabled=false 时是 noop,零开销。
	q, err := jobqueue.New(client, keyPrefix,
		jobqueue.WithDefaultMaxLen(maxLen),
		jobqueue.WithMetrics(jobqueue.NewOTelMetrics()),
	)
	if err != nil {
		return err
	}
	p.queue = q
	ioc.MustInstance(ctx, p.queue)

	log.Ctx(ctx).Info().
		Str("provider", "jobqueue").
		Str("redis", redisName).
		Str("key_prefix", keyPrefix).
		Int("max_len", maxLen).
		Dur("shutdown_timeout", p.shutdownTimeout).
		Msg("jobqueue registered")
	return nil
}

// Setup 无操作。业务方在自己的 Setup 里 MustGetJobQueue(ctx).Subscribe(...)。
func (p *JobQueueProvider) Setup(ctx context.Context) error {
	return nil
}

// Serve 启动 worker 并阻塞到 ctx 取消。enabled=false 时仅 wait,不参与消费。
func (p *JobQueueProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		<-ctx.Done()
		return nil
	}
	if err := p.queue.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

// Shutdown 等待 worker 退出。超过 shutdownTimeout 仍未结束则记录 Warn 后返回,
// 任务函数本身会通过 ctx.Done() 收到取消信号 —— Go 不能强杀 goroutine。
func (p *JobQueueProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	done := p.queue.Stop()
	timeout := p.shutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		log.Ctx(ctx).Info().Str("provider", "jobqueue").Msg("jobqueue stopped cleanly")
		return nil
	case <-timer.C:
		log.Ctx(ctx).Warn().
			Str("provider", "jobqueue").
			Dur("timeout", timeout).
			Msg("jobqueue shutdown timed out, workers still running in background")
		return nil
	}
}

// GetJobQueue 从容器获取 *jobqueue.Queue。
func GetJobQueue(ctx context.Context) (*jobqueue.Queue, error) {
	return ioc.Get[*jobqueue.Queue](ctx)
}

// MustGetJobQueue 从容器获取 *jobqueue.Queue,缺失时 panic
// (通常是 jobqueue.enabled=false 时被业务方误用)。
func MustGetJobQueue(ctx context.Context) *jobqueue.Queue {
	return ioc.MustGet[*jobqueue.Queue](ctx)
}
