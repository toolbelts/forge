package reliablequeue

import "time"

const (
	defaultKeyPrefix       = "reliablequeue:"
	defaultDlqTopic        = "dlq"
	defaultBlockTimeout    = 2 * time.Second
	defaultClaimIdle       = 30 * time.Second
	defaultRecoveryPeriod  = 5 * time.Second
	defaultBatchSize       = 32
	defaultWaitTimeout     = 2 * time.Second
	defaultHandlerParallel = 1
)

// QueueOption 调整 Queue 全局行为。
type QueueOption func(*Queue)

// SubscribeOption 调整单个 topic/group 的消费行为。
type SubscribeOption func(*subscription)

// WithBlockTimeout 设置 XREADGROUP 的阻塞时间；非正值保持默认值。
func WithBlockTimeout(timeout time.Duration) QueueOption {
	return func(queue *Queue) {
		if timeout > 0 {
			queue.blockTimeout = timeout
		}
	}
}

// WithClaimIdle 设置 Pending 消息可被 XAUTOCLAIM 认领前的最小空闲时间。
func WithClaimIdle(idle time.Duration) QueueOption {
	return func(queue *Queue) {
		if idle > 0 {
			queue.claimIdle = idle
		}
	}
}

// WithRecoveryInterval 设置扫描并恢复 Pending 消息的间隔。
func WithRecoveryInterval(interval time.Duration) QueueOption {
	return func(queue *Queue) {
		if interval > 0 {
			queue.recoveryInterval = interval
		}
	}
}

// WithBatchSize 设置每次读取或认领的最大消息数。
func WithBatchSize(size int) QueueOption {
	return func(queue *Queue) {
		if size > 0 {
			queue.batchSize = int64(size)
		}
	}
}

// WithWaitReplicas 设置关键发布需要 WAIT 确认的副本数和最长等待时间。
// replicas=0 关闭副本等待；启用时客户端必须支持 Conn 固定连接。
func WithWaitReplicas(replicas int, timeout time.Duration) QueueOption {
	return func(queue *Queue) {
		if replicas < 0 {
			return
		}
		queue.waitReplicas = replicas
		if timeout > 0 {
			queue.waitTimeout = timeout
		}
	}
}

// WithDlqTopic 设置 Permanent 错误使用的默认死信 topic。
func WithDlqTopic(topic string) QueueOption {
	return func(queue *Queue) {
		if topic != "" {
			queue.dlqTopic = topic
		}
	}
}

// WithMetrics 注入运行指标；nil 等价于 NoopMetrics。
func WithMetrics(metrics Metrics) QueueOption {
	return func(queue *Queue) {
		if metrics == nil {
			metrics = NoopMetrics{}
		}
		queue.metrics = metrics
	}
}

// WithConcurrency 设置单个订阅允许同时执行的 Handler 数量。
func WithConcurrency(concurrency int) SubscribeOption {
	return func(subscription *subscription) {
		if concurrency > 0 {
			subscription.concurrency = concurrency
		}
	}
}

// WithDeleteAfterAck 在确认消息后删除 Stream entry；仅适用于该 Stream 只有一个消费组。
func WithDeleteAfterAck(enabled bool) SubscribeOption {
	return func(subscription *subscription) {
		subscription.deleteAfterAck = enabled
	}
}

// WithSubscriptionDlqTopic 为单个订阅覆盖 Permanent 错误使用的死信 topic。
func WithSubscriptionDlqTopic(topic string) SubscribeOption {
	return func(subscription *subscription) {
		if topic != "" {
			subscription.dlqTopic = topic
		}
	}
}
