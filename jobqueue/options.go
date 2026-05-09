package jobqueue

// SubscribeOption 调整单个 topic 的消费策略。
type SubscribeOption func(*handler)

// WithConcurrency 设置同一 topic 上的并发 worker 数,n<=0 时按 1 处理。
// 多个 worker 抢同一个 Redis key,BRPOP 的原子性保证每条消息只被一个 worker 拿到。
//
// 注意:N 个 BRPOP worker 会各占一条 Redis 连接长期阻塞,
// 建议对应的 redis pool_size >= N+1 (留一条给 Publish/健康检查)。
func WithConcurrency(n int) SubscribeOption {
	return func(h *handler) {
		if n > 0 {
			h.concurrency = n
		}
	}
}

// WithBatch 切到 BLMPOP 模式,一次最多拉 n 条消息后顺序 dispatch。
// n<=1 等价于不开启 (默认走 BRPOP)。
//
// 适合"对单条延迟不敏感、关心总吞吐"的场景,典型如批量发邮件。
func WithBatch(n int) SubscribeOption {
	return func(h *handler) {
		if n > 1 {
			h.batch = n
		}
	}
}

// QueueOption 调整 Queue 全局行为,New 时传入。
type QueueOption func(*Queue)

// WithDefaultMaxLen 设置所有 topic 的默认 LIST 长度上限。0 表示不限 (默认)。
//
// 队列长度超过上限时,Publish 在 LPUSH 后通过 LTRIM 原子裁掉最老的消息,
// 这是为了防止消费者宕机/跟不上时 Redis 内存被撑爆。被丢弃的数量通过
// Metrics.PublishDropped 上报,业务应据此配阈值告警。
func WithDefaultMaxLen(n int) QueueOption {
	return func(q *Queue) {
		if n > 0 {
			q.defaultMaxLen = n
		}
	}
}

// WithTopicMaxLen 为单个 topic 覆盖 max_len,优先级高于 WithDefaultMaxLen。
// 多次调用同一 topic 后写覆盖前写。0 表示该 topic 不限 (即使全局有默认)。
func WithTopicMaxLen(topic string, n int) QueueOption {
	return func(q *Queue) {
		if topic == "" {
			return
		}
		if q.topicMaxLen == nil {
			q.topicMaxLen = make(map[string]int)
		}
		q.topicMaxLen[topic] = n
	}
}

// WithMetrics 注入 Metrics 实现,nil 等价于 NoopMetrics。
// 接 OTel 的标准用法:WithMetrics(jobqueue.NewOTelMetrics())。
func WithMetrics(m Metrics) QueueOption {
	return func(q *Queue) {
		if m == nil {
			m = NoopMetrics{}
		}
		q.metrics = m
	}
}
