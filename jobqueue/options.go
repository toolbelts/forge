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
