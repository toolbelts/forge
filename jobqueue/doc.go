// Package jobqueue 提供基于 Redis LIST + BRPOP/BLMPOP 的极简任务队列:
//
//   - Publish(ctx, topic, args...) 把参数 JSON 化后 LPUSH 到 "{prefix}{topic}"
//   - Subscribe(topic, fn) 通过反射拿到 fn 的参数类型,后台 worker BRPOP 阻塞拉取并按类型解码后调用
//   - WithBatch(n) 可切到 BLMPOP COUNT n 一次拉多条,适合吞吐优先、单条要求不严的场景
//   - WithConcurrency(n) 在同 topic 上起多个 worker 抢同一个 key
//
// 投递语义为 at-most-once:worker 进程崩溃可能丢失正在处理的消息,业务对此应有预期;
// handler 内 panic 由 recover 兜住、返回 err 仅打 Error 日志,均不会重新入队也不进死信。
//
// 容量保护:WithDefaultMaxLen(n) / WithTopicMaxLen(topic, n) 给 LIST 设上限。
// Publish 在 LPUSH 后通过 LTRIM 原子裁掉最老的消息(默认 0 = 不限),Publish 仍返回 nil。
// 触发裁剪时一次裁到 max/2 (high/low watermark),避免稳态下每次 Publish 都触发裁剪
// 与 warn 日志洪水。被丢弃的数量通过 Metrics.PublishDropped 上报,业务应据此配阈值告警
// —— 这是消费者宕机或处理跟不上的核心信号。OTel 接入显式
// jobqueue.WithMetrics(jobqueue.NewOTelMetrics())。
//
// 合并模式:对单条无关紧要、关心吞吐的 topic 用
// WithTopicCoalesce(topic, window, maxBatch) 切到内存缓冲 + 批量 LPUSH。
// Redis 最终结果与 N 次 Publish 等价 (顺序保留、LTRIM high/low watermark 仍逐条生效),
// 消费端 handler 形态不变。合并模式下 Publish 只表示 payload 已进入内存缓冲;
// flush 失败仅记日志和 PublishDropped(..., DropReasonFlushFailed) 指标,进程崩溃会丢缓冲。
// 重要事件(支付、状态扣减)勿用,典型适用:在线状态、心跳、埋点。
//
// fn 必须形如 func(ctx context.Context, ...) error —— 第一个参数固定 context.Context,
// 出参恰好 1 个 error,其它入参类型任意。Publish 必须按相同顺序传值。
package jobqueue
