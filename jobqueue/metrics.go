package jobqueue

// Metrics 用于上报 Publish 路径的运行期统计,业务方可注入自家实现接 prometheus / otel 等。
// 所有方法应保持快速且不 panic;实现需自行处理并发。
//
// jobqueue.New 默认用 NoopMetrics 不上报任何数据;欲接入 OTel 显式
// WithMetrics(jobqueue.NewOTelMetrics())。
type Metrics interface {
	// PublishTotal 一次 Publish 被接受后触发:同步模式表示 Redis 已 LPUSH 成功,
	// 合并模式表示 payload 已进入内存缓冲,还不代表已持久化到 Redis。
	PublishTotal(topic string)
	// PublishDropped 消息被丢弃时触发 (n>0),reason 区分 max_len 裁剪或合并 flush 失败。
	// 是核心告警信号。
	PublishDropped(topic string, n int64, reason DropReason)
	// QueueLength Publish 完成后采样的 LIST 长度 (已应用 LTRIM 之后的值)。
	// 合并模式下在 flush 完成时上报。
	QueueLength(topic string, n int64)
	// CoalesceFlushTotal 一次合并 flush 成功上报一次。
	CoalesceFlushTotal(topic string)
	// CoalesceBatchSize 一次合并 flush 成功后记录本批条数 (>=1)。
	CoalesceBatchSize(topic string, n int64)
}

// DropReason 标识消息丢弃原因,用于指标标签和告警分流。
type DropReason string

const (
	DropReasonOverflow    DropReason = "overflow"
	DropReasonFlushFailed DropReason = "flush_failed"
)

// NoopMetrics 不上报任何指标的 Metrics 实现,是 jobqueue.New 的默认值,
// 业务也可显式 WithMetrics(NoopMetrics{}) 覆盖外部注入回到无指标状态。
type NoopMetrics struct{}

func (NoopMetrics) PublishTotal(string)                      {}
func (NoopMetrics) PublishDropped(string, int64, DropReason) {}
func (NoopMetrics) QueueLength(string, int64)                {}
func (NoopMetrics) CoalesceFlushTotal(string)                {}
func (NoopMetrics) CoalesceBatchSize(string, int64)          {}
