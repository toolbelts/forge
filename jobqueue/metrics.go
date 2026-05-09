package jobqueue

// Metrics 用于上报 Publish 路径的运行期统计,业务方可注入自家实现接 prometheus / otel 等。
// 所有方法应保持快速且不 panic;实现需自行处理并发。
//
// jobqueue.New 默认用 NoopMetrics 不上报任何数据;欲接入 OTel 显式
// WithMetrics(jobqueue.NewOTelMetrics())。
type Metrics interface {
	// PublishTotal 一次成功入队 (LPUSH 返回正数) 触发,无论是否触发 LTRIM。
	PublishTotal(topic string)
	// PublishDropped LTRIM 裁剪掉了 n 条最老消息时触发 (n>0)。是核心告警信号。
	PublishDropped(topic string, n int64)
	// QueueLength Publish 完成后采样的 LIST 长度 (已应用 LTRIM 之后的值)。
	QueueLength(topic string, n int64)
}

// NoopMetrics 不上报任何指标的 Metrics 实现,是 jobqueue.New 的默认值,
// 业务也可显式 WithMetrics(NoopMetrics{}) 覆盖外部注入回到无指标状态。
type NoopMetrics struct{}

func (NoopMetrics) PublishTotal(string)          {}
func (NoopMetrics) PublishDropped(string, int64) {}
func (NoopMetrics) QueueLength(string, int64)    {}
