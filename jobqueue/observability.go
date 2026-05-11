package jobqueue

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName 用作 OTel meter 的 name,与 module path 对齐便于 collector 侧定位。
const instrumentationName = "github.com/toolbelts/forge/jobqueue"

// metric / span 共享的 attribute key。集中放这里便于改命名。
var (
	attrTopic      = attribute.Key("jobqueue.topic")
	attrDropReason = attribute.Key("jobqueue.drop_reason")
)

// otelMetrics 用全局 otel.MeterProvider 实现 Metrics 接口。
// instrument 在构造时被绑定到当时的 MeterProvider,所以必须在 OTel 全局 setup
// 完成之后调用 NewOTelMetrics —— 业务一般在 Setup 阶段或之后才 jobqueue.New,
// 那时 MetricsProvider.Register 已跑过,符合此约束。
type otelMetrics struct {
	publishTotal      metric.Int64Counter
	publishDropped    metric.Int64Counter
	queueLen          metric.Int64Gauge
	coalesceFlushes   metric.Int64Counter
	coalesceBatchSize metric.Int64Histogram
}

// NewOTelMetrics 构造一个把 PublishTotal/PublishDropped/QueueLength 转译为 OTel 指标的 Metrics 实现。
// MetricsProvider 未启用时全局是 noop,所有调用零开销。
//
// 接入方式:jobqueue.New(client, prefix, jobqueue.WithMetrics(jobqueue.NewOTelMetrics()))。
func NewOTelMetrics() Metrics {
	meter := otel.GetMeterProvider().Meter(instrumentationName)
	publishTotal, _ := meter.Int64Counter(
		"jobqueue.publish.total",
		metric.WithDescription("Number of jobqueue Publish calls accepted; coalesced topics are counted when buffered."),
	)
	publishDropped, _ := meter.Int64Counter(
		"jobqueue.publish.dropped",
		metric.WithDescription("Number of jobqueue messages dropped, tagged by reason."),
	)
	queueLen, _ := meter.Int64Gauge(
		"jobqueue.queue.length",
		metric.WithDescription("Current jobqueue LIST length sampled at Publish time."),
	)
	coalesceFlushes, _ := meter.Int64Counter(
		"jobqueue.coalesce.flush.total",
		metric.WithDescription("Number of successful coalesced flushes."),
	)
	coalesceBatchSize, _ := meter.Int64Histogram(
		"jobqueue.coalesce.batch.size",
		metric.WithDescription("Batch size distribution for successful coalesced flushes."),
	)
	return &otelMetrics{
		publishTotal:      publishTotal,
		publishDropped:    publishDropped,
		queueLen:          queueLen,
		coalesceFlushes:   coalesceFlushes,
		coalesceBatchSize: coalesceBatchSize,
	}
}

func (m *otelMetrics) PublishTotal(topic string) {
	m.publishTotal.Add(context.Background(), 1, metric.WithAttributes(attrTopic.String(topic)))
}

func (m *otelMetrics) PublishDropped(topic string, n int64, reason DropReason) {
	m.publishDropped.Add(
		context.Background(),
		n,
		metric.WithAttributes(attrTopic.String(topic), attrDropReason.String(string(reason))),
	)
}

func (m *otelMetrics) QueueLength(topic string, n int64) {
	m.queueLen.Record(context.Background(), n, metric.WithAttributes(attrTopic.String(topic)))
}

func (m *otelMetrics) CoalesceFlushTotal(topic string) {
	m.coalesceFlushes.Add(context.Background(), 1, metric.WithAttributes(attrTopic.String(topic)))
}

func (m *otelMetrics) CoalesceBatchSize(topic string, n int64) {
	m.coalesceBatchSize.Record(context.Background(), n, metric.WithAttributes(attrTopic.String(topic)))
}
