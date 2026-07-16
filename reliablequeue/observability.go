package reliablequeue

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/toolbelts/forge/reliablequeue"

var (
	attrTopic       = attribute.Key("reliablequeue.topic")
	attrGroup       = attribute.Key("reliablequeue.group")
	attrRedelivered = attribute.Key("reliablequeue.redelivered")
	attrPermanent   = attribute.Key("reliablequeue.permanent")
)

// otelMetrics 使用全局 MeterProvider 上报可靠队列指标。
type otelMetrics struct {
	publishTotal       metric.Int64Counter
	publishError       metric.Int64Counter
	replicaWaitTimeout metric.Int64Counter
	consumeTotal       metric.Int64Counter
	handlerError       metric.Int64Counter
	pending            metric.Int64Gauge
	processingDuration metric.Float64Histogram
	dlqTotal           metric.Int64Counter
}

// NewOTelMetrics 创建基于全局 OpenTelemetry MeterProvider 的 Metrics。
func NewOTelMetrics() Metrics {
	meter := otel.GetMeterProvider().Meter(instrumentationName)
	publishTotal, _ := meter.Int64Counter(
		"reliablequeue.publish.total",
		metric.WithDescription("Number of messages accepted by Redis Streams."),
	)
	publishError, _ := meter.Int64Counter(
		"reliablequeue.publish.error",
		metric.WithDescription("Number of messages whose publish flow returned an error."),
	)
	replicaWaitTimeout, _ := meter.Int64Counter(
		"reliablequeue.replica_wait.timeout",
		metric.WithDescription("Number of publishes not confirmed by the required replicas."),
	)
	consumeTotal, _ := meter.Int64Counter(
		"reliablequeue.consume.total",
		metric.WithDescription("Number of reliablequeue handler invocations."),
	)
	handlerError, _ := meter.Int64Counter(
		"reliablequeue.handler.error",
		metric.WithDescription("Number of reliablequeue handler errors."),
	)
	pending, _ := meter.Int64Gauge(
		"reliablequeue.pending",
		metric.WithDescription("Current Redis Stream consumer-group pending count."),
	)
	processingDuration, _ := meter.Float64Histogram(
		"reliablequeue.processing.duration",
		metric.WithDescription("Reliablequeue handler duration in seconds."),
		metric.WithUnit("s"),
	)
	dlqTotal, _ := meter.Int64Counter(
		"reliablequeue.dlq.total",
		metric.WithDescription("Number of messages published to the reliablequeue dead-letter topic."),
	)
	return &otelMetrics{
		publishTotal: publishTotal, publishError: publishError,
		replicaWaitTimeout: replicaWaitTimeout, consumeTotal: consumeTotal,
		handlerError: handlerError, pending: pending,
		processingDuration: processingDuration, dlqTotal: dlqTotal,
	}
}

func (metrics *otelMetrics) PublishTotal(topic string) {
	metrics.publishTotal.Add(context.Background(), 1, metric.WithAttributes(attrTopic.String(topic)))
}

func (metrics *otelMetrics) PublishError(topic string) {
	metrics.publishError.Add(context.Background(), 1, metric.WithAttributes(attrTopic.String(topic)))
}

func (metrics *otelMetrics) ReplicaWaitTimeout(topic string) {
	metrics.replicaWaitTimeout.Add(context.Background(), 1, metric.WithAttributes(attrTopic.String(topic)))
}

func (metrics *otelMetrics) ConsumeTotal(topic string, redelivered bool) {
	metrics.consumeTotal.Add(context.Background(), 1, metric.WithAttributes(
		attrTopic.String(topic), attrRedelivered.Bool(redelivered),
	))
}

func (metrics *otelMetrics) HandlerError(topic string, permanent bool) {
	metrics.handlerError.Add(context.Background(), 1, metric.WithAttributes(
		attrTopic.String(topic), attrPermanent.Bool(permanent),
	))
}

func (metrics *otelMetrics) Pending(topic, group string, count int64) {
	metrics.pending.Record(context.Background(), count, metric.WithAttributes(
		attrTopic.String(topic), attrGroup.String(group),
	))
}

func (metrics *otelMetrics) ProcessingDuration(topic string, duration time.Duration) {
	metrics.processingDuration.Record(
		context.Background(), duration.Seconds(), metric.WithAttributes(attrTopic.String(topic)),
	)
}

func (metrics *otelMetrics) DlqTotal(topic string) {
	metrics.dlqTotal.Add(context.Background(), 1, metric.WithAttributes(attrTopic.String(topic)))
}
