package dbcache

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// instrumentationName 用作 OTel meter / tracer 的 name,与 module path 对齐便于 collector 侧定位。
const instrumentationName = "github.com/toolbelts/forge/dbcache"

// span / metric 共享的 attribute key。集中放这里便于改命名。
var (
	attrCacheName   = attribute.Key("dbcache.name")
	attrStatus      = attribute.Key("dbcache.status")
	attrHit         = attribute.Key("dbcache.hit")
	attrNotFound    = attribute.Key("dbcache.notfound")
	attrKeysCount   = attribute.Key("dbcache.keys.count")
	attrHitsCount   = attribute.Key("dbcache.hits.count")
	attrMissesCount = attribute.Key("dbcache.misses.count")
)

// otelMetrics 用全局 otel.MeterProvider 实现 Metrics 接口。
// instrument 在构造时被绑定到当时的 MeterProvider,所以必须在 OTel 全局 setup
// 完成之后调用 NewOTelMetrics —— 业务一般在 Setup 阶段或之后才 dbcache.New,
// 那时 MetricsProvider.Register 已跑过,符合此约束。
type otelMetrics struct {
	hits     metric.Int64Counter
	misses   metric.Int64Counter
	duration metric.Float64Histogram
}

// NewOTelMetrics 构造一个把 Hit/Miss/LoadDuration 转译为 OTel 指标的 Metrics 实现。
// MetricsProvider 未启用时全局是 noop,所有调用零开销。
//
// 接入方式:dbcache.New(loader, dbcache.WithMetrics(dbcache.NewOTelMetrics()))。
func NewOTelMetrics() Metrics {
	meter := otel.GetMeterProvider().Meter(instrumentationName)
	hits, _ := meter.Int64Counter(
		"dbcache.hits",
		metric.WithDescription("Number of dbcache hits (including negative cache)."),
	)
	misses, _ := meter.Int64Counter(
		"dbcache.misses",
		metric.WithDescription("Number of dbcache misses that triggered the loader."),
	)
	duration, _ := meter.Float64Histogram(
		"dbcache.load.duration",
		metric.WithDescription("Duration of dbcache loader calls."),
		metric.WithUnit("ms"),
	)
	return &otelMetrics{hits: hits, misses: misses, duration: duration}
}

func (m *otelMetrics) Hit(name string) {
	m.hits.Add(context.Background(), 1, metric.WithAttributes(attrCacheName.String(name)))
}

func (m *otelMetrics) Miss(name string) {
	m.misses.Add(context.Background(), 1, metric.WithAttributes(attrCacheName.String(name)))
}

func (m *otelMetrics) LoadDuration(name string, d time.Duration, err error) {
	m.duration.Record(
		context.Background(),
		float64(d)/float64(time.Millisecond),
		metric.WithAttributes(
			attrCacheName.String(name),
			attrStatus.String(loadStatus(err)),
		),
	)
}

// loadStatus 把 loader 返回的 err 归一成 ok / not_found / error 三态。
func loadStatus(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}

// NewOTelTracer 从全局 TracerProvider 取 Tracer,每次调用动态获取,
// 不存在像 metric instrument 那样"绑定到旧 MeterProvider"的问题。
// TraceProvider 未启用时全局是 noop,所有 Start/End 零开销。
//
// 接入方式:dbcache.New(loader, dbcache.WithTracer(dbcache.NewOTelTracer()))。
func NewOTelTracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// NoopTracer 返回不产任何 span 的 trace.Tracer,是 dbcache.New 的默认值。
// 业务也可显式 WithTracer(NoopTracer()) 覆盖外部注入回到无 span 状态。
func NoopTracer() trace.Tracer {
	return tracenoop.NewTracerProvider().Tracer(instrumentationName)
}
