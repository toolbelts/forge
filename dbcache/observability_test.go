package dbcache

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// installOTel 用 SDK provider 接管全局 OTel,返回 manual reader 与 span recorder。
// 测试退出时还原全局为 noop,避免跨测试污染。
//
// 这些测试不能 t.Parallel(): SetMeterProvider/SetTracerProvider 是进程级副作用,
// 多个测试同时改全局会互相覆盖。
func installOTel(t *testing.T) (*sdkmetric.ManualReader, *tracetest.SpanRecorder) {
	t.Helper()

	mr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	otel.SetMeterProvider(mp)

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		_ = tp.Shutdown(context.Background())
		otel.SetMeterProvider(noopmetric.NewMeterProvider())
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
	})
	return mr, sr
}

// findSpan 在 recorder 里按名字找 span(必须唯一,否则失败)。
func findSpan(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var matched []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == name {
			matched = append(matched, s)
		}
	}
	if len(matched) != 1 {
		t.Fatalf("expected exactly 1 span %q, got %d (ended=%d)", name, len(matched), len(sr.Ended()))
	}
	return matched[0]
}

// hasSpan 判断 recorder 里是否存在指定名字的 span(允许 0 或多个,只判断存在性)。
func hasSpan(sr *tracetest.SpanRecorder, name string) bool {
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return true
		}
	}
	return false
}

// findAttr 从 span 上找指定 key 的 attribute。
func findAttr(span sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// collectMetric 在 manual reader 里找指定 metric,返回 (metricdata.Metrics, ok)。
func collectMetric(t *testing.T, mr *sdkmetric.ManualReader, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := mr.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// sumDataPoint 从 Counter 类型 metric 找匹配 attribute 的 data point 值之和。
func sumDataPoint(m metricdata.Metrics, want attribute.KeyValue) int64 {
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range sum.DataPoints {
		if hasAttr(dp.Attributes, want) {
			total += dp.Value
		}
	}
	return total
}

// histogramCount 从 Histogram 找匹配 attribute 的 data point 累计次数。
func histogramCount(m metricdata.Metrics, want ...attribute.KeyValue) uint64 {
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		return 0
	}
	var total uint64
	for _, dp := range h.DataPoints {
		match := true
		for _, kv := range want {
			if !hasAttr(dp.Attributes, kv) {
				match = false
				break
			}
		}
		if match {
			total += dp.Count
		}
	}
	return total
}

func hasAttr(set attribute.Set, kv attribute.KeyValue) bool {
	v, ok := set.Value(kv.Key)
	if !ok {
		return false
	}
	return v.Emit() == kv.Value.Emit()
}

// withOTel 给 New 加上 OTel metrics + tracer 的 Option,集中给观测测试复用。
func withOTel() []Option {
	return []Option{
		WithMetrics(NewOTelMetrics()),
		WithTracer(NewOTelTracer()),
	}
}

func TestObservability_DefaultIsNoop(t *testing.T) {
	mr, sr := installOTel(t)
	ctx := context.Background()

	// 不传 WithMetrics / WithTracer:就算全局 OTel 已 setup 也不应有任何上报 / span。
	c := New(func(ctx context.Context, k int) (string, error) { return "v", nil })
	_, _ = c.Get(ctx, 1)
	_, _ = c.Get(ctx, 1)

	if hasSpan(sr, "dbcache.Get") || hasSpan(sr, "dbcache.Loader") {
		t.Fatal("default Cache should not produce any dbcache spans")
	}
	if _, ok := collectMetric(t, mr, "dbcache.hits"); ok {
		t.Fatal("default Cache should not emit dbcache.hits")
	}
	if _, ok := collectMetric(t, mr, "dbcache.misses"); ok {
		t.Fatal("default Cache should not emit dbcache.misses")
	}
}

func TestObservability_Trace_Get_MissAndHit(t *testing.T) {
	_, sr := installOTel(t)
	ctx := context.Background()

	c := New(func(ctx context.Context, k int) (string, error) { return "v", nil }, withOTel()...)

	// Miss: 期望 dbcache.Get + dbcache.Loader 各一个;Get span hit=false。
	if _, err := c.Get(ctx, 1); err != nil {
		t.Fatalf("get: %v", err)
	}
	getSpan := findSpan(t, sr, "dbcache.Get")
	loaderSpan := findSpan(t, sr, "dbcache.Loader")

	if v, ok := findAttr(getSpan, attrHit); !ok || v.AsBool() {
		t.Fatalf("expected dbcache.Get hit=false, got %+v ok=%v", v, ok)
	}
	if v, ok := findAttr(getSpan, attrCacheName); !ok || !strings.Contains(v.AsString(), "string") {
		t.Fatalf("expected dbcache.name to contain 'string', got %+v", v)
	}
	if v, ok := findAttr(loaderSpan, attrKeysCount); !ok || v.AsInt64() != 1 {
		t.Fatalf("expected loader keys.count=1, got %+v", v)
	}

	// Hit: 第二次 Get,期望只有 dbcache.Get(无 Loader);hit=true。
	sr.Reset()
	if _, err := c.Get(ctx, 1); err != nil {
		t.Fatalf("second get: %v", err)
	}
	getSpan = findSpan(t, sr, "dbcache.Get")
	if hasSpan(sr, "dbcache.Loader") {
		t.Fatal("hit path should not produce loader span")
	}
	if v, _ := findAttr(getSpan, attrHit); !v.AsBool() {
		t.Fatalf("expected dbcache.Get hit=true on second call, got %+v", v)
	}
}

func TestObservability_Trace_NotFoundIsNotError(t *testing.T) {
	_, sr := installOTel(t)
	ctx := context.Background()

	c := New(func(ctx context.Context, k int) (string, error) { return "", ErrNotFound }, withOTel()...)
	_, err := c.Get(ctx, 1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	getSpan := findSpan(t, sr, "dbcache.Get")
	if v, ok := findAttr(getSpan, attrNotFound); !ok || !v.AsBool() {
		t.Fatalf("expected dbcache.notfound=true, got %+v ok=%v", v, ok)
	}
	// ErrNotFound 不应被记成 span error 状态。
	if getSpan.Status().Code == codes.Error {
		t.Fatalf("ErrNotFound should not set span status to Error, got %+v", getSpan.Status())
	}
}

func TestObservability_Trace_RealErrorRecorded(t *testing.T) {
	_, sr := installOTel(t)
	ctx := context.Background()

	dbErr := errors.New("connection refused")
	c := New(func(ctx context.Context, k int) (string, error) { return "", dbErr }, withOTel()...)
	_, err := c.Get(ctx, 1)
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected %v, got %v", dbErr, err)
	}

	getSpan := findSpan(t, sr, "dbcache.Get")
	if getSpan.Status().Code != codes.Error {
		t.Fatalf("expected Get span status=Error, got %+v", getSpan.Status())
	}
	loaderSpan := findSpan(t, sr, "dbcache.Loader")
	if loaderSpan.Status().Code != codes.Error {
		t.Fatalf("expected Loader span status=Error, got %+v", loaderSpan.Status())
	}
}

func TestObservability_Metrics_HitsMissesAndDuration(t *testing.T) {
	mr, _ := installOTel(t)
	ctx := context.Background()

	c := New(func(ctx context.Context, k int) (string, error) { return "v", nil }, withOTel()...)
	_, _ = c.Get(ctx, 1) // miss
	_, _ = c.Get(ctx, 1) // hit
	_, _ = c.Get(ctx, 2) // miss

	misses, ok := collectMetric(t, mr, "dbcache.misses")
	if !ok {
		t.Fatal("missing metric dbcache.misses")
	}
	if got := sumDataPoint(misses, attrCacheName.String(c.name)); got != 2 {
		t.Fatalf("expected 2 misses, got %d", got)
	}
	hits, ok := collectMetric(t, mr, "dbcache.hits")
	if !ok {
		t.Fatal("missing metric dbcache.hits")
	}
	if got := sumDataPoint(hits, attrCacheName.String(c.name)); got != 1 {
		t.Fatalf("expected 1 hit, got %d", got)
	}

	dur, ok := collectMetric(t, mr, "dbcache.load.duration")
	if !ok {
		t.Fatal("missing metric dbcache.load.duration")
	}
	if got := histogramCount(dur, attrCacheName.String(c.name), attrStatus.String("ok")); got != 2 {
		t.Fatalf("expected 2 ok loader durations, got %d", got)
	}
}

func TestObservability_Metrics_LoadStatus(t *testing.T) {
	mr, _ := installOTel(t)
	ctx := context.Background()

	dbErr := errors.New("boom")
	notFoundLoader := New(func(ctx context.Context, k int) (string, error) { return "", ErrNotFound }, withOTel()...)
	errorLoader := New(func(ctx context.Context, k int) (string, error) { return "", dbErr }, withOTel()...)

	_, _ = notFoundLoader.Get(ctx, 1)
	_, _ = errorLoader.Get(ctx, 1)

	dur, ok := collectMetric(t, mr, "dbcache.load.duration")
	if !ok {
		t.Fatal("missing metric dbcache.load.duration")
	}
	if got := histogramCount(dur, attrCacheName.String(notFoundLoader.name), attrStatus.String("not_found")); got != 1 {
		t.Fatalf("expected 1 not_found duration, got %d", got)
	}
	if got := histogramCount(dur, attrCacheName.String(errorLoader.name), attrStatus.String("error")); got != 1 {
		t.Fatalf("expected 1 error duration, got %d", got)
	}
}

func TestObservability_Trace_MGetAttributes(t *testing.T) {
	_, sr := installOTel(t)
	ctx := context.Background()

	loader := func(ctx context.Context, k int) (string, error) {
		if k == 99 {
			return "", ErrNotFound
		}
		return "v", nil
	}
	c := New(loader, withOTel()...)

	// 第一次:全 miss,含 1 个 not_found。
	if _, err := c.MGet(ctx, 1, 2, 99); err != nil {
		t.Fatalf("mget: %v", err)
	}
	mgetSpan := findSpan(t, sr, "dbcache.MGet")
	if v, _ := findAttr(mgetSpan, attrKeysCount); v.AsInt64() != 3 {
		t.Fatalf("expected keys.count=3, got %+v", v)
	}
	if v, _ := findAttr(mgetSpan, attrMissesCount); v.AsInt64() != 3 {
		t.Fatalf("expected misses.count=3 on first MGet, got %+v", v)
	}

	// 第二次:1 / 2 / 99 都已缓存(99 走负缓存),应该全 hit。
	sr.Reset()
	if _, err := c.MGet(ctx, 1, 2, 99); err != nil {
		t.Fatalf("mget 2: %v", err)
	}
	mgetSpan = findSpan(t, sr, "dbcache.MGet")
	if v, _ := findAttr(mgetSpan, attrHitsCount); v.AsInt64() != 3 {
		t.Fatalf("expected hits.count=3 on second MGet, got %+v", v)
	}
}
