package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"

	"github.com/toolbelts/forge/ioc"
)

// TraceProvider OTel 链路追踪提供者，使用 OTLP HTTP 协议把 span 上报到 collector。
//
// 编排约定：
//   - 排在 LoggerProvider 之后、所有依赖 trace 的 Provider 之前。
//     Redis/Database/Grpc/Gateway 在 Register 阶段就装 stats handler / query hook，
//     必须保证此时全局 TracerProvider 已就绪，否则 instrumentation 拿到的是 noop。
//   - 即使 trace.enabled=false，全局保留 OTel 默认的 noop tracer，instrumentation
//     仍可无条件挂载（性能开销可忽略），业务代码与开关解耦。
//
// Resource 字段：
//   - service.name        viper.GetString("app.name")，回退 "gpd"
//   - service.version     BuildInfo.Version
//   - service.instance.id hostname + commit short，hostname 缺失时 uuid 兜底
//   - env                 viper.GetString("trace.env")
type TraceProvider struct {
	enabled         bool
	tp              *traceSdk.TracerProvider
	shutdownTimeout time.Duration
}

// Register 读 trace.* 配置；启用时构建 otlphttp exporter + TracerProvider + 全局 Propagator，
// 并把 *traceSdk.TracerProvider 注入容器供测试或外部插桩使用。
// 未启用时不设 TracerProvider，OTel 自动回落到 noop，所有 instrumentation 不产生 span。
func (p *TraceProvider) Register(ctx context.Context) error {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Error().Err(err).Msg("otel internal error")
	}))

	v := MustGetViper(ctx)
	p.enabled = v.GetBool("trace.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "trace").Msg("trace disabled, skip register")
		return nil
	}

	endpoint := v.GetString("trace.endpoint")
	if endpoint == "" {
		return errors.New("trace endpoint is empty")
	}
	timeout := v.GetDuration("trace.timeout")
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ratio := v.GetFloat64("trace.sample_ratio")
	if ratio <= 0 {
		ratio = 1.0
	}
	headers := v.GetStringMapString("trace.headers")
	env := v.GetString("trace.env")

	expOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(timeout),
	}
	if len(headers) > 0 {
		expOpts = append(expOpts, otlptracehttp.WithHeaders(headers))
	}

	exp, err := otlptracehttp.New(ctx, expOpts...)
	if err != nil {
		return fmt.Errorf("init trace exporter failed: %w", err)
	}

	bi := MustGetBuildInfo(ctx)
	res := resource.NewSchemaless(
		semconv.ServiceNameKey.String(string(MustGetAppName(ctx))),
		semconv.ServiceVersionKey.String(strings.TrimSpace(bi.Version)),
		semconv.ServiceInstanceIDKey.String(bi.InstanceId()),
		attribute.Key("env").String(env),
	)

	p.tp = traceSdk.NewTracerProvider(
		traceSdk.WithSampler(traceSdk.ParentBased(traceSdk.TraceIDRatioBased(ratio))),
		traceSdk.WithBatcher(exp),
		traceSdk.WithResource(res),
	)
	otel.SetTracerProvider(p.tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	p.shutdownTimeout = v.GetDuration("trace.shutdown_timeout")
	if p.shutdownTimeout <= 0 {
		p.shutdownTimeout = 5 * time.Second
	}

	ioc.MustInstance(ctx, p.tp)
	log.Ctx(ctx).Info().
		Str("endpoint", endpoint).
		Float64("sample_ratio", ratio).
		Str("env", env).
		Msg("trace exporter ready")
	return nil
}

// Setup 无操作。
func (p *TraceProvider) Setup(ctx context.Context) error {
	return nil
}

// Shutdown 关闭 TracerProvider，确保 batcher 把缓冲数据导出到 collector。
// 用 context.WithoutCancel 兜底父 ctx 已取消的场景，避免 batch 数据被强制丢弃。
func (p *TraceProvider) Shutdown(ctx context.Context) error {
	if !p.enabled || p.tp == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.shutdownTimeout)
	defer cancel()
	if err := p.tp.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown tracer_provider failed: %w", err)
	}
	return nil
}

// MustGetTracerProvider 从容器获取 *traceSdk.TracerProvider，缺失时 panic。
// trace.enabled=false 时容器内不存在，业务方需自行判断是否调用。
func MustGetTracerProvider(ctx context.Context) *traceSdk.TracerProvider {
	return ioc.MustGet[*traceSdk.TracerProvider](ctx)
}
