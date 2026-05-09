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
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	metricSdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"

	"github.com/toolbelts/forge/ioc"
)

// MetricsProvider OTel 指标提供者，使用 OTLP HTTP 协议把 metric 上报到 collector。
//
// 编排约定（与 TraceProvider 一致）：
//   - 排在 LoggerProvider 之后、所有依赖 metric 的 Provider 之前。
//     开启 metrics instrumentation 的 Provider 会在 Register/Setup 阶段装 metrics hook,
//     必须保证此时全局 MeterProvider 已就绪，否则拿到的是 noop meter。
//   - metrics.enabled=false 时全局保留 OTel 默认的 noop meter,且默认关闭自动 instrumentation。
//
// Resource 字段与 trace 一致，便于在 collector 侧按 service.* 关联：
//   - service.name        viper.GetString("app.name")，回退 "gpd"
//   - service.version     BuildInfo.Version
//   - service.instance.id hostname + commit short，hostname 缺失时 uuid 兜底
//   - env                 viper.GetString("metrics.env")
type MetricsProvider struct {
	enabled         bool
	mp              *metricSdk.MeterProvider
	shutdownTimeout time.Duration
}

// Register 读 metrics.* 配置；启用时构建 otlphttp exporter + MeterProvider，
// 并把 *metricSdk.MeterProvider 注入容器供测试或外部插桩使用。
// 未启用时不设 MeterProvider，OTel 自动回落到 noop，所有 instrumentation 不产生 metric。
func (p *MetricsProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("metrics.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "metrics").Msg("metrics disabled, skip register")
		return nil
	}

	endpoint := v.GetString("metrics.endpoint")
	if endpoint == "" {
		return errors.New("metrics endpoint is empty")
	}
	timeout := v.GetDuration("metrics.timeout")
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	interval := v.GetDuration("metrics.interval")
	if interval <= 0 {
		interval = 30 * time.Second
	}
	headers := v.GetStringMapString("metrics.headers")
	env := v.GetString("metrics.env")

	expOpts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpointURL(endpoint),
		otlpmetrichttp.WithTimeout(timeout),
	}
	if len(headers) > 0 {
		expOpts = append(expOpts, otlpmetrichttp.WithHeaders(headers))
	}

	exp, err := otlpmetrichttp.New(ctx, expOpts...)
	if err != nil {
		return fmt.Errorf("init metrics exporter failed: %w", err)
	}

	bi := MustGetBuildInfo(ctx)
	res := resource.NewSchemaless(
		semconv.ServiceNameKey.String(string(MustGetAppName(ctx))),
		semconv.ServiceVersionKey.String(strings.TrimSpace(bi.Version)),
		semconv.ServiceInstanceIDKey.String(bi.InstanceId()),
		attribute.Key("env").String(env),
	)

	p.mp = metricSdk.NewMeterProvider(
		metricSdk.WithReader(metricSdk.NewPeriodicReader(exp, metricSdk.WithInterval(interval))),
		metricSdk.WithResource(res),
	)
	otel.SetMeterProvider(p.mp)

	p.shutdownTimeout = v.GetDuration("metrics.shutdown_timeout")
	if p.shutdownTimeout <= 0 {
		p.shutdownTimeout = 5 * time.Second
	}

	ioc.MustInstance(ctx, p.mp)
	log.Ctx(ctx).Info().
		Str("endpoint", endpoint).
		Dur("interval", interval).
		Str("env", env).
		Msg("metrics exporter ready")
	return nil
}

// Setup 无操作。
func (p *MetricsProvider) Setup(ctx context.Context) error {
	return nil
}

// Shutdown 关闭 MeterProvider，确保 reader 把缓冲数据导出到 collector。
// 用 context.WithoutCancel 兜底父 ctx 已取消的场景，避免最近一个周期的指标被强制丢弃。
func (p *MetricsProvider) Shutdown(ctx context.Context) error {
	if !p.enabled || p.mp == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.shutdownTimeout)
	defer cancel()
	if err := p.mp.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown meter_provider failed: %w", err)
	}
	return nil
}

// MustGetMeterProvider 从容器获取 *metricSdk.MeterProvider，缺失时 panic。
// metrics.enabled=false 时容器内不存在，业务方需自行判断是否调用。
func MustGetMeterProvider(ctx context.Context) *metricSdk.MeterProvider {
	return ioc.MustGet[*metricSdk.MeterProvider](ctx)
}
