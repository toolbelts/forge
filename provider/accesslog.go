package provider

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/accesslog"
	"github.com/toolbelts/forge/ioc"
)

// AccessLogProvider 把 gRPC 访问日志拦截器接入 InterceptorChain。
//
// 编排约定:排在 RecoveryProvider 之后、ErrorProvider 之前(Recovery 内层、Error 外层)。
//   - Recovery 已经把 panic 转成 *BizError 走 return,AccessLog 看到的是 error_code=PANIC 的归一化错误,
//     自身无需 defer recover,与 Recovery 职责互不重复。
//   - Error 已经把所有裸 error 归一化为 *BizError,AccessLog 在响应路径上拿到的 err 永远可以
//     errorpb.FromError 稳定提取,error_code/error_name 字段一致性最好。
//
// 一元拦截器与流式拦截器都接入:unary 含完整字段(req/resp 摘要、http 元数据等),
// stream 走轻量字段(只在 handler 返回后打一条),避免长连接日志体积爆炸。
type AccessLogProvider struct {
	enabled bool
}

// Register 读 accesslog.enabled。disabled 时 Setup 直接跳过,不挂任何拦截器,与 token/ratelimit 风格一致。
func (p *AccessLogProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("accesslog.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "accesslog").Msg("accesslog disabled, skip")
	}
	return nil
}

// Setup 读 accesslog.* 配置,把一元 + 流式拦截器加进 chain。
func (p *AccessLogProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	v := MustGetViper(ctx)

	payload := v.GetBool("accesslog.payload")
	payloadMaxBytes := v.GetInt("accesslog.payload_max_bytes")
	slowThreshold := v.GetDuration("accesslog.slow_threshold")
	skips := v.GetStringSlice("accesslog.skips")

	opts := []accesslog.Option{
		accesslog.WithPayload(payload, payloadMaxBytes),
		accesslog.WithSlowThreshold(slowThreshold),
		accesslog.WithSkips(skips),
	}

	chain := ioc.MustGet[*InterceptorChain](ctx)
	chain.Use(accesslog.UnaryInterceptor(opts...))
	chain.UseStream(accesslog.StreamInterceptor(opts...))

	log.Ctx(ctx).Info().
		Str("provider", "accesslog").
		Bool("payload", payload).
		Int("payload_max_bytes", payloadMaxBytes).
		Dur("slow_threshold", slowThreshold).
		Int("skips", len(skips)).
		Msg("accesslog interceptor registered")
	return nil
}
