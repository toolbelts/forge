// Package accesslog 提供 gRPC 一元/流式访问日志拦截器。
//
// 使用约定:
//   - 由 provider/accesslog.go 接入 InterceptorChain
//   - 注册位置:Recovery 内层、Error 外层。Recovery 已把 panic 转成 errkit.Error,
//     Error 已把裸 error 归一化,AccessLog 看到的 err 永远满足 errkit.Error 或 nil,
//     可以稳定通过 errkit.FromError 提取 error_code / error_name。
//   - 字段命名采用下划线风格(user_id、user_ip、error_code 等),与项目其它日志一致。
//   - 不做 panic recover、不做 error 归一化、不做限流/鉴权/校验,这些由对应 Provider 各自负责。
package accesslog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/meta"
)

// truncatedSuffix 在 payload 摘要被字节级截断后追加,提示日志查看者尾部已被裁剪。
const truncatedSuffix = "...[truncated]"

// payloadMarshaler 复用的 protojson 序列化配置。
// UseProtoNames 让字段名与 proto 定义一致便于日志检索;
// EmitUnpopulated 关掉以减少 zero value 字段噪声、控制日志体积。
var payloadMarshaler = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: false,
}

// options 控制 AccessLog 拦截器的行为。
type options struct {
	payload         bool
	payloadMaxBytes int
	slowThreshold   time.Duration
	skips           map[string]struct{}
}

// defaultOptions 给出未显式配置时的默认参数。
// payload 默认开启与 yaml 模板保持一致;maxBytes 限制单字段长度,避免大请求把日志撑爆;
// slowThreshold 留空(<=0 关闭慢分级);skips 留空。
var defaultOptions = options{
	payload:         true,
	payloadMaxBytes: 2048,
}

// Option 定义拦截器的可选配置。
type Option func(*options)

// WithPayload 控制是否记录 req/resp 摘要,以及单字段的字节级长度上限。
// maxBytes 传 0 时保留默认值,< 0 表示不截断。
func WithPayload(enabled bool, maxBytes int) Option {
	return func(o *options) {
		o.payload = enabled
		if maxBytes != 0 {
			o.payloadMaxBytes = maxBytes
		}
	}
}

// WithSlowThreshold 设置慢请求阈值。spent 超过该阈值且 err == nil 时,日志降级为 Warn。
// d <= 0 关闭慢分级。
func WithSlowThreshold(d time.Duration) Option {
	return func(o *options) {
		o.slowThreshold = d
	}
}

// WithSkips 跳过指定端点的访问日志(完全匹配,不做正则/前缀)。
// 列表项可以是 gRPC FullMethod(/grpc.health.v1.Health/Check)或
// HTTP path(/v1/health),任一命中即跳过。常用于 health check / reflection
// 等高频低价值方法。
func WithSkips(items []string) Option {
	return func(o *options) {
		o.skips = meta.BuildSkips(items)
	}
}

// buildOptions 把 Option 列表合并到默认配置之上。
func buildOptions(opts ...Option) options {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// UnaryInterceptor 一元 RPC 访问日志拦截器。
//
// 必填字段:service、method、full_method、http_method、http_path、user_ip、user_country、
// device_id、platform、version、language、spent。
// 条件字段:user_id(meta 有则加)、trace_id/span_id(span 有效则加)、
// req(payload 启用)、resp(payload 启用且 err==nil)、error_code/error_name(BizError 可提取)。
// 级别:err != nil → Error;spent > slowThreshold > 0 → Warn;否则 Info。
func UnaryInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	o := buildOptions(opts...)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if meta.MatchSkips(ctx, o.skips) {
			return handler(ctx, req)
		}
		ctx = meta.Attach(ctx)
		ctx = injectTraceLogger(ctx)
		start := time.Now()
		resp, err := handler(ctx, req)
		spent := time.Since(start)

		evt := startEvent(ctx, err, spent, o.slowThreshold)
		rm := meta.Request(ctx)
		service, method := splitFullMethod(info.FullMethod)
		evt.Str("service", service).
			Str("method", method).
			Str("full_method", info.FullMethod).
			Str("http_method", rm.Method).
			Str("http_path", rm.Path).
			Str("user_ip", rm.UserIp).
			Str("user_country", rm.UserCountry).
			Str("device_id", rm.DeviceId).
			Str("platform", rm.Platform).
			Str("version", rm.Version).
			Str("language", rm.Language)
		addConditionalFields(evt, ctx)
		if o.payload {
			evt.Str("req", marshalPayload(req, o.payloadMaxBytes))
			if err == nil {
				evt.Str("resp", marshalPayload(resp, o.payloadMaxBytes))
			}
		}
		addErrorFields(evt, err)
		evt.Dur("spent", spent).Msg(pickMsg(err, spent, o.slowThreshold, "grpc request"))
		return resp, err
	}
}

// StreamInterceptor 流式 RPC 访问日志拦截器。
//
// 轻量,只记录 service、method、full_method、user_ip、is_client_stream、is_server_stream、spent
// 必填字段;user_id、trace_id、span_id、error_code、error_name 条件字段。
// stream 不打开始日志,只在 handler 返回后记录一条,避免长连接日志体积爆炸。
func StreamInterceptor(opts ...Option) grpc.StreamServerInterceptor {
	o := buildOptions(opts...)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if meta.MatchSkips(ss.Context(), o.skips) {
			return handler(srv, ss)
		}
		ctx := injectTraceLogger(ss.Context())
		ss = &wrappedStream{ServerStream: ss, ctx: ctx}
		start := time.Now()
		err := handler(srv, ss)
		spent := time.Since(start)

		evt := startEvent(ctx, err, spent, o.slowThreshold)
		rm := meta.Request(ctx)
		service, method := splitFullMethod(info.FullMethod)
		evt.Str("service", service).
			Str("method", method).
			Str("full_method", info.FullMethod).
			Str("user_ip", rm.UserIp).
			Bool("is_client_stream", info.IsClientStream).
			Bool("is_server_stream", info.IsServerStream)
		addConditionalFields(evt, ctx)
		addErrorFields(evt, err)
		evt.Dur("spent", spent).Msg(pickMsg(err, spent, o.slowThreshold, "grpc stream"))
		return err
	}
}

// startEvent 根据 err / spent / slowThreshold 选择日志级别,返回已创建的 zerolog Event。
// err != nil 时自动 .Err 写入 error 字段;调用方继续追加字段后调 .Msg 完成输出。
func startEvent(ctx context.Context, err error, spent, slow time.Duration) *zerolog.Event {
	switch {
	case err != nil:
		return log.Ctx(ctx).Error().Err(err)
	case slow > 0 && spent > slow:
		return log.Ctx(ctx).Warn()
	default:
		return log.Ctx(ctx).Info()
	}
}

// pickMsg 根据状态选择对应日志消息文案。prefix 区分 "grpc request" / "grpc stream"。
func pickMsg(err error, spent, slow time.Duration, prefix string) string {
	switch {
	case err != nil:
		return prefix + " failed"
	case slow > 0 && spent > slow:
		return prefix + " slow"
	default:
		return prefix + " success"
	}
}

// addConditionalFields 写入仅在条件成立时才追加的字段。
// user_id 取自 meta(身份拦截器写入,匿名请求不写)。
// trace_id/span_id 在 UnaryInterceptor / StreamInterceptor 入口经 injectTraceLogger
// 注入到 ctx 上的 zerolog logger,本拦截器自身的日志和 handler 内部所有 log.Ctx(ctx)
// 都会自动携带,无需在此再显式写一遍。
func addConditionalFields(evt *zerolog.Event, ctx context.Context) {
	if uid := meta.UserId(ctx); uid != 0 {
		evt.Int64("user_id", uid)
	}
}

// injectTraceLogger 把当前 span 的 trace_id 和 span_id 通过 zerolog WithContext 注入到 ctx,
// handler 内部所有 log.Ctx(ctx) 输出的日志(慢查询、业务日志等)都会自动带这两个字段。
// SpanContext 无效时直接返回原 ctx,避免污染日志。
func injectTraceLogger(ctx context.Context) context.Context {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ctx
	}
	logger := log.Ctx(ctx).With().
		Str("trace_id", sc.TraceID().String()).
		Str("span_id", sc.SpanID().String()).
		Logger()
	return logger.WithContext(ctx)
}

// wrappedStream 包装 grpc.ServerStream 替换 Context,让上游注入的 ctx logger
// 能传递到流式 handler 内部。仅用于 accesslog 包内部。
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context 返回包装后的 ctx,覆盖底层 ServerStream.Context。
func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

// addErrorFields 当 err 满足 errkit.Error 时,追加业务错误码与可读名称。
// error_code 是 int32(运维筛选用),error_name 是字符串(排查体验用,依赖应用通过
// errkit.RegisterCodeNamer 注入业务码名解析器)。
func addErrorFields(evt *zerolog.Event, err error) {
	if err == nil {
		return
	}
	be, ok := errkit.FromError(err)
	if !ok {
		return
	}
	code := be.Code()
	evt.Int32("error_code", int32(code)).Str("error_name", code.String())
}

// splitFullMethod 把 "/svc.pkg.Svc/Method" 切成 ("svc.pkg.Svc", "Method")。
// 不带前导 "/" 或不含 "/" 时返回原串与空串,保证日志字段总是有值。
func splitFullMethod(full string) (service, method string) {
	s := strings.TrimPrefix(full, "/")
	if before, after, ok := strings.Cut(s, "/"); ok {
		return before, after
	}
	return s, ""
}

// marshalPayload 把 req/resp 序列化为日志摘要。
// proto.Message 走 protojson(typed nil 由 protojson 自身兜底返回 "{}");
// 非 proto 用 fmt.Sprint 兜底,避免在拦截器里 panic;
// 超过 maxBytes 字节先按字节截断,再 ToValidUTF8 修掉半个 rune,最后追加 truncatedSuffix。
func marshalPayload(v any, maxBytes int) string {
	if v == nil {
		return ""
	}
	var s string
	if msg, ok := v.(proto.Message); ok {
		buf, err := payloadMarshaler.Marshal(msg)
		if err != nil {
			return fmt.Sprintf("<marshal error: %v>", err)
		}
		s = string(buf)
	} else {
		s = fmt.Sprint(v)
	}
	return truncate(s, maxBytes)
}

// truncate 按字节级裁剪,保留 UTF-8 完整 rune 后追加截断标记。
// maxBytes <= 0 视为不截断,直接返回原串。
func truncate(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	return strings.ToValidUTF8(s[:maxBytes], "") + truncatedSuffix
}
