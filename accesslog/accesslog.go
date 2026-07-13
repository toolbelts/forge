package accesslog

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	json "github.com/goccy/go-json"
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

// tooLargeFmt 是 wire size 预检超限时的占位符,%s 为 proto 消息全名、%d 为 wire 字节数。
// 带类型名让排障时无需还原 payload 就能定位是哪个消息撑大了响应。
const tooLargeFmt = "<payload too large: %s, %d bytes>"

// maskPlaceholder 命中 mask 字段后的替换值。固定 "***",不开放配置避免无意义的样式分歧。
const maskPlaceholder = "***"

// payloadMarshaler 复用的 protojson 序列化配置。
// UseProtoNames 让字段名与 proto 定义一致便于日志检索;
// EmitUnpopulated 关掉以减少 zero value 字段噪声、控制日志体积。
var payloadMarshaler = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: false,
}

// options 控制 AccessLog 拦截器的行为。
// sizeLimit / maskProbes 是 resolve() 从原始配置派生的运行时值,不直接接受 Option 写入。
type options struct {
	payload          bool
	payloadMaxBytes  int
	payloadSizeLimit int // 原始配置:0 自动派生 / <0 禁用预检 / >0 显式 wire 字节阈值
	sizeLimit        int // resolve() 派生:>0 为预检阈值,<=0 关闭预检
	slowThreshold    time.Duration
	skips            map[string]struct{}
	maskFields       map[string]struct{}
	maskProbes       [][]byte // resolve() 派生:每个 mask key 的 `"key"` 字节串,序列化后快速预检用
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
// 完整 JSON 对象写入 req/resp 字段;超长截断后的前缀落 req_text/resp_text 字符串字段。
func WithPayload(enabled bool, maxBytes int) Option {
	return func(o *options) {
		o.payload = enabled
		if maxBytes != 0 {
			o.payloadMaxBytes = maxBytes
		}
	}
}

// WithPayloadSizeLimit 设置 proto 消息序列化前的 wire size 预检阈值(字节)。
// 超过阈值直接跳过 protojson,req_text/resp_text 记 "<payload too large: <消息全名>, N bytes>"
// 占位符,避免大消息白付全量序列化成本(最终反正会被截断到 payloadMaxBytes)。
// n > 0 显式阈值;n == 0(默认)自动派生为 4×payloadMaxBytes(payloadMaxBytes < 0 时不预检);
// n < 0 强制禁用预检。仅作用于 proto.Message 分支。
func WithPayloadSizeLimit(n int) Option {
	return func(o *options) {
		o.payloadSizeLimit = n
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

// WithMaskFields 把 protojson 摘要中匹配 fields 的字段值替换为 "***"。
// fields 走精确等值匹配(不做 prefix/suffix),建议传 proto 字段名(snake_case),
// 与 payloadMarshaler.UseProtoNames=true 保持一致。空列表 / nil 时不启用脱敏。
// 仅作用于 proto.Message 分支;非 proto 兜底走 fmt.Sprint 不做脱敏。
func WithMaskFields(fields []string) Option {
	return func(o *options) {
		if len(fields) == 0 {
			return
		}
		m := make(map[string]struct{}, len(fields))
		for _, f := range fields {
			if f == "" {
				continue
			}
			m[f] = struct{}{}
		}
		if len(m) == 0 {
			return
		}
		o.maskFields = m
	}
}

// buildOptions 把 Option 列表合并到默认配置之上,收尾统一做派生,保证 Option 传入顺序无关。
func buildOptions(opts ...Option) options {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	o.resolve()
	return o
}

// resolve 从原始配置派生运行时值,必须在所有 Option 应用完之后调用。
// sizeLimit 自动模式取 4×payloadMaxBytes:JSON 输出相对 wire 格式有膨胀(字段名、引号、
// base64),4 倍余量保证"能完整放进日志的消息"不会被预检误杀;payloadMaxBytes < 0
// 表示调用方要完整 payload,自动模式下预检一并禁用。
func (o *options) resolve() {
	switch {
	case o.payloadSizeLimit > 0:
		o.sizeLimit = o.payloadSizeLimit
	case o.payloadSizeLimit < 0:
		o.sizeLimit = 0
	default:
		// payloadMaxBytes > MaxInt/4 是退化配置,乘法会溢出,按禁用预检处理。
		if o.payloadMaxBytes > 0 && o.payloadMaxBytes <= math.MaxInt/4 {
			o.sizeLimit = 4 * o.payloadMaxBytes
		}
	}
	for k := range o.maskFields {
		o.maskProbes = append(o.maskProbes, []byte(`"`+k+`"`))
	}
}

// UnaryInterceptor 一元 RPC 访问日志拦截器。
//
// 必填字段:service、method、full_method、http_method、http_path、user_ip、user_country、
// device_id、platform、version、language、spent。
// 条件字段:user_id(meta 有则加)、trace_id/span_id(span 有效则加)、
// req/req_text(payload 启用,互斥:完整 JSON 对象走 req,截断/占位/非 proto 走 req_text)、
// resp/resp_text(payload 启用且 err==nil,互斥规则同上)、error_code/error_name(BizError 可提取)。
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
			addPayload(evt, "req", "req_text", marshalPayload(req, &o))
			if err == nil {
				addPayload(evt, "resp", "resp_text", marshalPayload(resp, &o))
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

// payloadKind 标记 marshalPayload 产物的写入方式。
type payloadKind uint8

const (
	payloadOmit payloadKind = iota // v == nil,req/resp 与 *_text 都不写
	payloadJson                    // 合法 JSON 对象,原始字节直接 RawJSON 嵌入,零二次转义
	payloadText                    // 文本(截断前缀/占位符/非 proto 值),走 *_text 字符串字段
)

// payloadValue 是 marshalPayload 的判别式结果,raw / text 按 kind 互斥有效。
type payloadValue struct {
	kind payloadKind
	raw  []byte // payloadJson 时有效
	text string // payloadText 时有效
}

// marshalPayload 把 req/resp 序列化为日志摘要。
// 决策流:nil → omit;非 proto → fmt.Sprint 截断落 text(无 schema 不脱敏);
// proto → wire size 预检(超限记占位符,省掉必然被截断的全量序列化)→ protojson →
// mask(probe 预检命中才做 JSON 树往返)→ 超长截断落 text → 顶层非对象(wrapper 类型)落 text →
// 合法 JSON 对象落 raw。
// 先 mask 再截断,避免被截断撕裂的 JSON 前缀漏脱敏字段。
// raw 分支给 zerolog RawJSON 用,两个前提由上游保证:protojson/goccy 会转义所有控制字符
// (输出无裸换行),无效 UTF-8 在 Marshal 时报错走占位符分支。
func marshalPayload(v any, o *options) payloadValue {
	if v == nil {
		return payloadValue{kind: payloadOmit}
	}
	msg, ok := v.(proto.Message)
	if !ok {
		return payloadValue{kind: payloadText, text: truncate(fmt.Sprint(v), o.payloadMaxBytes)}
	}
	if o.sizeLimit > 0 {
		if n := proto.Size(msg); n > o.sizeLimit {
			name := msg.ProtoReflect().Descriptor().FullName()
			return payloadValue{kind: payloadText, text: fmt.Sprintf(tooLargeFmt, name, n)}
		}
	}
	buf, err := payloadMarshaler.Marshal(msg)
	if err != nil {
		return payloadValue{kind: payloadText, text: fmt.Sprintf("<marshal error: %v>", err)}
	}
	if len(o.maskProbes) > 0 && maskProbeHit(buf, o.maskProbes) {
		buf = applyMask(buf, o.maskFields)
	}
	if o.payloadMaxBytes > 0 && len(buf) > o.payloadMaxBytes {
		return payloadValue{kind: payloadText, text: truncate(string(buf), o.payloadMaxBytes)}
	}
	// wrapper 类型(StringValue/Duration 等)顶层输出是标量/数组,raw 嵌入会让 req/resp
	// 字段跨日志行出现对象/非对象混型(ES mapping 冲突),统一落 text;顺带防御空 buf。
	if len(buf) == 0 || buf[0] != '{' {
		return payloadValue{kind: payloadText, text: string(buf)}
	}
	return payloadValue{kind: payloadJson, raw: buf}
}

// maskProbeHit 检查序列化输出中是否出现任一 mask key 的 `"key"` 字节串,
// 全部未命中时调用方可跳过 applyMask 的整轮 JSON 树往返。
// JSON 字符串值内部的引号必被转义为 \",`"key"` 不可能由值内容拼出,故无漏报;
// 唯一误报是某字符串值整体恰等于 key(如 {"type":"password"}),代价仅是白做一次往返。
func maskProbeHit(buf []byte, probes [][]byte) bool {
	for _, p := range probes {
		if bytes.Contains(buf, p) {
			return true
		}
	}
	return false
}

// addPayload 按 payloadValue 的判别结果写入事件字段:完整 JSON 对象以 RawJSON 嵌入
// jsonKey(零二次转义),文本走 textKey,omit 两个字段都不写。
// 不变量:jsonKey 要么缺席要么必为 JSON 对象,textKey 要么缺席要么必为字符串,
// 二者互斥,避免下游索引(ES 等)同字段混型。
func addPayload(evt *zerolog.Event, jsonKey, textKey string, pv payloadValue) {
	switch pv.kind {
	case payloadJson:
		evt.RawJSON(jsonKey, pv.raw)
	case payloadText:
		evt.Str(textKey, pv.text)
	}
}

// applyMask 对 protojson 输出做字段级脱敏:解析为通用 JSON 树,递归替换命中 mask 的 key 的 value
// 为 "***",再重新序列化。任何 unmarshal/marshal 失败都直接返回原 buf(best-effort,不阻塞日志)。
// 仅替换非 nil 叶子值;遇到 null 保持不动避免类型变形。
func applyMask(buf []byte, mask map[string]struct{}) []byte {
	var root any
	if err := json.Unmarshal(buf, &root); err != nil {
		return buf
	}
	maskWalk(root, mask)
	out, err := json.Marshal(root)
	if err != nil {
		return buf
	}
	return out
}

// maskWalk 递归遍历 JSON 树,命中 mask 的 map key 且 value 非 nil 时原地替换为占位符。
// 数组元素递归下钻;其它叶子节点不动。
func maskWalk(v any, mask map[string]struct{}) {
	switch x := v.(type) {
	case map[string]any:
		for k, vv := range x {
			if _, hit := mask[k]; hit && vv != nil {
				x[k] = maskPlaceholder
				continue
			}
			maskWalk(vv, mask)
		}
	case []any:
		for _, vv := range x {
			maskWalk(vv, mask)
		}
	}
}

// truncate 按字节级裁剪,保留 UTF-8 完整 rune 后追加截断标记。
// maxBytes <= 0 视为不截断,直接返回原串。
func truncate(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	return strings.ToValidUTF8(s[:maxBytes], "") + truncatedSuffix
}
