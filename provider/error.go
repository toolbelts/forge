package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"runtime/debug"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/ioc"
)

// ErrorProvider 注册 error 归一化拦截器到 InterceptorChain。
// 任何不满足 errkit.Error 的 handler 错误都会被转成 errkit.CodeInternal(带 cause);
// context.Canceled / DeadlineExceeded / 已存在的 grpc Status 会被映射到对应 errkit Code。
//
// 排在 RecoveryProvider 之后、ValidateProvider 之前。
type ErrorProvider struct{}

// Register 无前置依赖。
func (p *ErrorProvider) Register(ctx context.Context) error { return nil }

// Setup 把一元 + 流式归一化拦截器加进 chain。
func (p *ErrorProvider) Setup(ctx context.Context) error {
	chain := ioc.MustGet[*InterceptorChain](ctx)
	chain.Use(errorUnaryInterceptor())
	chain.UseStream(errorStreamInterceptor())
	log.Ctx(ctx).Info().Str("provider", "error").Msg("error interceptor registered")
	return nil
}

// errorUnaryInterceptor 兜底归一化任何 handler 错误为 errkit.Error。
func errorUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}
		return resp, toGrpcStatus(normalizeError(ctx, info.FullMethod, err))
	}
}

// errorStreamInterceptor 同 unary 但处理流式 RPC。
func errorStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err == nil {
			return nil
		}
		return toGrpcStatus(normalizeError(ss.Context(), info.FullMethod, err))
	}
}

// normalizeError 统一错误为 errkit.Error:
//   - nil → 直接返回(调用方已过滤,但保险起见)
//   - 已满足 errkit.Error → 透传
//   - context.Canceled → CodeCanceled
//   - context.DeadlineExceeded → CodeDeadlineExceeded
//   - 已是 grpc Status(如业务直接 status.Errorf)→ 按 grpc code 反查 Code
//   - 其它裸 error → CodeInternal,挂 cause 并写日志含 stack
func normalizeError(ctx context.Context, method string, err error) errkit.Error {
	if err == nil {
		return nil
	}
	if e, ok := errkit.FromError(err); ok {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return errkit.New(errkit.CodeCanceled, "request canceled").WithCause(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errkit.New(errkit.CodeDeadlineExceeded, "request deadline exceeded").WithCause(err)
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return errkit.New(errkit.FromGrpcCode(st.Code()), st.Message()).
			WithGrpcCode(st.Code()).
			WithCause(err)
	}
	log.Ctx(ctx).Error().
		Str("method", method).
		Err(err).
		Bytes("stack", debug.Stack()).
		Msg("unhandled error normalized to INTERNAL")
	return errkit.New(errkit.CodeInternal, err.Error()).WithCause(err)
}

// toGrpcStatus 把 errkit.Error 转成 gRPC Status,details 由应用注册的 errkit.Encoder 提供。
// 没注册 Encoder 时返回不带 details 的纯 Status,客户端只能看到 code+message。
func toGrpcStatus(e errkit.Error) error {
	if e == nil {
		return nil
	}
	grpcCode := errkit.ToGrpcCode(e.Code())
	// 优先采用 *simpleError 上 WithGrpcCode 显式设置的值
	type grpcCoder interface {
		GrpcCode() codes.Code
	}
	if gc, ok := e.(grpcCoder); ok {
		grpcCode = gc.GrpcCode()
	}
	st := status.New(grpcCode, e.Message())
	details := errkit.Encode(e)
	if len(details) == 0 {
		return st.Err()
	}
	v1Details := make([]protoadapt.MessageV1, 0, len(details))
	for _, d := range details {
		v1Details = append(v1Details, protoadapt.MessageV1Of(d))
	}
	withDetails, derr := st.WithDetails(v1Details...)
	if derr != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// GatewayErrorHandler 替换 grpc-gateway 默认错误处理器。
//
// 链路:err → errkit.FromGrpcError(errors.As + status details 解码,需应用 RegisterDecoder)
// → 决定 HTTP status → 用 errkit.Encode 拿 proto details 序列化为 JSON。
// 无 Encoder 时回退到 {code,message,metadata} 通用格式。
func GatewayErrorHandler(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	e, ok := errkit.FromGrpcError(err)
	if !ok {
		// 极端兜底:未注册 Decoder 或 details 缺失时按 grpc code 反推内置码,业务码无法恢复
		st, _ := status.FromError(err)
		log.Ctx(ctx).Warn().
			Str("path", r.URL.Path).
			Str("method", r.Method).
			Err(err).
			Msg("gateway received non-errkit error, fallback")
		e = errkit.New(errkit.FromGrpcCode(st.Code()), st.Message()).WithCause(err)
	}
	writeGatewayError(w, marshaler, e)
}

// writeGatewayError 优先走 Encoder 序列化,否则用 {code,message,metadata} 通用格式。
func writeGatewayError(w http.ResponseWriter, marshaler runtime.Marshaler, e errkit.Error) {
	httpStatus := errkit.ToHttpStatus(e.Code())
	if msgs := errkit.Encode(e); len(msgs) > 0 {
		writeErrorJson(w, marshaler, httpStatus, msgs[0])
		return
	}
	writeFallbackJson(w, httpStatus, e)
}

// writeErrorJson 把 proto 错误用 marshaler 序列化为 JSON 写到 HTTP 响应。
func writeErrorJson(w http.ResponseWriter, marshaler runtime.Marshaler, httpStatus int, msg proto.Message) {
	w.Header().Set("Content-Type", marshaler.ContentType(msg))
	w.WriteHeader(httpStatus)
	if buf, err := marshaler.Marshal(msg); err == nil {
		_, _ = w.Write(buf)
	}
}

// writeFallbackJson 当应用未注册 Encoder 时,输出通用 {code,message,metadata} JSON。
func writeFallbackJson(w http.ResponseWriter, httpStatus int, e errkit.Error) {
	body := map[string]any{
		"code":    int32(e.Code()),
		"message": e.Message(),
	}
	if md := e.Metadata(); len(md) > 0 {
		body["metadata"] = md
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(body)
}
