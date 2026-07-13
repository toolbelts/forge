package accesslog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/meta"
)

// capture 把 zerolog 写入的 JSON 行收集到 buffer,便于断言字段。
type capture struct {
	buf *bytes.Buffer
}

// newCapture 构造一对 (capture, ctx),ctx 已绑定写入 capture 的 zerolog logger。
func newCapture(t *testing.T) (*capture, context.Context) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := zerolog.New(buf)
	ctx := logger.WithContext(context.Background())
	return &capture{buf: buf}, ctx
}

// entries 解析 buffer 中的所有 JSON 行,返回字段 map 列表。
func (c *capture) entries(t *testing.T) []map[string]any {
	t.Helper()
	raw := bytes.TrimSpace(c.buf.Bytes())
	if len(raw) == 0 {
		return nil
	}
	var out []map[string]any
	for line := range bytes.SplitSeq(raw, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		out = append(out, entry)
	}
	return out
}

// only 断言 buffer 中正好一条日志,返回该条字段 map。
func (c *capture) only(t *testing.T) map[string]any {
	t.Helper()
	es := c.entries(t)
	if len(es) != 1 {
		t.Fatalf("want 1 log entry, got %d: %s", len(es), c.buf.String())
	}
	return es[0]
}

// fakeStream 实现 grpc.ServerStream 接口最小集合,供 stream 拦截器测试。
type fakeStream struct {
	ctx context.Context
}

func (s *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeStream) SetTrailer(metadata.MD)       {}
func (s *fakeStream) Context() context.Context     { return s.ctx }
func (s *fakeStream) SendMsg(any) error            { return nil }
func (s *fakeStream) RecvMsg(any) error            { return nil }

// fakeServerTransportStream 实现 grpc.ServerTransportStream,只保留 Method,
// 让 grpc.Method(ctx) 能在测试 ctx 中拿到 fullMethod。
type fakeServerTransportStream struct {
	method string
}

func (f *fakeServerTransportStream) Method() string               { return f.method }
func (f *fakeServerTransportStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerTransportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerTransportStream) SetTrailer(metadata.MD) error { return nil }

// withFullMethod 把 fullMethod 注入 ctx,模拟 grpc 框架在 server 入口写好的 transport stream。
func withFullMethod(ctx context.Context, method string) context.Context {
	return grpc.NewContextWithServerTransportStream(ctx, &fakeServerTransportStream{method: method})
}

// TestUnaryInterceptor_Success 验证成功路径:level=info、msg=success、必填字段齐、无 error_code。
func TestUnaryInterceptor_Success(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	resp, err := interceptor(ctx, "req-data", info, handler)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("resp = %v, want ok", resp)
	}

	e := c.only(t)
	if e["level"] != "info" {
		t.Errorf("level = %v, want info", e["level"])
	}
	if e["message"] != "grpc request success" {
		t.Errorf("message = %v, want grpc request success", e["message"])
	}
	if e["service"] != "svc.Foo" {
		t.Errorf("service = %v, want svc.Foo", e["service"])
	}
	if e["method"] != "Bar" {
		t.Errorf("method = %v, want Bar", e["method"])
	}
	if e["full_method"] != "/svc.Foo/Bar" {
		t.Errorf("full_method = %v", e["full_method"])
	}
	if _, has := e["error_code"]; has {
		t.Errorf("unexpected error_code on success path")
	}
	if _, has := e["spent"]; !has {
		t.Errorf("missing spent field")
	}
}

// TestUnaryInterceptor_BizError 验证业务错误:level=error、msg=failed、error_code/error_name 写入、不写 resp。
func TestUnaryInterceptor_BizError(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	bizErr := errkit.New(errkit.CodeResourceExhausted, "rate limit exceeded")
	handler := func(ctx context.Context, req any) (any, error) { return "leak", bizErr }

	_, err := interceptor(ctx, "req", info, handler)
	if !errors.Is(err, bizErr) {
		t.Fatalf("err = %v, want bizErr", err)
	}

	e := c.only(t)
	if e["level"] != "error" {
		t.Errorf("level = %v, want error", e["level"])
	}
	if e["message"] != "grpc request failed" {
		t.Errorf("message = %v, want grpc request failed", e["message"])
	}
	code, _ := e["error_code"].(float64)
	if int32(code) != int32(errkit.CodeResourceExhausted) {
		t.Errorf("error_code = %v, want %d", code, errkit.CodeResourceExhausted)
	}
	if e["error_name"] != "RESOURCE_EXHAUSTED" {
		t.Errorf("error_name = %v, want RESOURCE_EXHAUSTED", e["error_name"])
	}
	if _, has := e["resp"]; has {
		t.Errorf("unexpected resp on error path")
	}
	if _, has := e["resp_text"]; has {
		t.Errorf("unexpected resp_text on error path")
	}
}

// TestUnaryInterceptor_Slow 验证 spent > slow_threshold 且无错误时降级为 Warn。
func TestUnaryInterceptor_Slow(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithSlowThreshold(5 * time.Millisecond))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) {
		time.Sleep(15 * time.Millisecond)
		return "ok", nil
	}
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if e["level"] != "warn" {
		t.Errorf("level = %v, want warn", e["level"])
	}
	if e["message"] != "grpc request slow" {
		t.Errorf("message = %v, want grpc request slow", e["message"])
	}
}

// TestUnaryInterceptor_ErrorBeatsSlow 验证错误优先于慢:err 与 spent>slow 同时成立时仍打 Error/failed。
func TestUnaryInterceptor_ErrorBeatsSlow(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithSlowThreshold(time.Millisecond))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	bizErr := errkit.New(errkit.CodeInternal, "boom")
	handler := func(ctx context.Context, req any) (any, error) {
		time.Sleep(5 * time.Millisecond)
		return nil, bizErr
	}
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if e["level"] != "error" {
		t.Errorf("level = %v, want error", e["level"])
	}
	if e["message"] != "grpc request failed" {
		t.Errorf("message = %v, want grpc request failed", e["message"])
	}
}

// TestUnaryInterceptor_PayloadDisabled 验证 payload=off 时 req/resp 与 *_text 字段都不写。
func TestUnaryInterceptor_PayloadDisabled(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	for _, k := range []string{"req", "req_text", "resp", "resp_text"} {
		if _, has := e[k]; has {
			t.Errorf("unexpected %s field with payload=off", k)
		}
	}
}

// TestUnaryInterceptor_PayloadEnabled 验证 payload=on 时非 proto 值 fallback 到 fmt.Sprint,
// 落 *_text 字符串字段,req/resp 对象字段缺席。
func TestUnaryInterceptor_PayloadEnabled(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req-data", info, handler)

	e := c.only(t)
	if e["req_text"] != "req-data" {
		t.Errorf("req_text = %v, want req-data", e["req_text"])
	}
	if e["resp_text"] != "ok" {
		t.Errorf("resp_text = %v, want ok", e["resp_text"])
	}
	if _, has := e["req"]; has {
		t.Errorf("unexpected req object field for non-proto payload")
	}
	if _, has := e["resp"]; has {
		t.Errorf("unexpected resp object field for non-proto payload")
	}
}

// TestUnaryInterceptor_PayloadOmitsRespOnError 验证错误路径只写 req,不写 resp(避免泄漏 zero value/未初始化响应)。
func TestUnaryInterceptor_PayloadOmitsRespOnError(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	bizErr := errkit.New(errkit.CodeInternal, "boom")
	handler := func(ctx context.Context, req any) (any, error) { return "leak", bizErr }
	_, _ = interceptor(ctx, "req-data", info, handler)

	e := c.only(t)
	if e["req_text"] != "req-data" {
		t.Errorf("req_text = %v, want req-data", e["req_text"])
	}
	if _, has := e["resp"]; has {
		t.Errorf("unexpected resp on error path")
	}
	if _, has := e["resp_text"]; has {
		t.Errorf("unexpected resp_text on error path")
	}
}

// TestUnaryInterceptor_PayloadTruncate 验证字节级截断,head <= maxBytes 且尾部追加 truncatedSuffix,
// 截断值落 req_text 字符串字段。
func TestUnaryInterceptor_PayloadTruncate(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 8))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	longReq := strings.Repeat("abcd", 32)
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, longReq, info, handler)

	e := c.only(t)
	req, _ := e["req_text"].(string)
	if !strings.HasSuffix(req, truncatedSuffix) {
		t.Errorf("req_text=%q, want suffix %q", req, truncatedSuffix)
	}
	head := strings.TrimSuffix(req, truncatedSuffix)
	if len(head) > 8 {
		t.Errorf("head len=%d, want <=8 (got %q)", len(head), head)
	}
}

// TestUnaryInterceptor_PayloadProtoMessage 验证 proto.Message 走 protojson 序列化(字段名遵循
// UseProtoNames),以 RawJSON 嵌套对象形态写入 req/resp。protojson 输出含 detrand 随机空格,
// 断言必须解析回对象比较,不做字符串匹配。
func TestUnaryInterceptor_PayloadProtoMessage(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Code: 42, Message: "hi"}
	handler := func(ctx context.Context, _ any) (any, error) { return req, nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	for _, key := range []string{"req", "resp"} {
		m, ok := e[key].(map[string]any)
		if !ok {
			t.Fatalf("%s = %T(%v), want JSON object", key, e[key], e[key])
		}
		if m["code"] != float64(42) {
			t.Errorf("%s.code = %v, want 42", key, m["code"])
		}
		if m["message"] != "hi" {
			t.Errorf("%s.message = %v, want hi", key, m["message"])
		}
	}
	if _, has := e["req_text"]; has {
		t.Errorf("unexpected req_text alongside req object")
	}
	if _, has := e["resp_text"]; has {
		t.Errorf("unexpected resp_text alongside resp object")
	}
}

// TestUnaryInterceptor_SkipsByFullMethod 验证用 gRPC FullMethod 命中 skips 时既不打日志也仍调 handler。
func TestUnaryInterceptor_SkipsByFullMethod(t *testing.T) {
	c, ctx := newCapture(t)
	ctx = withFullMethod(ctx, "/grpc.health.v1.Health/Check")
	interceptor := UnaryInterceptor(WithSkips([]string{"/grpc.health.v1.Health/Check"}))
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	called := false
	handler := func(ctx context.Context, req any) (any, error) { called = true; return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	if !called {
		t.Errorf("handler not called on skip path")
	}
	if got := bytes.TrimSpace(c.buf.Bytes()); len(got) != 0 {
		t.Errorf("expected no log on skip, got %q", got)
	}
}

// TestUnaryInterceptor_SkipsByHttpPath 验证用 HTTP path 命中 skips 时也跳过(经 gateway 的请求)。
// fullMethod 在 ctx 里但不在 skips 列表,path 在列表 → 仍命中。
func TestUnaryInterceptor_SkipsByHttpPath(t *testing.T) {
	c, ctx := newCapture(t)
	ctx = withFullMethod(ctx, "/api.v1.Auth/Login")
	ctx = meta.Set(ctx, meta.MetaRequestPath, "/v1/auth/login")
	interceptor := UnaryInterceptor(WithSkips([]string{"/v1/auth/login"}))
	info := &grpc.UnaryServerInfo{FullMethod: "/api.v1.Auth/Login"}
	called := false
	handler := func(ctx context.Context, req any) (any, error) { called = true; return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	if !called {
		t.Errorf("handler not called on skip path")
	}
	if got := bytes.TrimSpace(c.buf.Bytes()); len(got) != 0 {
		t.Errorf("expected no log on skip, got %q", got)
	}
}

// TestUnaryInterceptor_UserIdFromMeta 验证 meta 中有 user_id 时字段写入(int64)。
func TestUnaryInterceptor_UserIdFromMeta(t *testing.T) {
	c, ctx := newCapture(t)
	ctx = meta.Set(ctx, meta.MetaUserId, int64(12345))
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	uid, _ := e["user_id"].(float64)
	if int64(uid) != 12345 {
		t.Errorf("user_id = %v, want 12345", e["user_id"])
	}
}

// TestUnaryInterceptor_UserIdFromInnerMetaSet 验证外层 accesslog 能看到内层拦截器写入的 user_id。
func TestUnaryInterceptor_UserIdFromInnerMetaSet(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) {
		_ = meta.Set(ctx, meta.MetaUserId, int64(67890))
		return "ok", nil
	}
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	uid, _ := e["user_id"].(float64)
	if int64(uid) != 67890 {
		t.Errorf("user_id = %v, want 67890", e["user_id"])
	}
}

// TestUnaryInterceptor_NoUserIdWhenAbsent 验证 meta 无 user_id 时不写字段(保持日志干净)。
func TestUnaryInterceptor_NoUserIdWhenAbsent(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if _, has := e["user_id"]; has {
		t.Errorf("unexpected user_id when meta empty")
	}
}

// TestUnaryInterceptor_TraceIdFromSpan 验证 OTel SpanContext 有效时 trace_id/span_id 写入。
func TestUnaryInterceptor_TraceIdFromSpan(t *testing.T) {
	c, ctx := newCapture(t)
	tid, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex("0102030405060708")
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid})
	ctx = trace.ContextWithSpanContext(ctx, sc)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if e["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id = %v", e["trace_id"])
	}
	if e["span_id"] != "0102030405060708" {
		t.Errorf("span_id = %v", e["span_id"])
	}
}

// TestUnaryInterceptor_NoTraceIdWhenSpanInvalid 验证默认 ctx 无 span 时不写 trace_id。
func TestUnaryInterceptor_NoTraceIdWhenSpanInvalid(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if _, has := e["trace_id"]; has {
		t.Errorf("unexpected trace_id when span invalid")
	}
	if _, has := e["span_id"]; has {
		t.Errorf("unexpected span_id when span invalid")
	}
}

// TestUnaryInterceptor_PanicAfterRecovery 模拟 RecoveryProvider 已把 panic 转成 BizError 的场景,
// AccessLog 看到 error_code=PANIC,自身不再额外处理 panic,与 Recovery 职责互不重复。
func TestUnaryInterceptor_PanicAfterRecovery(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, errkit.New(errkit.CodePanic, "internal panic")
	}
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if e["error_name"] != "PANIC" {
		t.Errorf("error_name = %v, want PANIC", e["error_name"])
	}
	if e["level"] != "error" {
		t.Errorf("level = %v, want error", e["level"])
	}
}

// TestStreamInterceptor_Success 验证流式成功路径:msg=success、is_client/server_stream 与 info 一致、不写 unary 专属字段。
func TestStreamInterceptor_Success(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := StreamInterceptor()
	info := &grpc.StreamServerInfo{FullMethod: "/svc.Foo/Stream", IsClientStream: true, IsServerStream: false}
	ss := &fakeStream{ctx: ctx}
	handler := func(srv any, stream grpc.ServerStream) error { return nil }

	if err := interceptor(nil, ss, info, handler); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	e := c.only(t)
	if e["level"] != "info" {
		t.Errorf("level = %v, want info", e["level"])
	}
	if e["message"] != "grpc stream success" {
		t.Errorf("message = %v, want grpc stream success", e["message"])
	}
	if e["is_client_stream"] != true {
		t.Errorf("is_client_stream = %v, want true", e["is_client_stream"])
	}
	if e["is_server_stream"] != false {
		t.Errorf("is_server_stream = %v, want false", e["is_server_stream"])
	}
	if _, has := e["req"]; has {
		t.Errorf("unexpected req in stream log")
	}
	if _, has := e["req_text"]; has {
		t.Errorf("unexpected req_text in stream log")
	}
	if _, has := e["http_method"]; has {
		t.Errorf("unexpected http_method in stream log")
	}
}

// TestStreamInterceptor_Error 验证流式错误路径:level=error、msg=failed、error_code/error_name 写入。
func TestStreamInterceptor_Error(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := StreamInterceptor()
	info := &grpc.StreamServerInfo{FullMethod: "/svc.Foo/Stream"}
	ss := &fakeStream{ctx: ctx}
	bizErr := errkit.New(errkit.CodeInternal, "boom")
	handler := func(srv any, stream grpc.ServerStream) error { return bizErr }

	if err := interceptor(nil, ss, info, handler); !errors.Is(err, bizErr) {
		t.Fatalf("err = %v, want bizErr", err)
	}

	e := c.only(t)
	if e["level"] != "error" {
		t.Errorf("level = %v, want error", e["level"])
	}
	if e["message"] != "grpc stream failed" {
		t.Errorf("message = %v, want grpc stream failed", e["message"])
	}
	if e["error_name"] != "INTERNAL" {
		t.Errorf("error_name = %v, want INTERNAL", e["error_name"])
	}
}

// TestStreamInterceptor_SkipsByFullMethod 验证用 gRPC FullMethod 命中 skips 时不打日志,handler 仍正常调用。
func TestStreamInterceptor_SkipsByFullMethod(t *testing.T) {
	c, ctx := newCapture(t)
	ctx = withFullMethod(ctx, "/svc.Foo/Stream")
	interceptor := StreamInterceptor(WithSkips([]string{"/svc.Foo/Stream"}))
	info := &grpc.StreamServerInfo{FullMethod: "/svc.Foo/Stream"}
	ss := &fakeStream{ctx: ctx}
	called := false
	handler := func(srv any, stream grpc.ServerStream) error { called = true; return nil }
	_ = interceptor(nil, ss, info, handler)

	if !called {
		t.Errorf("handler not called on skip path")
	}
	if len(bytes.TrimSpace(c.buf.Bytes())) != 0 {
		t.Errorf("expected no log on skip, got %q", c.buf.String())
	}
}

// TestStreamInterceptor_SkipsByHttpPath 验证用 HTTP path 命中 skips 时也跳过(经 gateway 的请求)。
func TestStreamInterceptor_SkipsByHttpPath(t *testing.T) {
	c, ctx := newCapture(t)
	ctx = withFullMethod(ctx, "/svc.Foo/Stream")
	ctx = meta.Set(ctx, meta.MetaRequestPath, "/v1/events/stream")
	interceptor := StreamInterceptor(WithSkips([]string{"/v1/events/stream"}))
	info := &grpc.StreamServerInfo{FullMethod: "/svc.Foo/Stream"}
	ss := &fakeStream{ctx: ctx}
	called := false
	handler := func(srv any, stream grpc.ServerStream) error { called = true; return nil }
	_ = interceptor(nil, ss, info, handler)

	if !called {
		t.Errorf("handler not called on skip path")
	}
	if len(bytes.TrimSpace(c.buf.Bytes())) != 0 {
		t.Errorf("expected no log on skip, got %q", c.buf.String())
	}
}

// TestSplitFullMethod 覆盖 splitFullMethod 的常见与边界输入。
func TestSplitFullMethod(t *testing.T) {
	cases := []struct {
		in, svc, mth string
	}{
		{"/svc.pkg.Service/Method", "svc.pkg.Service", "Method"},
		{"svc/Method", "svc", "Method"},
		{"NoSlash", "NoSlash", ""},
		{"/Only", "Only", ""},
		{"", "", ""},
	}
	for _, tc := range cases {
		s, m := splitFullMethod(tc.in)
		if s != tc.svc || m != tc.mth {
			t.Errorf("splitFullMethod(%q) = (%q, %q), want (%q, %q)", tc.in, s, m, tc.svc, tc.mth)
		}
	}
}

// TestTruncate 覆盖字节级截断与 UTF-8 半 rune 修复。
func TestTruncate(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"under", "hello", 100, "hello"},
		{"unlimited_zero", "hello world", 0, "hello world"},
		{"unlimited_negative", "hello world", -1, "hello world"},
		{"truncate_ascii", "abcdefgh", 4, "abcd" + truncatedSuffix},
		// "你好世界" UTF-8 为 12 字节(3+3+3+3),max=4 截断后头 4 字节是 "你" + 半个 "好",
		// ToValidUTF8 把半个 rune 移除,head 变 "你"。
		{"truncate_utf8", "你好世界", 4, "你" + truncatedSuffix},
	}
	for _, tc := range cases {
		got := truncate(tc.in, tc.max)
		if got != tc.want {
			t.Errorf("%s: truncate(%q, %d) = %q, want %q", tc.name, tc.in, tc.max, got, tc.want)
		}
	}
}

// TestApplyMask 表格驱动覆盖脱敏的核心场景:命中替换、嵌套、数组、null 保持、空 mask、非命中。
func TestApplyMask(t *testing.T) {
	cases := []struct {
		name string
		in   string
		mask []string
		want string
	}{
		{
			name: "top_level_hit",
			in:   `{"password":"p","email":"a@b.c"}`,
			mask: []string{"password"},
			want: `{"email":"a@b.c","password":"***"}`,
		},
		{
			name: "nested_hit",
			in:   `{"profile":{"password":"p","name":"x"}}`,
			mask: []string{"password"},
			want: `{"profile":{"name":"x","password":"***"}}`,
		},
		{
			name: "array_of_objects",
			in:   `{"items":[{"password":"a"},{"password":"b"}]}`,
			mask: []string{"password"},
			want: `{"items":[{"password":"***"},{"password":"***"}]}`,
		},
		{
			name: "null_preserved",
			in:   `{"password":null}`,
			mask: []string{"password"},
			want: `{"password":null}`,
		},
		{
			name: "non_match_unchanged",
			in:   `{"email":"a@b.c","user_id":1}`,
			mask: []string{"password"},
			want: `{"email":"a@b.c","user_id":1}`,
		},
		{
			name: "multiple_keys",
			in:   `{"old_password":"o","new_password":"n","keep":"v"}`,
			mask: []string{"old_password", "new_password"},
			want: `{"keep":"v","new_password":"***","old_password":"***"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set := make(map[string]struct{}, len(tc.mask))
			for _, k := range tc.mask {
				set[k] = struct{}{}
			}
			got := applyMask([]byte(tc.in), set)
			// 用 unmarshal 回 map 比较,绕开 key 顺序差异。
			var gotV, wantV any
			if err := json.Unmarshal(got, &gotV); err != nil {
				t.Fatalf("unmarshal got: %v (raw=%s)", err, got)
			}
			if err := json.Unmarshal([]byte(tc.want), &wantV); err != nil {
				t.Fatalf("unmarshal want: %v", err)
			}
			gotJ, _ := json.Marshal(gotV)
			wantJ, _ := json.Marshal(wantV)
			if string(gotJ) != string(wantJ) {
				t.Errorf("got=%s want=%s", gotJ, wantJ)
			}
		})
	}
}

// TestApplyMask_MalformedJsonPassthrough 非法 JSON 时返回原 buf,不阻塞日志主链路。
func TestApplyMask_MalformedJsonPassthrough(t *testing.T) {
	in := []byte(`{"password":`)
	mask := map[string]struct{}{"password": {}}
	got := applyMask(in, mask)
	if string(got) != string(in) {
		t.Errorf("got=%q want=%q", got, in)
	}
}

// TestUnaryInterceptor_MaskFields 集成路径:WithMaskFields 配置后,protojson 摘要的命中字段被替换为 "***"。
// 这里借用 *statuspb.Status 的 message 字段做集成验证 — 不引入新 proto 依赖。
func TestUnaryInterceptor_MaskFields(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0), WithMaskFields([]string{"message"}))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Code: 42, Message: "secret-payload"}
	handler := func(ctx context.Context, _ any) (any, error) { return req, nil }
	_, _ = interceptor(ctx, req, info, handler)

	// 泄漏检查对整行日志做,比只查 req 字段更强(mask 失效的任何形态都会被抓到)。
	if strings.Contains(c.buf.String(), "secret-payload") {
		t.Errorf("log line leaked masked value: %s", c.buf.String())
	}
	e := c.only(t)
	m, ok := e["req"].(map[string]any)
	if !ok {
		t.Fatalf("req = %T(%v), want JSON object", e["req"], e["req"])
	}
	if m["message"] != maskPlaceholder {
		t.Errorf("req.message = %v, want %q", m["message"], maskPlaceholder)
	}
	if m["code"] != float64(42) {
		t.Errorf("req lost non-masked field code: %v", m["code"])
	}
}

// TestUnaryInterceptor_MaskFields_EmptyNoOp 空 mask 列表时 payload 与未启用一致(快速路径)。
func TestUnaryInterceptor_MaskFields_EmptyNoOp(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0), WithMaskFields(nil))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Code: 1, Message: "hi"}
	handler := func(ctx context.Context, _ any) (any, error) { return req, nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	m, ok := e["req"].(map[string]any)
	if !ok {
		t.Fatalf("req = %T(%v), want JSON object", e["req"], e["req"])
	}
	if m["message"] != "hi" {
		t.Errorf("empty mask should leave value intact: %v", m["message"])
	}
}

// TestMarshalPayload 单元覆盖判别式决策流的各分支归属:omit / text / json。
func TestMarshalPayload(t *testing.T) {
	tooLarge := &statuspb.Status{Message: strings.Repeat("x", 10000)}
	cases := []struct {
		name     string
		v        any
		opts     []Option
		wantKind payloadKind
		check    func(t *testing.T, pv payloadValue)
	}{
		{
			name:     "nil_omit",
			v:        nil,
			wantKind: payloadOmit,
		},
		{
			name:     "non_proto_text",
			v:        "hello",
			wantKind: payloadText,
			check: func(t *testing.T, pv payloadValue) {
				if pv.text != "hello" {
					t.Errorf("text = %q, want hello", pv.text)
				}
			},
		},
		{
			name:     "small_proto_json_object",
			v:        &statuspb.Status{Code: 42, Message: "hi"},
			wantKind: payloadJson,
			check: func(t *testing.T, pv payloadValue) {
				var m map[string]any
				if err := json.Unmarshal(pv.raw, &m); err != nil {
					t.Fatalf("raw not valid JSON: %v (raw=%s)", err, pv.raw)
				}
				if m["code"] != float64(42) || m["message"] != "hi" {
					t.Errorf("raw = %s, want code=42 message=hi", pv.raw)
				}
			},
		},
		{
			// proto3 string 字段必须是合法 UTF-8,protojson 对脏字节直接报错,走占位符分支。
			name:     "invalid_utf8_marshal_error",
			v:        &statuspb.Status{Message: string([]byte{0xff, 0xfe})},
			wantKind: payloadText,
			check: func(t *testing.T, pv payloadValue) {
				if !strings.HasPrefix(pv.text, "<marshal error") {
					t.Errorf("text = %q, want <marshal error prefix", pv.text)
				}
			},
		},
		{
			name:     "too_large_placeholder",
			v:        tooLarge,
			wantKind: payloadText,
			check: func(t *testing.T, pv payloadValue) {
				want := fmt.Sprintf(tooLargeFmt, tooLarge.ProtoReflect().Descriptor().FullName(), proto.Size(tooLarge))
				if pv.text != want {
					t.Errorf("text = %q, want %q", pv.text, want)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := buildOptions(tc.opts...)
			pv := marshalPayload(tc.v, &o)
			if pv.kind != tc.wantKind {
				t.Fatalf("kind = %d, want %d (pv=%+v)", pv.kind, tc.wantKind, pv)
			}
			if tc.check != nil {
				tc.check(t, pv)
			}
		})
	}
}

// TestMarshalPayload_WrapperNonObject wrapper 类型顶层输出是 JSON 标量而非对象,
// 必须落 text 分支,维持 req/resp 字段"恒为对象"的形态不变量。
func TestMarshalPayload_WrapperNonObject(t *testing.T) {
	o := buildOptions()
	pv := marshalPayload(wrapperspb.String("hi"), &o)
	if pv.kind != payloadText {
		t.Fatalf("kind = %d, want payloadText (pv=%+v)", pv.kind, pv)
	}
	if pv.text != `"hi"` {
		t.Errorf("text = %q, want %q", pv.text, `"hi"`)
	}
}

// TestUnaryInterceptor_PayloadTooLarge 超过 wire size 预检阈值(自动派生 4×maxBytes)的消息
// 跳过序列化,req_text 记占位符,req 对象字段缺席。
func TestUnaryInterceptor_PayloadTooLarge(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 8)) // 阈值 = 4×8 = 32
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Message: strings.Repeat("a", 100)}
	handler := func(ctx context.Context, _ any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	reqText, _ := e["req_text"].(string)
	if !strings.HasPrefix(reqText, "<payload too large: ") {
		t.Errorf("req_text = %q, want too-large placeholder", reqText)
	}
	if !strings.Contains(reqText, "google.rpc.Status") {
		t.Errorf("req_text = %q, want message full name", reqText)
	}
	if _, has := e["req"]; has {
		t.Errorf("unexpected req object field for too-large payload")
	}
}

// TestUnaryInterceptor_ProtoTruncateGoesText 预检通过但 JSON 输出超 maxBytes 的消息,
// 截断前缀落 req_text(截断后的 JSON 片段非法,不能 RawJSON 嵌入)。
func TestUnaryInterceptor_ProtoTruncateGoesText(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 32)) // 阈值 = 128
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Code: 42, Message: strings.Repeat("m", 60)} // wire ≈ 66 过预检,JSON ≈ 85 > 32
	handler := func(ctx context.Context, _ any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	reqText, _ := e["req_text"].(string)
	if !strings.HasSuffix(reqText, truncatedSuffix) {
		t.Errorf("req_text = %q, want suffix %q", reqText, truncatedSuffix)
	}
	if strings.HasPrefix(reqText, "<payload too large") {
		t.Errorf("req_text = %q, should not hit size precheck", reqText)
	}
	if _, has := e["req"]; has {
		t.Errorf("unexpected req object field for truncated payload")
	}
}

// TestUnaryInterceptor_PayloadNoTruncateNegativeMax maxBytes < 0(要完整 payload)时
// 自动模式下预检与截断都关闭,大消息以完整对象写入 req。
func TestUnaryInterceptor_PayloadNoTruncateNegativeMax(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, -1))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	msg := strings.Repeat("z", 5000)
	req := &statuspb.Status{Message: msg}
	handler := func(ctx context.Context, _ any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	m, ok := e["req"].(map[string]any)
	if !ok {
		t.Fatalf("req = %T, want JSON object", e["req"])
	}
	if m["message"] != msg {
		t.Errorf("req.message length = %d, want %d (payload should be complete)", len(m["message"].(string)), len(msg))
	}
}

// TestWithPayloadSizeLimit_Explicit 显式 payload_size_limit 与自动派生的解耦:
// 正值在 maxBytes<0 下仍生效("不截断但设防"),负值强制禁用预检。
func TestWithPayloadSizeLimit_Explicit(t *testing.T) {
	t.Run("limit_with_unbounded_max_bytes", func(t *testing.T) {
		c, ctx := newCapture(t)
		interceptor := UnaryInterceptor(WithPayload(true, -1), WithPayloadSizeLimit(64))
		info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
		req := &statuspb.Status{Message: strings.Repeat("a", 100)}
		handler := func(ctx context.Context, _ any) (any, error) { return "ok", nil }
		_, _ = interceptor(ctx, req, info, handler)

		e := c.only(t)
		reqText, _ := e["req_text"].(string)
		if !strings.HasPrefix(reqText, "<payload too large: ") {
			t.Errorf("req_text = %q, want too-large placeholder", reqText)
		}
	})
	t.Run("disable_precheck", func(t *testing.T) {
		c, ctx := newCapture(t)
		interceptor := UnaryInterceptor(WithPayload(true, 0), WithPayloadSizeLimit(-1))
		info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
		req := &statuspb.Status{Message: strings.Repeat("a", 10000)}
		handler := func(ctx context.Context, _ any) (any, error) { return "ok", nil }
		_, _ = interceptor(ctx, req, info, handler)

		e := c.only(t)
		reqText, _ := e["req_text"].(string)
		if !strings.HasSuffix(reqText, truncatedSuffix) {
			t.Errorf("req_text = %q, want truncated payload (precheck disabled)", reqText)
		}
		if strings.HasPrefix(reqText, "<payload too large") {
			t.Errorf("req_text = %q, precheck should be disabled", reqText)
		}
	})
}

// TestOptionsResolveSizeLimit 覆盖 sizeLimit 的派生规则与 Option 顺序无关性。
func TestOptionsResolveSizeLimit(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
		want int
	}{
		{"default_auto", nil, 4 * 2048},
		{"explicit_max_bytes_auto", []Option{WithPayload(true, 100)}, 400},
		{"explicit_limit", []Option{WithPayloadSizeLimit(100)}, 100},
		{"disable_limit", []Option{WithPayloadSizeLimit(-1)}, 0},
		{"unbounded_max_bytes_auto_off", []Option{WithPayload(true, -1)}, 0},
		{"unbounded_max_bytes_explicit_limit", []Option{WithPayload(true, -1), WithPayloadSizeLimit(64)}, 64},
		{"order_independent", []Option{WithPayloadSizeLimit(64), WithPayload(true, -1)}, 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := buildOptions(tc.opts...)
			if o.sizeLimit != tc.want {
				t.Errorf("sizeLimit = %d, want %d", o.sizeLimit, tc.want)
			}
		})
	}
}

// TestMaskProbeHit 覆盖 probe 预检:命中、未命中,以及"值整体恰等于 key"的可接受误报
// (代价仅是白做一次 applyMask 往返,无正确性影响)。
func TestMaskProbeHit(t *testing.T) {
	probes := [][]byte{[]byte(`"password"`)}
	cases := []struct {
		name string
		buf  string
		want bool
	}{
		{"key_hit", `{"password":"x"}`, true},
		{"miss", `{"email":"a@b.c"}`, false},
		{"value_equals_key_false_positive", `{"type":"password"}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskProbeHit([]byte(tc.buf), probes); got != tc.want {
				t.Errorf("maskProbeHit(%q) = %v, want %v", tc.buf, got, tc.want)
			}
		})
	}
}

// TestUnaryInterceptor_MaskMissUnchanged mask 配置了但 payload 不含命中字段时,
// 短路路径必须产出与无 mask 完全一致的对象(证明 probe 预检不改变行为)。
func TestUnaryInterceptor_MaskMissUnchanged(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0), WithMaskFields([]string{"password"}))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Code: 7, Message: "plain"}
	handler := func(ctx context.Context, _ any) (any, error) { return req, nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	m, ok := e["req"].(map[string]any)
	if !ok {
		t.Fatalf("req = %T, want JSON object", e["req"])
	}
	if m["code"] != float64(7) || m["message"] != "plain" {
		t.Errorf("req = %v, want untouched {code:7, message:plain}", m)
	}
}

// TestUnaryInterceptor_PayloadSpecialChars RawJSON 直插的合法性专项:引号/换行/制表符/中文
// 经 protojson 转义后整行日志仍可解析,且值精确回环。
func TestUnaryInterceptor_PayloadSpecialChars(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	msg := "带\"引号\"\n换行\t制表和中文"
	req := &statuspb.Status{Message: msg}
	handler := func(ctx context.Context, _ any) (any, error) { return req, nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t) // c.only 内部对整行 json.Unmarshal,即 RawJSON 合法性校验
	m, ok := e["req"].(map[string]any)
	if !ok {
		t.Fatalf("req = %T, want JSON object", e["req"])
	}
	if m["message"] != msg {
		t.Errorf("req.message = %q, want %q (round-trip)", m["message"], msg)
	}
}
