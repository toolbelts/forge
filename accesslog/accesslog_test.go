package accesslog

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/rs/zerolog"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

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

// TestUnaryInterceptor_PayloadDisabled 验证 payload=off 时 req/resp 字段都不写。
func TestUnaryInterceptor_PayloadDisabled(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(false, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req", info, handler)

	e := c.only(t)
	if _, has := e["req"]; has {
		t.Errorf("unexpected req field with payload=off")
	}
	if _, has := e["resp"]; has {
		t.Errorf("unexpected resp field with payload=off")
	}
}

// TestUnaryInterceptor_PayloadEnabled 验证 payload=on 时 req/resp 字段写入(非 proto fallback 到 fmt.Sprint)。
func TestUnaryInterceptor_PayloadEnabled(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, "req-data", info, handler)

	e := c.only(t)
	if e["req"] != "req-data" {
		t.Errorf("req = %v, want req-data", e["req"])
	}
	if e["resp"] != "ok" {
		t.Errorf("resp = %v, want ok", e["resp"])
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
	if e["req"] != "req-data" {
		t.Errorf("req = %v, want req-data", e["req"])
	}
	if _, has := e["resp"]; has {
		t.Errorf("unexpected resp on error path")
	}
}

// TestUnaryInterceptor_PayloadTruncate 验证字节级截断,head <= maxBytes 且尾部追加 truncatedSuffix。
func TestUnaryInterceptor_PayloadTruncate(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 8))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	longReq := strings.Repeat("abcd", 32)
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, _ = interceptor(ctx, longReq, info, handler)

	e := c.only(t)
	req, _ := e["req"].(string)
	if !strings.HasSuffix(req, truncatedSuffix) {
		t.Errorf("req=%q, want suffix %q", req, truncatedSuffix)
	}
	head := strings.TrimSuffix(req, truncatedSuffix)
	if len(head) > 8 {
		t.Errorf("head len=%d, want <=8 (got %q)", len(head), head)
	}
}

// TestUnaryInterceptor_PayloadProtoMessage 验证 proto.Message 走 protojson 序列化(字段名遵循 UseProtoNames)。
func TestUnaryInterceptor_PayloadProtoMessage(t *testing.T) {
	c, ctx := newCapture(t)
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	req := &statuspb.Status{Code: 42, Message: "hi"}
	handler := func(ctx context.Context, _ any) (any, error) { return req, nil }
	_, _ = interceptor(ctx, req, info, handler)

	e := c.only(t)
	reqStr, _ := e["req"].(string)
	if !strings.Contains(reqStr, `"code":42`) || !strings.Contains(reqStr, `"message":"hi"`) {
		t.Errorf("req payload not protojson: %q", reqStr)
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
