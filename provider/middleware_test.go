package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// newProviderTestContext 构造带容器与给定配置键的最小测试 ctx。
func newProviderTestContext(t *testing.T, keys map[string]any) context.Context {
	t.Helper()

	ctx := ioc.NewContainer().WithContext(context.Background())
	v := viper.New()
	for key, value := range keys {
		v.Set(key, value)
	}
	ioc.MustInstance(ctx, v)
	return ctx
}

// appendMiddleware 返回把 name 记入 order 后继续调用内层 handler 的记录型中间件。
func appendMiddleware(order *[]string, name string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*order = append(*order, name)
			next.ServeHTTP(w, r)
		})
	}
}

// TestMiddlewareChainHandlerOrder 验证先 Use 的中间件在最外层执行。
func TestMiddlewareChainHandlerOrder(t *testing.T) {
	var order []string
	chain := &MiddlewareChain{}
	chain.Use(appendMiddleware(&order, "a"))
	chain.Use(appendMiddleware(&order, "b"))

	handler := chain.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if want := []string{"a", "b", "handler"}; !slices.Equal(order, want) {
		t.Fatalf("expected order %v, got %v", want, order)
	}
}

// TestMiddlewareChainEmptyReturnsSameHandler 验证空链原样返回传入的 handler。
func TestMiddlewareChainEmptyReturnsSameHandler(t *testing.T) {
	mux := http.NewServeMux()
	chain := &MiddlewareChain{}

	if got := chain.Handler(mux); got != http.Handler(mux) {
		t.Fatalf("expected same handler, got %#v", got)
	}
}

// TestHttpProviderMiddleware 验证 http server handler 被中间件按序包裹且路由仍生效。
func TestHttpProviderMiddleware(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{
		"http.enabled": true,
		"http.addr":    "127.0.0.1:0",
	})

	p := &HttpProvider{}
	if err := p.Register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() { _ = p.listener.Close() })

	var order []string
	MustGetHttpMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))
	MustGetHttpMux(ctx).HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		_, _ = w.Write([]byte("pong"))
	})

	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	rec := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping", nil))

	if want := []string{"mw", "handler"}; !slices.Equal(order, want) {
		t.Fatalf("expected order %v, got %v", want, order)
	}
	if body := rec.Body.String(); body != "pong" {
		t.Fatalf("expected body pong, got %q", body)
	}
}

// TestGatewayProviderMiddleware 验证中间件包在 gateway runtime mux 外层，未匹配路由时仍先经过中间件。
func TestGatewayProviderMiddleware(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{
		"gateway.enabled":       true,
		"gateway.addr":          "127.0.0.1:0",
		"gateway.grpc_endpoint": "127.0.0.1:1",
	})

	p := &GatewayProvider{}
	if err := p.Register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = p.listener.Close()
		_ = p.conn.Close()
	})

	var order []string
	MustGetGatewayMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))

	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	rec := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nowhere", nil))

	if want := []string{"mw"}; !slices.Equal(order, want) {
		t.Fatalf("expected order %v, got %v", want, order)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
}

// TestGatewayProviderMiddlewareWithTrace 验证 trace 开启时 Setup 正常组装且中间件仍执行。
func TestGatewayProviderMiddlewareWithTrace(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{
		"gateway.enabled":       true,
		"gateway.addr":          "127.0.0.1:0",
		"gateway.grpc_endpoint": "127.0.0.1:1",
		"trace.enabled":         true,
	})

	p := &GatewayProvider{}
	if err := p.Register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = p.listener.Close()
		_ = p.conn.Close()
	})
	if !p.traceEnabled {
		t.Fatal("expected trace enabled")
	}

	var order []string
	MustGetGatewayMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))

	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	rec := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nowhere", nil))

	if want := []string{"mw"}; !slices.Equal(order, want) {
		t.Fatalf("expected order %v, got %v", want, order)
	}
}

// TestHttpProviderDisabledStillProvidesMiddlewareChain 验证 http 关闭时链仍注入且 Setup 无操作。
func TestHttpProviderDisabledStillProvidesMiddlewareChain(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{"http.enabled": false})

	p := &HttpProvider{}
	if err := p.Register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	var order []string
	MustGetHttpMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))

	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
}

// TestGatewayProviderDisabledStillProvidesMiddlewareChain 验证 gateway 关闭时链仍注入且 Setup 无操作。
func TestGatewayProviderDisabledStillProvidesMiddlewareChain(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{"gateway.enabled": false})

	p := &GatewayProvider{}
	if err := p.Register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	var order []string
	MustGetGatewayMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))

	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
}
