package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/toolbelts/forge/ioc"
)

// newProviderTestContext 构造带容器与给定配置键的最小测试 ctx。
func newProviderTestContext(t *testing.T, keys map[string]any) context.Context {
	t.Helper()

	ctx := ioc.New().WithContext(t.Context())
	v := viper.New()
	for key, value := range keys {
		v.Set(key, value)
	}
	ioc.FromContext(ctx).Instance(v)
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

	// 中间件必须先于 HttpProvider.Setup 登记。
	var order []string
	MustGetHttpMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))

	p := &HttpProvider{}
	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	t.Cleanup(func() { _ = p.listener.Close() })

	// 路由在 Setup 之后注册，注册时机与中间件包装无关。
	MustGetHttpMux(ctx).HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		_, _ = w.Write([]byte("pong"))
	})

	rec := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping", nil))

	if want := []string{"mw", "handler"}; !slices.Equal(order, want) {
		t.Fatalf("expected order %v, got %v", want, order)
	}
	if body := rec.Body.String(); body != "pong" {
		t.Fatalf("expected body pong, got %q", body)
	}
}

// setupGatewayProvider 用给定配置跑 GatewayProvider.Setup，测试结束时释放监听与反向连接。
func setupGatewayProvider(t *testing.T, ctx context.Context) *GatewayProvider {
	t.Helper()

	p := &GatewayProvider{}
	if err := p.Setup(ctx); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	t.Cleanup(func() {
		_ = p.listener.Close()
		_ = p.conn.Close()
	})
	return p
}

// TestGatewayProviderMiddleware 验证中间件包在 gateway runtime mux 外层，未匹配路由时仍先经过中间件。
func TestGatewayProviderMiddleware(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{
		"gateway.enabled":       true,
		"gateway.addr":          "127.0.0.1:0",
		"gateway.grpc_endpoint": "127.0.0.1:1",
	})

	var order []string
	MustGetGatewayMiddlewareChain(ctx).Use(appendMiddleware(&order, "mw"))
	p := setupGatewayProvider(t, ctx)

	rec := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nowhere", nil))

	if want := []string{"mw"}; !slices.Equal(order, want) {
		t.Fatalf("expected order %v, got %v", want, order)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
}

// TestGatewayProviderMiddlewareWithTrace 验证 trace 开启时 trace 提取在中间件之外的最外层：
// 中间件拿到的请求 ctx 已经接续了上游 traceparent。
func TestGatewayProviderMiddlewareWithTrace(t *testing.T) {
	old := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(old) })

	ctx := newProviderTestContext(t, map[string]any{
		"gateway.enabled":       true,
		"gateway.addr":          "127.0.0.1:0",
		"gateway.grpc_endpoint": "127.0.0.1:1",
		"trace.enabled":         true,
	})

	var traced bool
	MustGetGatewayMiddlewareChain(ctx).Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			traced = trace.SpanContextFromContext(r.Context()).IsValid()
			next.ServeHTTP(w, r)
		})
	})
	p := setupGatewayProvider(t, ctx)

	req := httptest.NewRequest(http.MethodGet, "/nowhere", nil)
	req.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	p.server.Handler.ServeHTTP(httptest.NewRecorder(), req)

	if !traced {
		t.Fatal("expected middleware to see the upstream trace context")
	}
}

// TestChainsAreCreatedOnDemand 验证链不依赖任何 Provider：谁先取谁创建，之后取到的是同一个实例。
func TestChainsAreCreatedOnDemand(t *testing.T) {
	ctx := newProviderTestContext(t, nil)

	httpChain := MustGetHttpMiddlewareChain(ctx)
	gatewayChain := MustGetGatewayMiddlewareChain(ctx)
	if httpChain == gatewayChain {
		t.Fatal("http and gateway chains must be distinct")
	}
	if MustGetHttpMiddlewareChain(ctx) != httpChain || MustGetGatewayMiddlewareChain(ctx) != gatewayChain {
		t.Fatal("expected the same middleware chain on repeated lookups")
	}
	if chain := MustGetInterceptorChain(ctx); MustGetInterceptorChain(ctx) != chain {
		t.Fatal("expected the same interceptor chain on repeated lookups")
	}

	// 不是容器派生的 ctx 取不到链，也不会 panic 在容器内部。
	if _, err := GetInterceptorChain(t.Context()); !errors.Is(err, ioc.ErrNoContainer) {
		t.Fatalf("err = %v, want ErrNoContainer", err)
	}
}

// TestServerProvidersDisabled 验证 server 关闭时 Setup 不监听、Serve 直接返回，中间件链仍可照常登记。
func TestServerProvidersDisabled(t *testing.T) {
	ctx := newProviderTestContext(t, map[string]any{
		"http.enabled":    false,
		"gateway.enabled": false,
		"grpc.enabled":    false,
	})
	MustGetHttpMiddlewareChain(ctx).Use(func(next http.Handler) http.Handler { return next })

	for name, p := range map[string]interface {
		ioc.Component
		ioc.Server
		ioc.Shutdowner
	}{
		"http":    &HttpProvider{},
		"gateway": &GatewayProvider{},
		"grpc":    &GrpcProvider{},
	} {
		if err := p.Setup(ctx); err != nil {
			t.Fatalf("%s setup failed: %v", name, err)
		}
		if err := p.Serve(ctx); err != nil {
			t.Fatalf("%s serve failed: %v", name, err)
		}
		if err := p.Shutdown(ctx); err != nil {
			t.Fatalf("%s shutdown failed: %v", name, err)
		}
	}
	if _, err := GetHttpMux(ctx); !errors.Is(err, ioc.ErrNotFound) {
		t.Fatalf("disabled http must not bind a mux, err = %v", err)
	}
}
