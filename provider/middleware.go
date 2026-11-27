package provider

import (
	"context"
	"net/http"
	"slices"
)

const (
	// httpMiddlewareChainName 是 HttpProvider 的 *MiddlewareChain 在容器中的命名 key。
	httpMiddlewareChainName = "http"
	// gatewayMiddlewareChainName 是 GatewayProvider 的 *MiddlewareChain 在容器中的命名 key。
	gatewayMiddlewareChainName = "gateway"
)

// Middleware 定义通用 HTTP 中间件，包装 http.Handler 返回新的 http.Handler。
type Middleware func(http.Handler) http.Handler

// MiddlewareChain 收集 HTTP 中间件，与 InterceptorChain 的 gRPC 拦截器收集机制对应。
//
// 编排约定：
//   - 链没有固定的创建者：第一次 MustGetHttpMiddlewareChain / MustGetGatewayMiddlewareChain 时
//     创建，以命名实例（"http" / "gateway"）绑定到容器，与对应 server 是否 enabled 无关
//   - 各中间件 Provider 在 Setup 中通过 chain.Use 添加自己的中间件
//   - HttpProvider / GatewayProvider 的 Setup 取出 chain，调 Handler() 包装各自 mux，
//     之后追加的中间件不生效 —— 中间件 Provider 必须注册在两者之前
//
// 执行顺序：Use 顺序 = chain 外层执行顺序，在 main.go 中越靠前的中间件 Provider 越外层。
// 中间件包装的是整个 server handler：http 场景下 pprof 等挂同一 mux 的路由同样被包裹；
// gateway 场景下 trace 提取（withTracePropagation）固定在链外最外层。
type MiddlewareChain struct {
	middlewares []Middleware
}

// Use 追加中间件。
func (c *MiddlewareChain) Use(middlewares ...Middleware) {
	c.middlewares = append(c.middlewares, middlewares...)
}

// Handler 用链上全部中间件包装 h，先 Use 的在最外层；空链原样返回 h。
func (c *MiddlewareChain) Handler(h http.Handler) http.Handler {
	for _, mw := range slices.Backward(c.middlewares) {
		h = mw(h)
	}
	return h
}

// GetHttpMiddlewareChain 从容器获取 HttpProvider 的 *MiddlewareChain，尚不存在时创建并绑定。
func GetHttpMiddlewareChain(ctx context.Context) (*MiddlewareChain, error) {
	return chainOf[MiddlewareChain](ctx, httpMiddlewareChainName)
}

// MustGetHttpMiddlewareChain 同 GetHttpMiddlewareChain，失败时 panic。
func MustGetHttpMiddlewareChain(ctx context.Context) *MiddlewareChain {
	return must(GetHttpMiddlewareChain(ctx))
}

// GetGatewayMiddlewareChain 从容器获取 GatewayProvider 的 *MiddlewareChain，尚不存在时创建并绑定。
func GetGatewayMiddlewareChain(ctx context.Context) (*MiddlewareChain, error) {
	return chainOf[MiddlewareChain](ctx, gatewayMiddlewareChainName)
}

// MustGetGatewayMiddlewareChain 同 GetGatewayMiddlewareChain，失败时 panic。
func MustGetGatewayMiddlewareChain(ctx context.Context) *MiddlewareChain {
	return must(GetGatewayMiddlewareChain(ctx))
}
