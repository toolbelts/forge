package provider

import (
	"context"
	"net/http"

	"github.com/toolbelts/forge/ioc"
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
//   - HttpProvider / GatewayProvider 的 Register 阶段各自构造 *MiddlewareChain，
//     以命名实例（"http" / "gateway"）注入 IOC，与 enabled 无关、始终存在
//   - 各中间件 Provider 在 Setup 阶段通过 chain.Use 添加自己的中间件
//   - HttpProvider / GatewayProvider 的 Setup 阶段取出 chain，调 Handler() 包装各自 mux，
//     之后追加的中间件不生效 —— 中间件 Provider 必须 Use 在两者之前
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
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		h = c.middlewares[i](h)
	}
	return h
}

// GetHttpMiddlewareChain 从容器获取 HttpProvider 的 *MiddlewareChain。
func GetHttpMiddlewareChain(ctx context.Context) (*MiddlewareChain, error) {
	return ioc.GetNamed[*MiddlewareChain](ctx, httpMiddlewareChainName)
}

// MustGetHttpMiddlewareChain 从容器获取 HttpProvider 的 *MiddlewareChain，缺失时 panic。
func MustGetHttpMiddlewareChain(ctx context.Context) *MiddlewareChain {
	return ioc.MustGetNamed[*MiddlewareChain](ctx, httpMiddlewareChainName)
}

// GetGatewayMiddlewareChain 从容器获取 GatewayProvider 的 *MiddlewareChain。
func GetGatewayMiddlewareChain(ctx context.Context) (*MiddlewareChain, error) {
	return ioc.GetNamed[*MiddlewareChain](ctx, gatewayMiddlewareChainName)
}

// MustGetGatewayMiddlewareChain 从容器获取 GatewayProvider 的 *MiddlewareChain，缺失时 panic。
func MustGetGatewayMiddlewareChain(ctx context.Context) *MiddlewareChain {
	return ioc.MustGetNamed[*MiddlewareChain](ctx, gatewayMiddlewareChainName)
}
