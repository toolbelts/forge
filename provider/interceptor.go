package provider

import (
	"context"

	"google.golang.org/grpc"
)

// InterceptorChain 收集 gRPC server 拦截器。
//
// 编排约定：
//   - 链没有固定的创建者：第一次 MustGetInterceptorChain 时创建并绑定到容器
//   - 各 InterceptorProvider 在 Setup 中通过 chain.Use / chain.UseStream 添加自己的拦截器
//   - GrpcProvider.Setup 取出 chain，调 ServerOptions() 传给 grpc.NewServer，
//     之后追加的拦截器不生效 —— 拦截器 Provider 必须注册在 GrpcProvider 之前
//
// 执行顺序：Use 顺序 = chain 外层执行顺序。在 main.go 中越靠前的 InterceptorProvider 越外层。
// 推荐顺序：Recovery（最外层，必须最先 recover panic）→ AccessLog（访问日志）→ Error（归一化）
// → RateLimit → Validate → Token → 业务 interceptor。
type InterceptorChain struct {
	unary  []grpc.UnaryServerInterceptor
	stream []grpc.StreamServerInterceptor
}

// Use 追加一元拦截器。
func (c *InterceptorChain) Use(interceptors ...grpc.UnaryServerInterceptor) {
	c.unary = append(c.unary, interceptors...)
}

// UseStream 追加流式拦截器。
func (c *InterceptorChain) UseStream(interceptors ...grpc.StreamServerInterceptor) {
	c.stream = append(c.stream, interceptors...)
}

// ServerOptions 把链路转成 grpc.ServerOption 切片，供 grpc.NewServer 使用。
func (c *InterceptorChain) ServerOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption
	if len(c.unary) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(c.unary...))
	}
	if len(c.stream) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(c.stream...))
	}
	return opts
}

// GetInterceptorChain 从容器获取 *InterceptorChain，尚不存在时创建并绑定。
func GetInterceptorChain(ctx context.Context) (*InterceptorChain, error) {
	return chainOf[InterceptorChain](ctx)
}

// MustGetInterceptorChain 同 GetInterceptorChain，失败时 panic。
func MustGetInterceptorChain(ctx context.Context) *InterceptorChain {
	return must(GetInterceptorChain(ctx))
}
