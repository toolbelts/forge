package provider

import "google.golang.org/grpc"

// InterceptorChain 收集 gRPC server 拦截器。
//
// 编排约定：
//   - GrpcProvider.Register 阶段构造 *InterceptorChain 注入 IOC
//   - 各 InterceptorProvider 在 Setup 阶段通过 chain.Use / chain.UseStream 添加自己的拦截器
//   - GrpcProvider.Setup 阶段从 IOC 取出 chain，调 ServerOptions() 传给 grpc.NewServer
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
