package provider

import (
	"context"
	"errors"

	"github.com/toolbelts/forge/ioc"
)

// 访问函数（GetXxx / MustGetXxx）取产物时用 ioc 的 GetFrom[T, Owner]：产出它的 Provider 即使注册得更靠后，
// 也会被容器提前 Setup，所以 Provider 之间的依赖不必靠注册顺序来保证。
//
// 例外是读写拦截器链 / 中间件链的 Provider，它们的 Setup 结果取决于执行时机，不能被提前拉起：
// GrpcProvider / HttpProvider / GatewayProvider 提前 Setup 会漏掉还没登记的拦截器和中间件，
// TokenProvider / RateLimitProvider 提前 Setup 会改变自己在链上的内外层位置，而且都是静默发生。
// 所以它们的访问函数保持普通的 Get：顺序不对时直接报 binding not found。

// needOtel 在组件挂 OTel hook 之前调用：确保已注册的 MetricsProvider / TraceProvider 先完成 Setup，
// 否则拿到的是 noop 的全局 MeterProvider / TracerProvider，插桩静默失效。
func needOtel(ctx context.Context) error {
	c := ioc.FromContext(ctx)
	return errors.Join(c.Ensure[*MetricsProvider](), c.Ensure[*TraceProvider]())
}

// chainOf 取出 (*T, name) 绑定，不存在则创建零值并绑定到容器。
// 拦截器链、中间件链这类收集器没有固定的创建者：生产方（各拦截器 / 中间件 Provider）
// 和消费方（GrpcProvider 等）只约定注册顺序，谁先用到谁创建。
func chainOf[T any](ctx context.Context, name ...string) (*T, error) {
	c := ioc.FromContext(ctx)
	chain, err := c.Get[*T](name...)
	if errors.Is(err, ioc.ErrNotFound) {
		chain, err = new(T), nil
		c.Instance(chain, name...)
	}
	return chain, err
}

// must 在 err 非 nil 时 panic，否则返回 v。
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
