package ioc

import "context"

type containerContextKey struct{}

// WithContext 将当前容器写入上下文。
func (c *Container) WithContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, containerContextKey{}, c)
}

// FromContext 从上下文中读取容器。
func FromContext(ctx context.Context) (*Container, bool) {
	if ctx == nil {
		return nil, false
	}

	container, ok := ctx.Value(containerContextKey{}).(*Container)
	return container, ok && container != nil
}

// MustFromContext 从上下文中读取容器，缺失时直接 panic。
func MustFromContext(ctx context.Context) *Container {
	container, ok := FromContext(ctx)
	if !ok {
		panic(ErrContainerNotFound)
	}

	return container
}
