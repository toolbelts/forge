package ioc

import (
	"context"
	"strings"
)

type resolutionContextKey struct{}

// enterResolution 将当前解析 key 写入上下文，并检测循环依赖。
func enterResolution(ctx context.Context, key bindingKey) (context.Context, error) {
	stack := resolutionStack(ctx)
	for _, item := range stack {
		if item == key {
			return ctx, wrapError(ErrCircularDependency, circularPath(stack, key))
		}
	}

	next := append(append([]bindingKey(nil), stack...), key)
	return context.WithValue(ctx, resolutionContextKey{}, next), nil
}

// resolutionStack 从上下文中读取当前解析栈。
func resolutionStack(ctx context.Context) []bindingKey {
	if ctx == nil {
		return nil
	}

	stack, _ := ctx.Value(resolutionContextKey{}).([]bindingKey)
	return stack
}

// circularPath 返回循环依赖的可读路径。
func circularPath(stack []bindingKey, key bindingKey) string {
	parts := make([]string, 0, len(stack)+1)
	for _, item := range stack {
		parts = append(parts, item.String())
	}
	parts = append(parts, key.String())

	return strings.Join(parts, " -> ")
}
