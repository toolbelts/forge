package meta

import (
	"context"

	"google.golang.org/grpc"
)

// BuildSkips 把 yaml 里的 string 列表编译成查找集合。空串过滤掉,
// 输入项可以是 HTTP path(/v1/auth/login)或 gRPC FullMethod(/pkg.Svc/Method),
// 两种形式混写都接受。
func BuildSkips(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item != "" {
			set[item] = struct{}{}
		}
	}
	return set
}

// MatchSkips 判断当前请求是否命中跳过集合:gRPC FullMethod 或 HTTP path
// (MetaRequestPath)任一在 set 里即返回 true。set 为空永远返回 false。
// fullMethod 直接走 grpc.Method(ctx),省去调用方传参;
// path 只读 MetaRequestPath 一个键,避免在跳过判断热点上构造完整 RequestMeta。
// 直连 gRPC 没有 path,只走 fullMethod 维度;经 grpc-gateway 的 HTTP 请求
// 两个维度都可命中,任一匹配即视为命中。
func MatchSkips(ctx context.Context, set map[string]struct{}) bool {
	if len(set) == 0 {
		return false
	}
	if fullMethod, ok := grpc.Method(ctx); ok {
		if _, hit := set[fullMethod]; hit {
			return true
		}
	}
	if path := String(ctx, MetaRequestPath); path != "" {
		if _, hit := set[path]; hit {
			return true
		}
	}
	return false
}
