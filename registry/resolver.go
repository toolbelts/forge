package registry

import (
	"context"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/resolver"
)

// Scheme gRPC resolver 的 scheme 名,业务方拨号写作 "redis:///<service>"。
const Scheme = "redis"

// builder 实现 google.golang.org/grpc/resolver.Builder。
// 由 Manager.ResolverBuilder / Manager.RegisterResolver 暴露。
type builder struct {
	mgr *Manager
}

// Build 解析 target.URL.Path 拿 service 名,启动 watcher goroutine 后返回 resolver。
// 兼容 "redis:///gpd" 与 "redis://authority/gpd" 两种形式。
func (b *builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	service := strings.TrimPrefix(target.URL.Path, "/")
	if service == "" {
		service = target.Endpoint()
	}
	if service == "" {
		return nil, ErrEmptyService
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &grpcResolver{
		mgr:     b.mgr,
		cc:      cc,
		service: service,
		cancel:  cancel,
		nudge:   make(chan struct{}, 1),
	}
	go r.watch(ctx)
	return r, nil
}

// Scheme 返回固定 scheme "redis"。
func (b *builder) Scheme() string {
	return Scheme
}

// grpcResolver 实现 google.golang.org/grpc/resolver.Resolver。
// last 字段保存上次推送的地址列表,Resolve 失败时不清空,避免抖动期把所有连接清空。
type grpcResolver struct {
	mgr     *Manager
	cc      resolver.ClientConn
	service string
	cancel  context.CancelFunc
	nudge   chan struct{}
	last    []resolver.Address
}

// ResolveNow 触发一次立即查询,grpc 在 picker 收到 ErrNoAvailable 等场景调用。
// nudge channel 容量为 1,重复触发会被合并。
func (r *grpcResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	select {
	case r.nudge <- struct{}{}:
	default:
	}
}

// Close 停止内部 watcher goroutine。
func (r *grpcResolver) Close() {
	r.cancel()
}

// watch 周期 + nudge 触发 tick,首次构造时立即查询一次让 client 拿到初始地址。
func (r *grpcResolver) watch(ctx context.Context) {
	r.tick(ctx)

	ticker := time.NewTicker(r.mgr.opt.resolveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		case <-r.nudge:
			r.tick(ctx)
		}
	}
}

// tick 拉取一次实例列表并推给 grpc;失败 ReportError 但保留上次地址不清空。
func (r *grpcResolver) tick(ctx context.Context) {
	instances, err := r.mgr.Resolve(ctx, r.service)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("service", r.service).Msg("registry resolve failed")
		r.cc.ReportError(err)
		return
	}

	addrs := make([]resolver.Address, 0, len(instances))
	for _, inst := range instances {
		addrs = append(addrs, resolver.Address{Addr: inst.Addr})
	}
	if addressesEqual(r.last, addrs) {
		return
	}
	r.last = addrs

	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("service", r.service).Msg("registry update state failed")
	}
}

// addressesEqual 判断两组 addr 是否完全相同,Resolve 已按 instance id 排好序,顺序稳定。
func addressesEqual(a, b []resolver.Address) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Addr != b[i].Addr {
			return false
		}
	}
	return true
}
