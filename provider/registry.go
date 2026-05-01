package provider

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/registry"
)

// registryDefaultRedisName 是注册中心默认使用的 Redis 实例名,可通过 registry.redis 覆盖。
const registryDefaultRedisName = "default"

// registryDeregisterTimeout 是 Shutdown 阶段调 Deregister 的最大等待时间。
const registryDeregisterTimeout = 2 * time.Second

// RegistryProvider 基于 Redis 的 gRPC 服务注册发现。
//
// 编排约定:排在 RedisProvider、GrpcProvider 之后(读 Redis client 与 grpc 实际监听端口),
// GatewayProvider 之前(让 gateway.grpc_endpoint 未来可改为 redis:///<service> 走自定义 resolver)。
//
// 行为:
//   - 注入 *registry.Manager 供业务方拨号 / 查询使用
//   - 全局注册 grpc resolver scheme "redis",业务方调 grpc.NewClient("redis:///<service>") 即可
//   - 若 grpc.enabled=true,自动用 BuildInfo.InstanceId + 推导的 advertise host:port 注册当前实例
//   - Shutdown 时主动 Deregister,kill -9 场景靠 TTL 兜底
type RegistryProvider struct {
	enabled bool
	mgr     *registry.Manager
	reg     *registry.Registration
}

// Register 读 registry.enabled,disabled 时 Setup/Shutdown 直接跳过。
func (p *RegistryProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("registry.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "registry").Msg("registry disabled, skip")
	}
	return nil
}

// Setup 构造 Manager 注入容器,全局注册 grpc resolver,
// 若 grpc.enabled 则推导 advertise 地址自注册当前实例。
func (p *RegistryProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	v := MustGetViper(ctx)
	redisName := v.GetString("registry.redis")
	if redisName == "" {
		redisName = registryDefaultRedisName
	}

	rdb, err := GetRedis(ctx, redisName)
	if err != nil {
		return err
	}

	opts := make([]registry.Option, 0, 4)
	if prefix := v.GetString("registry.prefix"); prefix != "" {
		opts = append(opts, registry.WithPrefix(prefix))
	}
	if d := v.GetDuration("registry.ttl"); d > 0 {
		opts = append(opts, registry.WithTtl(d))
	}
	if d := v.GetDuration("registry.heartbeat"); d > 0 {
		opts = append(opts, registry.WithHeartbeat(d))
	}
	if d := v.GetDuration("registry.resolve_interval"); d > 0 {
		opts = append(opts, registry.WithResolveInterval(d))
	}

	mgr, err := registry.NewManager(rdb, opts...)
	if err != nil {
		return err
	}
	p.mgr = mgr
	ioc.MustInstance(ctx, p.mgr)
	mgr.RegisterResolver()

	log.Ctx(ctx).Info().
		Str("provider", "registry").
		Str("redis", redisName).
		Msg("registry manager registered")

	if !v.GetBool("grpc.enabled") {
		return nil
	}

	service := v.GetString("app.name")
	if service == "" {
		return fmt.Errorf("registry: app.name is empty, cannot self-register")
	}

	host, err := resolveAdvertiseHost(v)
	if err != nil {
		return err
	}
	port, err := resolveAdvertisePort(ctx, v)
	if err != nil {
		return err
	}

	bi := MustGetBuildInfo(ctx)
	inst := registry.Instance{
		Id:      bi.InstanceId(),
		Service: service,
		Addr:    net.JoinHostPort(host, strconv.Itoa(port)),
	}
	reg, err := mgr.Register(ctx, inst)
	if err != nil {
		return err
	}
	p.reg = reg
	return nil
}

// Shutdown 主动注销当前实例并停掉心跳 goroutine。
// 用 context.WithoutCancel 避免父 ctx 已取消导致 DEL 跳过,同时加 2s 超时防卡死。
func (p *RegistryProvider) Shutdown(ctx context.Context) error {
	if p.reg == nil {
		return nil
	}
	deregCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), registryDeregisterTimeout)
	defer cancel()
	return p.reg.Deregister(deregCtx)
}

// MustGetRegistryManager 从容器获取注册中心工厂,缺失时 panic。
// 业务方在需要 client 端解析的代码里调 grpc.NewClient("redis:///<service>"),
// 或直接 mgr.Resolve / mgr.Watch 拿实例列表。
func MustGetRegistryManager(ctx context.Context) *registry.Manager {
	return ioc.MustGet[*registry.Manager](ctx)
}

// GetRegistryManager 从容器获取注册中心工厂。
func GetRegistryManager(ctx context.Context) (*registry.Manager, error) {
	return ioc.Get[*registry.Manager](ctx)
}

// resolveAdvertiseHost 推导对外宣告的 host:配置 → grpc.addr host 段 → 内网私有 IPv4。
func resolveAdvertiseHost(v *viper.Viper) (string, error) {
	if h := v.GetString("registry.advertise_host"); h != "" {
		return h, nil
	}
	if grpcAddr := v.GetString("grpc.addr"); grpcAddr != "" {
		host, _, err := net.SplitHostPort(grpcAddr)
		if err == nil && host != "" && host != "0.0.0.0" && host != "::" {
			return host, nil
		}
	}
	host, err := firstPrivateIpv4()
	if err != nil {
		return "", fmt.Errorf("registry: derive advertise host: %w", err)
	}
	return host, nil
}

// resolveAdvertisePort 推导对外宣告的 port:配置 → grpc listener 实际监听端口。
func resolveAdvertisePort(ctx context.Context, v *viper.Viper) (int, error) {
	if p := v.GetInt("registry.advertise_port"); p > 0 {
		return p, nil
	}
	lis := MustGetGrpcListener(ctx)
	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("registry: grpc listener is not tcp, got %T", lis.Addr())
	}
	return tcpAddr.Port, nil
}

// firstPrivateIpv4 返回第一个非 loopback 的 RFC1918 私有 IPv4 地址。
// 用于 docker bridge / 多网卡场景下兜底推导对外 host。
func firstPrivateIpv4() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ip4 := ipNet.IP.To4()
		if ip4 == nil || ip4.IsLoopback() {
			continue
		}
		if ip4.IsPrivate() {
			return ip4.String(), nil
		}
	}
	return "", fmt.Errorf("registry: no private ipv4 found")
}
