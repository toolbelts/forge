package provider

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/toolbelts/forge/ioc"
)

// GrpcProvider gRPC 服务提供者，根据 grpc.* 配置选择性启动并向容器注入 *grpc.Server。
//
// 编排：
//   - Register: 注入 *InterceptorChain；读 enabled / addr 并 net.Listen，让端口冲突早失败
//   - Setup: 收集 chain 与 MaxRecv/Send 选项，构造 *grpc.Server 注入容器
type GrpcProvider struct {
	enabled         bool
	listener        net.Listener
	server          *grpc.Server
	maxRecvMsgSize  int
	maxSendMsgSize  int
	shutdownTimeout time.Duration
}

// Register 注入 *InterceptorChain，读 viper 配置并 listen，让其它 Provider 能在 Setup 中加拦截器。
func (p *GrpcProvider) Register(ctx context.Context) error {
	ioc.MustInstance(ctx, &InterceptorChain{})

	v := ioc.MustGet[*viper.Viper](ctx)
	p.enabled = v.GetBool("grpc.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "grpc").Msg("grpc disabled, skip register")
		return nil
	}

	addr := v.GetString("grpc.addr")
	if addr == "" {
		return fmt.Errorf("grpc addr is empty")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", addr, err)
	}
	p.listener = lis
	p.maxRecvMsgSize = v.GetInt("grpc.max_recv_msg_size")
	p.maxSendMsgSize = v.GetInt("grpc.max_send_msg_size")
	p.shutdownTimeout = v.GetDuration("grpc.shutdown_timeout")

	// 注入 listener 让后续 Provider(如 RegistryProvider) 拿到实际监听端口,
	// 支持 grpc.addr 配 ":0" 让内核选随机端口的场景。
	ioc.MustInstance(ctx, p.listener)

	log.Ctx(ctx).Info().Str("addr", addr).Msg("grpc listening")
	return nil
}

// Setup 当 enabled 时收集 InterceptorChain 与 MaxMsgSize 配置，构造 *grpc.Server 注入容器。
func (p *GrpcProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	chain := ioc.MustGet[*InterceptorChain](ctx)
	opts := chain.ServerOptions()
	opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	if p.maxRecvMsgSize > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(p.maxRecvMsgSize))
	}
	if p.maxSendMsgSize > 0 {
		opts = append(opts, grpc.MaxSendMsgSize(p.maxSendMsgSize))
	}
	p.server = grpc.NewServer(opts...)

	ioc.MustInstance(ctx, p.server)
	return nil
}

// Serve 当 enabled 时阻塞 server.Serve，未启用时等待 ctx.Done()。
func (p *GrpcProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		<-ctx.Done()
		return nil
	}
	return p.server.Serve(p.listener)
}

// Shutdown 优雅关闭 grpc.Server，超过 shutdownTimeout 后强制 Stop。
// 容忍 Setup 失败时 server 未构造、仅 listener 已开的情况。
func (p *GrpcProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	if p.server == nil {
		if p.listener != nil {
			_ = p.listener.Close()
		}
		return nil
	}

	done := make(chan struct{})
	go func() {
		p.server.GracefulStop()
		close(done)
	}()

	if p.shutdownTimeout > 0 {
		timer := time.NewTimer(p.shutdownTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			p.server.Stop()
			<-done
		}
	} else {
		<-done
	}
	return nil
}

// GetGrpcServer 从容器获取 *grpc.Server。
func GetGrpcServer(ctx context.Context) (*grpc.Server, error) {
	return ioc.Get[*grpc.Server](ctx)
}

// MustGetGrpcServer 从容器获取 *grpc.Server，缺失时 panic。
func MustGetGrpcServer(ctx context.Context) *grpc.Server {
	return ioc.MustGet[*grpc.Server](ctx)
}

// GetGrpcListener 从容器获取 gRPC 监听器,仅 grpc.enabled=true 时可用。
func GetGrpcListener(ctx context.Context) (net.Listener, error) {
	return ioc.Get[net.Listener](ctx)
}

// MustGetGrpcListener 从容器获取 gRPC 监听器,缺失时 panic。
// 调用方可断言为 *net.TCPAddr 取实际端口,支持 grpc.addr 配 ":0" 的随机端口。
func MustGetGrpcListener(ctx context.Context) net.Listener {
	return ioc.MustGet[net.Listener](ctx)
}
