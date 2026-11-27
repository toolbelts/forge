package provider

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/toolbelts/forge/ioc"
)

// GrpcProvider gRPC 服务提供者，根据 grpc.* 配置选择性启动并向容器注入 *grpc.Server。
//
// 编排：Setup 读 enabled / addr 并 net.Listen，再用此前收集到的 InterceptorChain 与 MaxRecv/Send 选项
// 构造 *grpc.Server，把 listener 和 server 注入容器。拦截器 Provider 必须注册在它之前，
// 业务方在它之后的 Setup 中通过 MustGetGrpcServer 注册服务。
type GrpcProvider struct {
	enabled         bool
	listener        net.Listener
	server          *grpc.Server
	shutdownTimeout time.Duration
}

// Setup 当 grpc.enabled 为真时 listen 并构造 *grpc.Server，将 listener 与 server 注入容器。
func (p *GrpcProvider) Setup(ctx context.Context) error {
	v := MustGetViper(ctx)
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
	p.shutdownTimeout = v.GetDuration("grpc.shutdown_timeout")

	opts := MustGetInterceptorChain(ctx).ServerOptions()
	if err := needOtel(ctx); err != nil {
		return err
	}
	if traceInstrumentationEnabled(v, observabilityComponentGrpc) ||
		metricsInstrumentationEnabled(v, observabilityComponentGrpc) {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	if n := v.GetInt("grpc.max_recv_msg_size"); n > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(n))
	}
	if n := v.GetInt("grpc.max_send_msg_size"); n > 0 {
		opts = append(opts, grpc.MaxSendMsgSize(n))
	}
	p.server = grpc.NewServer(opts...)

	// 注入 listener 让后续 Provider(如 RegistryProvider) 拿到实际监听端口,
	// 支持 grpc.addr 配 ":0" 让内核选随机端口的场景。
	ioc.FromContext(ctx).Instance(p.listener).Instance(p.server)

	log.Ctx(ctx).Info().Str("addr", addr).Msg("grpc listening")
	return nil
}

// Serve 当 enabled 时阻塞 server.Serve，未启用时直接返回。
func (p *GrpcProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	return p.server.Serve(p.listener)
}

// Shutdown 优雅关闭 grpc.Server，超过 shutdownTimeout 后强制 Stop。
func (p *GrpcProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
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
	return ioc.FromContext(ctx).Get[*grpc.Server]()
}

// MustGetGrpcServer 从容器获取 *grpc.Server，缺失时 panic。
func MustGetGrpcServer(ctx context.Context) *grpc.Server {
	return ioc.FromContext(ctx).MustGet[*grpc.Server]()
}

// GetGrpcListener 从容器获取 gRPC 监听器,仅 grpc.enabled=true 时可用。
func GetGrpcListener(ctx context.Context) (net.Listener, error) {
	return ioc.FromContext(ctx).Get[net.Listener]()
}

// MustGetGrpcListener 从容器获取 gRPC 监听器,缺失时 panic。
// 调用方可断言为 *net.TCPAddr 取实际端口,支持 grpc.addr 配 ":0" 的随机端口。
func MustGetGrpcListener(ctx context.Context) net.Listener {
	return ioc.FromContext(ctx).MustGet[net.Listener]()
}
