package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/meta"
)

// gatewayConnName 是 *grpc.ClientConn 在容器中的命名 key，避免与未来其它 grpc client 冲突。
const gatewayConnName = "gateway"

// GatewayProvider grpc-gateway v2 服务提供者，根据 gateway.* 配置选择性启动并向容器注入 *runtime.ServeMux 与 *grpc.ClientConn。
type GatewayProvider struct {
	enabled         bool
	listener        net.Listener
	mux             *runtime.ServeMux
	server          *http.Server
	conn            *grpc.ClientConn
	shutdownTimeout time.Duration
	otelEnabled     bool
	traceEnabled    bool
}

// Register 当 gateway.enabled 为真时建立监听、dial gRPC 后端并构造 *runtime.ServeMux 与 *http.Server。
func (p *GatewayProvider) Register(ctx context.Context) error {
	v := ioc.MustGet[*viper.Viper](ctx)
	p.enabled = v.GetBool("gateway.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "gateway").Msg("gateway disabled, skip register")
		return nil
	}

	addr := v.GetString("gateway.addr")
	if addr == "" {
		return fmt.Errorf("gateway addr is empty")
	}
	endpoint := v.GetString("gateway.grpc_endpoint")
	if endpoint == "" {
		return fmt.Errorf("gateway grpc_endpoint is empty")
	}
	p.traceEnabled = traceInstrumentationEnabled(v, observabilityComponentGateway)
	p.otelEnabled = p.traceEnabled || metricsInstrumentationEnabled(v, observabilityComponentGateway)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("gateway listen %s: %w", addr, err)
	}
	p.listener = lis

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if p.otelEnabled {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}
	conn, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		_ = lis.Close()
		return fmt.Errorf("gateway dial %s: %w", endpoint, err)
	}
	p.conn = conn

	p.mux = runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, gatewayJsonPb()),
		runtime.WithErrorHandler(GatewayErrorHandler),
		runtime.WithMetadata(meta.Annotator),
	)

	handler := http.Handler(p.mux)
	if p.traceEnabled {
		handler = withTracePropagation(handler)
	}

	p.server = &http.Server{
		Handler:           handler,
		ReadTimeout:       v.GetDuration("gateway.read_timeout"),
		ReadHeaderTimeout: v.GetDuration("gateway.read_header_timeout"),
		WriteTimeout:      v.GetDuration("gateway.write_timeout"),
		IdleTimeout:       v.GetDuration("gateway.idle_timeout"),
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	p.shutdownTimeout = v.GetDuration("gateway.shutdown_timeout")

	ioc.MustInstance(ctx, p.mux)
	ioc.MustInstanceNamed(ctx, gatewayConnName, p.conn)
	log.Ctx(ctx).Info().Str("addr", addr).Str("grpc_endpoint", endpoint).Msg("gateway listening")
	return nil
}

// Setup 无操作，业务方在自己的 Setup 中通过 MustGetGatewayMux + MustGetGatewayConn 调 pb.RegisterXxxHandler 注册路由。
func (p *GatewayProvider) Setup(ctx context.Context) error {
	return nil
}

// Serve 当 enabled 时阻塞 server.Serve；http.ErrServerClosed 视为正常退出。
func (p *GatewayProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		<-ctx.Done()
		return nil
	}

	if err := p.server.Serve(p.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown 优雅关闭 *http.Server 并 close grpc client conn，错误用 errors.Join 聚合。
func (p *GatewayProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	shutdownCtx := context.WithoutCancel(ctx)
	if p.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(shutdownCtx, p.shutdownTimeout)
		defer cancel()
	}

	var errs []error
	if err := p.server.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("gateway http shutdown: %w", err))
	}
	if err := p.conn.Close(); err != nil {
		errs = append(errs, fmt.Errorf("gateway grpc conn close: %w", err))
	}
	return errors.Join(errs...)
}

func gatewayJsonPb() *runtime.JSONPb {
	return &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{
			UseProtoNames:     true,
			UseEnumNumbers:    false,
			EmitDefaultValues: true,
		},
		UnmarshalOptions: protojson.UnmarshalOptions{
			DiscardUnknown: true,
		},
	}
}

// GetGatewayMux 从容器获取 *runtime.ServeMux。
func GetGatewayMux(ctx context.Context) (*runtime.ServeMux, error) {
	return ioc.Get[*runtime.ServeMux](ctx)
}

// MustGetGatewayMux 从容器获取 *runtime.ServeMux，缺失时 panic。
func MustGetGatewayMux(ctx context.Context) *runtime.ServeMux {
	return ioc.MustGet[*runtime.ServeMux](ctx)
}

// GetGatewayConn 从容器获取 gateway 反向连接到 gRPC 后端的 *grpc.ClientConn。
func GetGatewayConn(ctx context.Context) (*grpc.ClientConn, error) {
	return ioc.GetNamed[*grpc.ClientConn](ctx, gatewayConnName)
}

// MustGetGatewayConn 从容器获取 gateway *grpc.ClientConn，缺失时 panic。
func MustGetGatewayConn(ctx context.Context) *grpc.ClientConn {
	return ioc.MustGetNamed[*grpc.ClientConn](ctx, gatewayConnName)
}

// withTracePropagation 从 HTTP 请求头提取 W3C traceparent 注入到 ctx，
// 让 gateway 反向调用 gRPC 后端时通过 otelgrpc.ClientHandler 把上游 trace 接续下去。
// 不在此处创建独立 HTTP server span，避免与 otelgrpc server span 重复，与 simd 风格一致。
func withTracePropagation(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
