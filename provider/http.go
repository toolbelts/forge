package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// HttpProvider HTTP 服务提供者，根据 http.* 配置选择性启动并向容器注入 *http.ServeMux。
//
// 编排：
//   - Register: 注入命名 "http" 的 *MiddlewareChain；读 enabled / addr 并 net.Listen，让端口冲突早失败
//   - Setup: 从 IOC 取出 chain，用中间件包装 mux 作为 server handler
type HttpProvider struct {
	enabled         bool
	listener        net.Listener
	mux             *http.ServeMux
	server          *http.Server
	shutdownTimeout time.Duration
}

// Register 注入 *MiddlewareChain；当 http.enabled 为真时建立监听并构造 *http.ServeMux 与 *http.Server，将 mux 绑定到容器。
func (p *HttpProvider) Register(ctx context.Context) error {
	ioc.MustInstanceNamed(ctx, httpMiddlewareChainName, &MiddlewareChain{})

	v := ioc.MustGet[*viper.Viper](ctx)
	p.enabled = v.GetBool("http.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "http").Msg("http disabled, skip register")
		return nil
	}

	addr := v.GetString("http.addr")
	if addr == "" {
		return fmt.Errorf("http addr is empty")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http listen %s: %w", addr, err)
	}
	p.listener = lis

	p.mux = http.NewServeMux()
	p.server = &http.Server{
		Handler:           p.mux, // 占位，Setup 阶段用 MiddlewareChain 包装后覆盖
		ReadTimeout:       v.GetDuration("http.read_timeout"),
		ReadHeaderTimeout: v.GetDuration("http.read_header_timeout"),
		WriteTimeout:      v.GetDuration("http.write_timeout"),
		IdleTimeout:       v.GetDuration("http.idle_timeout"),
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	p.shutdownTimeout = v.GetDuration("http.shutdown_timeout")

	ioc.MustInstance(ctx, p.mux)
	log.Ctx(ctx).Info().Str("addr", addr).Msg("http listening")
	return nil
}

// Setup 当 enabled 时从容器取 MiddlewareChain，用中间件包装 mux 作为 server handler。
// 中间件 Provider 必须 Use 在 HttpProvider 之前，此后追加的中间件会被静默丢弃；
// 业务方仍在自己的 Setup 中通过 MustGetHttpMux 注册路由，路由注册时机与包装无关。
func (p *HttpProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	p.server.Handler = MustGetHttpMiddlewareChain(ctx).Handler(p.mux)
	return nil
}

// Serve 当 enabled 时阻塞 server.Serve；http.ErrServerClosed 视为正常退出。
func (p *HttpProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		<-ctx.Done()
		return nil
	}

	if err := p.server.Serve(p.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown 优雅关闭 *http.Server，使用 shutdownTimeout 限制最大耗时。
func (p *HttpProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	shutdownCtx := context.WithoutCancel(ctx)
	if p.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(shutdownCtx, p.shutdownTimeout)
		defer cancel()
	}
	return p.server.Shutdown(shutdownCtx)
}

// GetHttpMux 从容器获取 *http.ServeMux。
func GetHttpMux(ctx context.Context) (*http.ServeMux, error) {
	return ioc.Get[*http.ServeMux](ctx)
}

// MustGetHttpMux 从容器获取 *http.ServeMux，缺失时 panic。
func MustGetHttpMux(ctx context.Context) *http.ServeMux {
	return ioc.MustGet[*http.ServeMux](ctx)
}
