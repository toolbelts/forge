package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// TcpHandler TCP 连接处理器，业务方实现并通过 MustSetTcpHandler 注入。
type TcpHandler interface {
	HandleConn(ctx context.Context, conn net.Conn)
}

// TcpProvider TCP 服务提供者，根据 tcp.* 配置选择性 net.Listen 并在 Serve 阶段做 accept loop。
type TcpProvider struct {
	enabled  bool
	listener net.Listener
	wg       sync.WaitGroup
}

// Register 当 tcp.enabled 为真时建立监听，listener 不暴露到容器。
func (p *TcpProvider) Register(ctx context.Context) error {
	v := ioc.MustGet[*viper.Viper](ctx)
	p.enabled = v.GetBool("tcp.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "tcp").Msg("tcp disabled, skip register")
		return nil
	}

	addr := v.GetString("tcp.addr")
	if addr == "" {
		return fmt.Errorf("tcp addr is empty")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp listen %s: %w", addr, err)
	}
	p.listener = lis
	log.Ctx(ctx).Info().Str("addr", addr).Msg("tcp listening")
	return nil
}

// Setup 无操作，业务方在自己的 Setup 中通过 MustSetTcpHandler 注入连接处理器。
func (p *TcpProvider) Setup(ctx context.Context) error {
	return nil
}

// Serve 当 enabled 时启动 accept loop 并将连接派发给已注入的 TcpHandler。
// listener 的关闭由 Shutdown 触发，关闭后 Accept 返回 net.ErrClosed，循环正常退出。
func (p *TcpProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		<-ctx.Done()
		return nil
	}

	handler, err := ioc.Get[TcpHandler](ctx)
	if err != nil {
		return fmt.Errorf("get tcp handler: %w", err)
	}

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		p.wg.Go(func() {
			defer conn.Close()
			handler.HandleConn(ctx, conn)
		})
	}
}

// Shutdown 关闭 listener 触发 accept loop 退出，再等待全部活跃连接结束或上下文超时。
func (p *TcpProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	if p.listener != nil {
		_ = p.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MustSetTcpHandler 将 TcpHandler 注入到容器，业务方应在 Setup 阶段调用。
func MustSetTcpHandler(ctx context.Context, h TcpHandler) {
	ioc.MustInstance(ctx, h)
}

// GetTcpHandler 从容器读取 TcpHandler。
func GetTcpHandler(ctx context.Context) (TcpHandler, error) {
	return ioc.Get[TcpHandler](ctx)
}
