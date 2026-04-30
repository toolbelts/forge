package provider

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/notify"
)

// RecoveryProvider 注册 panic 恢复拦截器到 InterceptorChain。
// 必须排在 ErrorProvider/ValidateProvider 之前注册（最外层），才能 recover 到内层所有 panic。
type RecoveryProvider struct{}

// Register 无前置依赖。
func (p *RecoveryProvider) Register(ctx context.Context) error { return nil }

// Setup 把一元 + 流式 panic 恢复拦截器加进 chain。
func (p *RecoveryProvider) Setup(ctx context.Context) error {
	chain := ioc.MustGet[*InterceptorChain](ctx)
	hostname, _ := os.Hostname()
	deps := recoveryDeps{
		notifier: ioc.MustGet[notify.Notifier](ctx),
		hostname: hostname,
		appName:  string(MustGetAppName(ctx)),
	}
	chain.Use(recoveryUnaryInterceptor(deps))
	chain.UseStream(recoveryStreamInterceptor(deps))
	log.Ctx(ctx).Info().Str("provider", "recovery").Msg("recovery interceptor registered")
	return nil
}

// recoveryDeps 拦截器闭包共享的依赖。
type recoveryDeps struct {
	notifier notify.Notifier
	hostname string
	appName  string
}

// recoveryUnaryInterceptor 捕获 unary RPC 中的 panic,转 errkit.CodePanic,stack 写日志,并异步推送告警。
func recoveryUnaryInterceptor(deps recoveryDeps) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				log.Ctx(ctx).Error().
					Str("method", info.FullMethod).
					Interface("panic", r).
					Bytes("stack", stack).
					Msg("grpc panic recovered")
				go notifyPanic(deps, info.FullMethod, r, stack)
				err = errkit.New(errkit.CodePanic, "internal panic").
					WithMetadata("panic", fmt.Sprint(r))
			}
		}()
		return handler(ctx, req)
	}
}

// recoveryStreamInterceptor 同 unary 但处理流式 RPC。
func recoveryStreamInterceptor(deps recoveryDeps) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				log.Ctx(ss.Context()).Error().
					Str("method", info.FullMethod).
					Interface("panic", r).
					Bytes("stack", stack).
					Msg("grpc stream panic recovered")
				go notifyPanic(deps, info.FullMethod, r, stack)
				err = errkit.New(errkit.CodePanic, "internal panic").
					WithMetadata("panic", fmt.Sprint(r))
			}
		}()
		return handler(srv, ss)
	}
}

// notifyPanic 用独立 background ctx + 超时异步推送 panic 告警，避免阻塞 RPC 返回，
// 也避免 RPC 自身 ctx 已被 cancel 影响通知发送。
func notifyPanic(deps recoveryDeps, method string, panicValue any, stack []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), notifySendTimeout)
	defer cancel()

	title, content := buildPanicMessage(deps.hostname, deps.appName, method, panicValue, stack)
	if err := deps.notifier.Send(ctx, title, content); err != nil {
		log.Error().Err(err).Str("method", method).Msg("panic notify failed")
	}
}
