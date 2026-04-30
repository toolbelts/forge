package ioc

import (
	"context"
	"sync"
	"testing"
)

type containerTestService struct {
	id int
}

type namedTestProvider struct {
	name         string
	registerFunc func(context.Context) error
	setupFunc    func(context.Context) error
}

// Name 返回测试 provider 的显式名称。
func (p *namedTestProvider) Name() string {
	return p.name
}

// Register 执行测试 provider 的注册回调。
func (p *namedTestProvider) Register(ctx context.Context) error {
	if p.registerFunc == nil {
		return nil
	}

	return p.registerFunc(ctx)
}

// Setup 执行测试 provider 的初始化回调。
func (p *namedTestProvider) Setup(ctx context.Context) error {
	if p.setupFunc == nil {
		return nil
	}

	return p.setupFunc(ctx)
}

type reflectedTestProvider struct{}

// Register 实现反射命名 provider 的注册阶段。
func (p *reflectedTestProvider) Register(ctx context.Context) error {
	return nil
}

// Setup 实现反射命名 provider 的初始化阶段。
func (p *reflectedTestProvider) Setup(ctx context.Context) error {
	return nil
}

type stopTestProvider struct {
	*namedTestProvider
	shutdownFunc func(context.Context) error
}

// Shutdown 执行测试 provider 的关闭回调。
func (p *stopTestProvider) Shutdown(ctx context.Context) error {
	if p.shutdownFunc == nil {
		return nil
	}

	return p.shutdownFunc(ctx)
}

type controlledServerProvider struct {
	*stopTestProvider
	started   chan struct{}
	stop      chan struct{}
	serveErr  error
	startOnce sync.Once
	stopOnce  sync.Once
}

// newControlledServerProvider 创建可由测试控制退出时机的 server provider。
func newControlledServerProvider(name string, serveErr error, shutdownFunc func(context.Context) error) *controlledServerProvider {
	return &controlledServerProvider{
		stopTestProvider: &stopTestProvider{
			namedTestProvider: &namedTestProvider{name: name},
			shutdownFunc:      shutdownFunc,
		},
		started:  make(chan struct{}),
		stop:     make(chan struct{}),
		serveErr: serveErr,
	}
}

// Serve 阻塞到测试释放 stop channel 或上下文取消。
func (p *controlledServerProvider) Serve(ctx context.Context) error {
	p.startOnce.Do(func() {
		close(p.started)
	})

	select {
	case <-p.stop:
		return p.serveErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown 释放阻塞中的 Serve 并执行关闭回调。
func (p *controlledServerProvider) Shutdown(ctx context.Context) error {
	p.Release()
	return p.stopTestProvider.Shutdown(ctx)
}

// Release 让阻塞中的 Serve 返回。
func (p *controlledServerProvider) Release() {
	p.stopOnce.Do(func() {
		close(p.stop)
	})
}

// requirePanic 断言传入函数会触发 panic。
func requirePanic(t *testing.T, fn func()) any {
	t.Helper()

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		fn()
	}()

	if recovered == nil {
		t.Fatal("expected panic")
	}

	return recovered
}

// waitClosed 等待 channel 被关闭。
func waitClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for channel")
	}
}
