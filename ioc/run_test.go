package ioc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const testTimeout = 2 * time.Second

// timeAfter 包装 time.After，方便测试 helper 统一使用。
func timeAfter(timeout time.Duration) <-chan time.Time {
	return time.After(timeout)
}

// TestRunWithoutServerWaitsUntilCancel 验证没有 Server 时 Run 会阻塞到上下文取消再 shutdown。
func TestRunWithoutServerWaitsUntilCancel(t *testing.T) {
	setupDone := make(chan struct{})
	shutdownDone := make(chan struct{})

	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(&stopTestProvider{
		namedTestProvider: &namedTestProvider{
			name: "worker",
			setupFunc: func(ctx context.Context) error {
				close(setupDone)
				return nil
			},
		},
		shutdownFunc: func(ctx context.Context) error {
			close(shutdownDone)
			return nil
		},
	}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, nil)
	}()

	waitClosed(t, setupDone)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run failed: %v", err)
		}
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for run")
	}
	waitClosed(t, shutdownDone)
}

// TestRunStartsServersConcurrently 验证多个 Server 会并发启动，任意一个返回 nil 即触发 shutdown。
func TestRunStartsServersConcurrently(t *testing.T) {
	var shutdowns []string
	first := newControlledServerProvider("first", nil, func(ctx context.Context) error {
		shutdowns = append(shutdowns, "first")
		return nil
	})
	second := newControlledServerProvider("second", nil, func(ctx context.Context) error {
		shutdowns = append(shutdowns, "second")
		return nil
	})

	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(first, second); err != nil {
		t.Fatalf("use providers failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- app.Run(context.Background(), nil)
	}()

	waitClosed(t, first.started)
	waitClosed(t, second.started)
	first.Release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run failed: %v", err)
		}
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for run")
	}

	if strings.Join(shutdowns, ",") != "second,first" {
		t.Fatalf("expected reverse shutdown order, got %v", shutdowns)
	}
}

// TestRunServeErrorLogsAndReturns 验证 Serve 返回错误时会记录日志并返回错误。
func TestRunServeErrorLogsAndReturns(t *testing.T) {
	buffer, restore := captureLogs(t)
	defer restore()

	wantErr := errors.New("serve failed")
	server := newControlledServerProvider("api", wantErr, nil)
	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(server); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- app.Run(context.Background(), nil)
	}()

	waitClosed(t, server.started)
	server.Release()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected serve error, got %v", err)
		}
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for run")
	}

	logs := buffer.String()
	if !strings.Contains(logs, `"message":"provider serve failed"`) {
		t.Fatalf("expected serve failure log in %s", logs)
	}
}

// TestNormalizeServeErrorIgnoresContextCancel 验证上下文取消导致的 Serve 错误不会被当成失败。
func TestNormalizeServeErrorIgnoresContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := normalizeServeError(ctx, serveResult{name: "api", err: context.Canceled}); err != nil {
		t.Fatalf("expected context canceled to be ignored, got %v", err)
	}
	if err := normalizeServeError(ctx, serveResult{name: "api", err: context.DeadlineExceeded}); err != nil {
		t.Fatalf("expected context deadline to be ignored, got %v", err)
	}
}

// TestRunServerContextCancelReturnsNil 验证运行期取消上下文时 server 退出不返回失败。
func TestRunServerContextCancelReturnsNil(t *testing.T) {
	server := newControlledServerProvider("api", nil, nil)
	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(server); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, nil)
	}()

	waitClosed(t, server.started)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected run to exit cleanly, got %v", err)
		}
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for run")
	}
}

// TestRunWithTaskExecutesFnAndShutsDown 验证 Run(ctx, fn) 在 fn 跑完后正确触发 shutdown。
func TestRunWithTaskExecutesFnAndShutsDown(t *testing.T) {
	var phases []string
	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(&stopTestProvider{
		namedTestProvider: &namedTestProvider{
			name: "worker",
			setupFunc: func(ctx context.Context) error {
				phases = append(phases, "setup")
				return nil
			},
		},
		shutdownFunc: func(ctx context.Context) error {
			phases = append(phases, "shutdown")
			return nil
		},
	}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	err := app.Run(context.Background(), func(ctx context.Context) error {
		phases = append(phases, "fn")
		return nil
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if strings.Join(phases, ",") != "setup,fn,shutdown" {
		t.Fatalf("expected setup,fn,shutdown, got %v", phases)
	}
}

// TestRunWithTaskJoinsFnAndShutdownErrors 验证 fn 与 Shutdown 的错误都会被汇总返回。
func TestRunWithTaskJoinsFnAndShutdownErrors(t *testing.T) {
	fnErr := errors.New("fn boom")
	shutdownErr := errors.New("shutdown boom")
	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(&stopTestProvider{
		namedTestProvider: &namedTestProvider{name: "worker"},
		shutdownFunc:      func(ctx context.Context) error { return shutdownErr },
	}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	err := app.Run(context.Background(), func(ctx context.Context) error { return fnErr })
	if !errors.Is(err, fnErr) || !errors.Is(err, shutdownErr) {
		t.Fatalf("expected joined errors, got %v", err)
	}
}

// TestRunWithNilTaskFallsBackToServe 验证显式传 nil fn 时回落到 serve 模式
// (没有 Server 即等到 ctx 取消)。
func TestRunWithNilTaskFallsBackToServe(t *testing.T) {
	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(&namedTestProvider{name: "noop"}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, nil)
	}()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean exit, got %v", err)
		}
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for run")
	}
}

// TestRunWithTaskContainerInCtx 验证 fn 接收的 ctx 已挂上容器,可直接 ioc.MustGet 解析。
func TestRunWithTaskContainerInCtx(t *testing.T) {
	app := New(WithShutdownTimeout(time.Second))
	want := &containerTestService{id: 42}
	if err := app.Use(&namedTestProvider{
		name: "binder",
		registerFunc: func(ctx context.Context) error {
			return Instance(ctx, want)
		},
	}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}

	var got *containerTestService
	err := app.Run(context.Background(), func(ctx context.Context) error {
		got = MustGet[*containerTestService](ctx)
		return nil
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got != want {
		t.Fatalf("expected resolved instance, got %v", got)
	}
}

// TestShutdownAllJoinsErrors 验证 shutdown 逆序执行并汇总多个错误。
func TestShutdownAllJoinsErrors(t *testing.T) {
	firstErr := errors.New("first shutdown failed")
	secondErr := errors.New("second shutdown failed")
	var shutdowns []string

	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(
		&stopTestProvider{
			namedTestProvider: &namedTestProvider{name: "first"},
			shutdownFunc: func(ctx context.Context) error {
				shutdowns = append(shutdowns, "first")
				return firstErr
			},
		},
		&stopTestProvider{
			namedTestProvider: &namedTestProvider{name: "second"},
			shutdownFunc: func(ctx context.Context) error {
				shutdowns = append(shutdowns, "second")
				return secondErr
			},
		},
	); err != nil {
		t.Fatalf("use providers failed: %v", err)
	}
	if err := app.RegisterAll(context.Background()); err != nil {
		t.Fatalf("register all failed: %v", err)
	}
	if err := app.SetupAll(context.Background()); err != nil {
		t.Fatalf("setup all failed: %v", err)
	}

	err := app.ShutdownAll(context.Background())
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("expected joined shutdown errors, got %v", err)
	}
	if strings.Join(shutdowns, ",") != "second,first" {
		t.Fatalf("expected reverse shutdown order, got %v", shutdowns)
	}
	if err := app.ShutdownAll(context.Background()); err != nil {
		t.Fatalf("expected second shutdown to be clean, got %v", err)
	}
	if strings.Join(shutdowns, ",") != "second,first" {
		t.Fatalf("expected second shutdown not to repeat, got %v", shutdowns)
	}
}
