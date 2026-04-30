package ioc

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestLifecycleRegisterThenSetup 验证所有 Register 完成后才执行 Setup。
func TestLifecycleRegisterThenSetup(t *testing.T) {
	var events []string

	app := New(WithShutdownTimeout(time.Second))
	first := &namedTestProvider{
		name: "first",
		registerFunc: func(ctx context.Context) error {
			events = append(events, "first register")
			return nil
		},
		setupFunc: func(ctx context.Context) error {
			events = append(events, "first setup")
			value, err := Get[int](ctx)
			if err != nil {
				return err
			}
			if value != 42 {
				return errors.New("unexpected value")
			}

			return nil
		},
	}
	second := &namedTestProvider{
		name: "second",
		registerFunc: func(ctx context.Context) error {
			events = append(events, "second register")
			return Instance(ctx, 42)
		},
		setupFunc: func(ctx context.Context) error {
			events = append(events, "second setup")
			return nil
		},
	}

	if err := app.Use(first, second); err != nil {
		t.Fatalf("use providers failed: %v", err)
	}
	if err := app.RegisterAll(context.Background()); err != nil {
		t.Fatalf("register all failed: %v", err)
	}
	if err := app.SetupAll(context.Background()); err != nil {
		t.Fatalf("setup all failed: %v", err)
	}

	want := []string{"first register", "second register", "first setup", "second setup"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("expected events %v, got %v", want, events)
	}
}

// TestLifecycleRegisterFailureStops 验证 Register 失败会停止后续 provider。
func TestLifecycleRegisterFailureStops(t *testing.T) {
	wantErr := errors.New("register failed")
	var events []string

	app := New()
	if err := app.Use(
		&namedTestProvider{
			name: "bad",
			registerFunc: func(ctx context.Context) error {
				events = append(events, "bad register")
				return wantErr
			},
		},
		&namedTestProvider{
			name: "skipped",
			registerFunc: func(ctx context.Context) error {
				events = append(events, "skipped register")
				return nil
			},
		},
	); err != nil {
		t.Fatalf("use providers failed: %v", err)
	}

	err := app.RegisterAll(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected register error, got %v", err)
	}
	if err := app.SetupAll(context.Background()); !errors.Is(err, ErrAppFailed) {
		t.Fatalf("expected setup after failed register to fail, got %v", err)
	}
	if err := app.RegisterAll(context.Background()); !errors.Is(err, ErrAppFailed) {
		t.Fatalf("expected register after failed register to fail, got %v", err)
	}
	if err := app.Use(&namedTestProvider{name: "late"}); !errors.Is(err, ErrAppFailed) {
		t.Fatalf("expected use after failed register to fail, got %v", err)
	}
	if strings.Join(events, ",") != "bad register" {
		t.Fatalf("expected only first provider to register, got %v", events)
	}
}

// TestLifecycleSetupFailureShutsDownPartial 验证 setup 部分失败后只关闭已成功 setup 的 provider。
func TestLifecycleSetupFailureShutsDownPartial(t *testing.T) {
	wantErr := errors.New("setup failed")
	var events []string

	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(
		&stopTestProvider{
			namedTestProvider: &namedTestProvider{name: "ready"},
			shutdownFunc: func(ctx context.Context) error {
				events = append(events, "ready shutdown")
				return nil
			},
		},
		&stopTestProvider{
			namedTestProvider: &namedTestProvider{
				name: "bad",
				setupFunc: func(ctx context.Context) error {
					return wantErr
				},
			},
			shutdownFunc: func(ctx context.Context) error {
				events = append(events, "bad shutdown")
				return nil
			},
		},
	); err != nil {
		t.Fatalf("use providers failed: %v", err)
	}

	if err := app.RegisterAll(context.Background()); err != nil {
		t.Fatalf("register all failed: %v", err)
	}
	err := app.SetupAll(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected setup error, got %v", err)
	}
	if err := app.Use(&namedTestProvider{name: "late"}); !errors.Is(err, ErrAppFailed) {
		t.Fatalf("expected use after failed setup to fail, got %v", err)
	}
	if err := app.ShutdownAll(context.Background()); err != nil {
		t.Fatalf("shutdown all failed: %v", err)
	}
	if err := app.Use(&namedTestProvider{name: "closed"}); !errors.Is(err, ErrAppClosed) {
		t.Fatalf("expected use after shutdown to fail, got %v", err)
	}
	if strings.Join(events, ",") != "ready shutdown" {
		t.Fatalf("expected only ready shutdown, got %v", events)
	}
}

// TestLifecycleRejectsInvalidState 验证生命周期阶段不能重复或越序调用。
func TestLifecycleRejectsInvalidState(t *testing.T) {
	app := New()
	if err := app.SetupAll(context.Background()); !errors.Is(err, ErrProvidersNotRegistered) {
		t.Fatalf("expected setup before register error, got %v", err)
	}
	if err := app.RegisterAll(context.Background()); err != nil {
		t.Fatalf("register all failed: %v", err)
	}
	if err := app.RegisterAll(context.Background()); !errors.Is(err, ErrProvidersAlreadyRegistered) {
		t.Fatalf("expected duplicate register error, got %v", err)
	}
	if err := app.SetupAll(context.Background()); err != nil {
		t.Fatalf("setup all failed: %v", err)
	}
	if err := app.SetupAll(context.Background()); !errors.Is(err, ErrProvidersAlreadySetup) {
		t.Fatalf("expected duplicate setup error, got %v", err)
	}
	if err := app.Use(&namedTestProvider{name: "late"}); !errors.Is(err, ErrAppAlreadyStarted) {
		t.Fatalf("expected late use error, got %v", err)
	}
}

// TestLifecycleShutdownBeforeStartClosesApp 验证未启动时调用 ShutdownAll 会关闭应用生命周期。
func TestLifecycleShutdownBeforeStartClosesApp(t *testing.T) {
	app := New(WithShutdownTimeout(time.Second))
	if err := app.ShutdownAll(context.Background()); err != nil {
		t.Fatalf("shutdown before start failed: %v", err)
	}
	if err := app.Use(&namedTestProvider{name: "late"}); !errors.Is(err, ErrAppClosed) {
		t.Fatalf("expected use after closed error, got %v", err)
	}
	if err := app.RegisterAll(context.Background()); !errors.Is(err, ErrAppClosed) {
		t.Fatalf("expected register after closed error, got %v", err)
	}
	if err := app.SetupAll(context.Background()); !errors.Is(err, ErrAppClosed) {
		t.Fatalf("expected setup after closed error, got %v", err)
	}
}

// TestLifecycleSetupInProgress 验证 setup 正在执行时再次调用 SetupAll 会返回明确错误。
func TestLifecycleSetupInProgress(t *testing.T) {
	setupStarted := make(chan struct{})
	releaseSetup := make(chan struct{})

	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(&namedTestProvider{
		name: "blocked",
		setupFunc: func(ctx context.Context) error {
			close(setupStarted)
			<-releaseSetup
			return nil
		},
	}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}
	if err := app.RegisterAll(context.Background()); err != nil {
		t.Fatalf("register all failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- app.SetupAll(context.Background())
	}()

	waitClosed(t, setupStarted)
	if err := app.SetupAll(context.Background()); !errors.Is(err, ErrSetupInProgress) {
		t.Fatalf("expected setup in progress error, got %v", err)
	}

	close(releaseSetup)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("setup all failed: %v", err)
		}
	case <-timeAfter(testTimeout):
		t.Fatal("timeout waiting for setup")
	}
}

// TestLifecycleLogsUseLowercaseMessages 验证生命周期日志消息使用小写英文。
func TestLifecycleLogsUseLowercaseMessages(t *testing.T) {
	buffer, restore := captureLogs(t)
	defer restore()

	app := New(WithShutdownTimeout(time.Second))
	if err := app.Use(&stopTestProvider{
		namedTestProvider: &namedTestProvider{name: "logged"},
	}); err != nil {
		t.Fatalf("use provider failed: %v", err)
	}
	if err := app.RegisterAll(context.Background()); err != nil {
		t.Fatalf("register all failed: %v", err)
	}
	if err := app.SetupAll(context.Background()); err != nil {
		t.Fatalf("setup all failed: %v", err)
	}
	if err := app.ShutdownAll(context.Background()); err != nil {
		t.Fatalf("shutdown all failed: %v", err)
	}

	logs := buffer.String()
	for _, message := range []string{"provider register", "provider setup", "provider shutdown"} {
		if !strings.Contains(logs, `"message":"`+message+`"`) {
			t.Fatalf("expected log message %q in %s", message, logs)
		}
	}
}

// captureLogs 将 zerolog 全局 logger 替换为内存 buffer。
func captureLogs(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()

	var buffer bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&buffer)

	return &buffer, func() {
		log.Logger = previous
	}
}
