package ioc

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

const defaultShutdownTimeout = 30 * time.Second

// Option 定义创建应用时可选的配置函数。
type Option func(*App)

type providerEntry struct {
	name     string
	provider Provider
}

type appPhase uint8

const (
	phaseNew appPhase = iota
	phaseRegistering
	phaseRegistered
	phaseSettingUp
	phaseSetup
	phaseRunning
	phaseClosed
	phaseFailed
)

// App 管理服务提供者、容器和应用生命周期。
type App struct {
	mu              sync.Mutex
	container       *Container
	providers       []providerEntry
	providerNames   map[string]struct{}
	phase           appPhase
	setupCount      int
	shutdownTimeout time.Duration
}

// New 创建一个带默认容器和生命周期配置的应用。
func New(opts ...Option) *App {
	app := &App{
		container:       NewContainer(),
		providerNames:   make(map[string]struct{}),
		shutdownTimeout: defaultShutdownTimeout,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(app)
		}
	}

	return app
}

// WithShutdownTimeout 设置 ShutdownAll 的最大执行时间，非正数表示不设置超时。
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(app *App) {
		app.shutdownTimeout = timeout
	}
}

// Use 注册一个或多个服务提供者。
func (a *App) Use(providers ...Provider) error {
	if len(providers) == 0 {
		return nil
	}

	entries := make([]providerEntry, 0, len(providers))
	names := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		name, err := providerName(provider)
		if err != nil {
			return err
		}
		if _, ok := names[name]; ok {
			return wrapError(ErrProviderNameExists, name)
		}

		names[name] = struct{}{}
		entries = append(entries, providerEntry{name: name, provider: provider})
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	switch a.phase {
	case phaseNew:
	case phaseFailed:
		return ErrAppFailed
	case phaseClosed:
		return ErrAppClosed
	default:
		return ErrAppAlreadyStarted
	}

	for _, entry := range entries {
		if _, ok := a.providerNames[entry.name]; ok {
			return wrapError(ErrProviderNameExists, entry.name)
		}
	}
	for _, entry := range entries {
		a.providerNames[entry.name] = struct{}{}
		a.providers = append(a.providers, entry)
	}

	return nil
}

// Container 返回应用内部容器。
func (a *App) Container() *Container {
	return a.container
}

// RegisterAll 按注册顺序执行全部 provider 的 Register。
func (a *App) RegisterAll(ctx context.Context) error {
	ctx = a.lifecycleContext(ctx)

	providers, err := a.beginRegister()
	if err != nil {
		return err
	}

	for _, entry := range providers {
		log.Ctx(ctx).Info().Str("provider", entry.name).Msg("provider register")
		if err := entry.provider.Register(ctx); err != nil {
			wrapped := providerError("register", entry.name, err)
			log.Ctx(ctx).Error().Err(wrapped).Str("provider", entry.name).Msg("provider register failed")
			a.finishRegister(false)
			return wrapped
		}
	}

	a.finishRegister(true)
	return nil
}

// SetupAll 按注册顺序执行全部 provider 的 Setup。
func (a *App) SetupAll(ctx context.Context) error {
	ctx = a.lifecycleContext(ctx)

	providers, err := a.beginSetup()
	if err != nil {
		return err
	}

	for _, entry := range providers {
		log.Ctx(ctx).Info().Str("provider", entry.name).Msg("provider setup")
		if err := entry.provider.Setup(ctx); err != nil {
			wrapped := providerError("setup", entry.name, err)
			log.Ctx(ctx).Error().Err(wrapped).Str("provider", entry.name).Msg("provider setup failed")
			a.finishSetup(false)
			return wrapped
		}

		a.markSetupDone()
	}

	a.finishSetup(true)
	return nil
}

// ShutdownAll 按 Setup 成功的逆序执行全部 Shutdowner。
func (a *App) ShutdownAll(ctx context.Context) error {
	ctx = a.lifecycleContext(ctx)
	if a.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.shutdownTimeout)
		defer cancel()
	}

	providers := a.takeSetupProviders()
	slices.Reverse(providers)

	var errs []error
	for _, entry := range providers {
		stopper, ok := entry.provider.(Shutdowner)
		if !ok {
			continue
		}

		log.Ctx(ctx).Info().Str("provider", entry.name).Msg("provider shutdown")
		if err := stopper.Shutdown(ctx); err != nil {
			wrapped := providerError("shutdown", entry.name, err)
			log.Ctx(ctx).Error().Err(wrapped).Str("provider", entry.name).Msg("provider shutdown failed")
			errs = append(errs, wrapped)
		}
	}

	return errors.Join(errs...)
}

// Run 执行完整生命周期: Register → Setup → (fn 或 Serve) → Shutdown。
//
//   - fn == nil:  serve 模式,起 Server 阻塞到信号或任一 Server 退出
//   - fn != nil:  一次性模式,跳过 Server,fn 跑完即退出 (CLI 子命令、批处理场景)
//
// fn 在所有 Provider Setup 完毕、容器与 logger 已写入 ctx 的状态下被调用,
// 内部可直接 ioc.MustGet[T](ctx) 取依赖。SIGINT/SIGTERM 通过 ctx 传给 fn,
// fn 自行决定是否提前返回。fn(或 Serve) 与 Shutdown 错误经 errors.Join 聚合返回。
func (a *App) Run(ctx context.Context, fn func(context.Context) error) error {
	ctx = a.lifecycleContext(ctx)

	signalCtx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	runCtx, cancelRun := context.WithCancel(signalCtx)
	defer cancelRun()

	if err := a.RegisterAll(runCtx); err != nil {
		return err
	}
	if err := a.SetupAll(runCtx); err != nil {
		shutdownErr := a.ShutdownAll(context.WithoutCancel(ctx))
		return errors.Join(err, shutdownErr)
	}

	var runErr error
	if fn != nil {
		runErr = fn(runCtx)
	} else {
		a.beginRunning()
		runErr = a.serveOrWait(runCtx)
	}
	cancelRun()

	shutdownErr := a.ShutdownAll(context.WithoutCancel(ctx))
	return errors.Join(runErr, shutdownErr)
}

// beginRegister 标记注册阶段开始，并返回 provider 快照。
func (a *App) beginRegister() ([]providerEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch a.phase {
	case phaseNew:
		a.phase = phaseRegistering
		return slices.Clone(a.providers), nil
	case phaseFailed:
		return nil, ErrAppFailed
	case phaseClosed:
		return nil, ErrAppClosed
	default:
		return nil, ErrProvidersAlreadyRegistered
	}
}

// finishRegister 根据注册结果更新应用生命周期阶段。
func (a *App) finishRegister(ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if ok {
		a.phase = phaseRegistered
		return
	}

	a.phase = phaseFailed
}

// lifecycleContext 将容器和默认 logger 写入生命周期上下文。
func (a *App) lifecycleContext(ctx context.Context) context.Context {
	ctx = a.container.WithContext(ctx)
	return log.Logger.WithContext(ctx)
}

// beginSetup 标记 setup 阶段开始，并返回 provider 快照。
func (a *App) beginSetup() ([]providerEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch a.phase {
	case phaseNew:
		return nil, ErrProvidersNotRegistered
	case phaseRegistered:
		a.phase = phaseSettingUp
		a.setupCount = 0
		return slices.Clone(a.providers), nil
	case phaseSettingUp:
		return nil, ErrSetupInProgress
	case phaseSetup, phaseRunning:
		return nil, ErrProvidersAlreadySetup
	case phaseRegistering:
		return nil, ErrAppAlreadyStarted
	case phaseFailed:
		return nil, ErrAppFailed
	case phaseClosed:
		return nil, ErrAppClosed
	default:
		return nil, ErrAppAlreadyStarted
	}
}

// finishSetup 根据 setup 结果更新应用生命周期阶段。
func (a *App) finishSetup(ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if ok {
		a.phase = phaseSetup
		return
	}

	a.phase = phaseFailed
}

// markSetupDone 记录已经成功 setup 的 provider 数量。
func (a *App) markSetupDone() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.setupCount++
}

// takeSetupProviders 取出已成功 setup 的 provider，避免重复 shutdown。
func (a *App) takeSetupProviders() []providerEntry {
	a.mu.Lock()
	defer a.mu.Unlock()

	count := min(a.setupCount, len(a.providers))
	providers := slices.Clone(a.providers[:count])
	a.setupCount = 0
	a.phase = phaseClosed

	return providers
}

// beginRunning 标记应用已经进入运行期。
func (a *App) beginRunning() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.phase == phaseSetup {
		a.phase = phaseRunning
	}
}

// providerList 返回当前服务提供者列表的快照。
func (a *App) providerList() []providerEntry {
	a.mu.Lock()
	defer a.mu.Unlock()

	return slices.Clone(a.providers)
}

// serveOrWait 运行所有 Server，或在没有 Server 时等待上下文取消。
func (a *App) serveOrWait(ctx context.Context) error {
	providers := a.providerList()
	servers := make([]providerEntry, 0, len(providers))
	for _, entry := range providers {
		if _, ok := entry.provider.(Server); ok {
			servers = append(servers, entry)
		}
	}

	if len(servers) == 0 {
		log.Ctx(ctx).Info().Msg("application wait")
		<-ctx.Done()
		return nil
	}

	results := make(chan serveResult, len(servers))
	for _, entry := range servers {
		go serveProvider(ctx, entry, results)
	}

	select {
	case result := <-results:
		if err := normalizeServeError(ctx, result); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("provider", result.name).Msg("provider serve failed")
			return err
		}

		log.Ctx(ctx).Info().Str("provider", result.name).Msg("provider serve stopped")
		return nil
	case <-ctx.Done():
		return nil
	}
}

type serveResult struct {
	name string
	err  error
}

// normalizeServeError 将上下文取消导致的 Serve 错误归一化为正常退出。
func normalizeServeError(ctx context.Context, result serveResult) error {
	if result.err == nil {
		return nil
	}

	if ctx.Err() != nil && (errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded)) {
		return nil
	}

	return providerError("serve", result.name, result.err)
}

// serveProvider 执行单个 provider 的 Serve 并上报结果。
func serveProvider(ctx context.Context, entry providerEntry, results chan<- serveResult) {
	server := entry.provider.(Server)
	log.Ctx(ctx).Info().Str("provider", entry.name).Msg("provider serve")
	results <- serveResult{name: entry.name, err: server.Serve(ctx)}
}
