// Package ioc 是一个参考 Laravel Container 思路、面向 Go 优化的最小 IoC 容器,同时负责应用生命周期。
//
// 设计要点:
//   - 只管理"类型"或"命名类型":key = (reflect.Type, name),全部单例。
//   - 两种绑定:Provide 注册构造函数 func(deps...) T 或 func(deps...) (T, error),依赖按参数类型注入;
//     Instance 绑定现成实例,按静态类型 T 登记(可显式写成接口类型)。
//   - 生命周期接口 Component / Server / Shutdowner 全部可选,实现了什么就参与什么阶段。
//   - 构造是惰性的,依赖先于依赖者构造;Setup 在构造完成后立即执行,因此天然满足"依赖先 Setup";
//     Boot 按注册顺序逐个构造,Shutdown 按 Setup 的逆序执行。
//   - 在某个组件的构造函数或 Setup 里 Instance 的实例归该组件所有:容器只负责存取,
//     不调用它的 Setup / Serve / Shutdown(由所属组件自己管理)。
//     这类实例要等所属组件 Setup 后才存在,用 GetFrom[T, Owner] 取可以不依赖注册顺序。
//   - 注册期的编程错误(非法构造函数、重复注册、nil 实例)直接 panic,
//     运行期错误(找不到依赖、循环依赖、构造或 Setup 失败)返回 error。
//
// 典型用法:
//
//	c := ioc.New()
//	c.Instance(&ConfigProvider{}).Provide(NewUserService)
//	err := c.Run(ctx)            // 服务模式:Boot → 并发 Serve → Shutdown
//	err := c.Exec(ctx, migrate)  // 一次性模式:Boot → fn → Shutdown,不启动 Server
//
// Setup / Serve / Shutdown 收到的 ctx 带着容器和全局 zerolog logger,
// 可用 ioc.FromContext(ctx).MustGet[T]() 取依赖、log.Ctx(ctx) 打日志。
//
// 并发约定:Provide / Instance / Boot 在同一个 goroutine 完成(通常是 main);
// Boot 之后所有绑定都已构造,Get 是纯读操作,可以在任意 goroutine 使用。
// 不要在 Serve 里或 Boot 之后再绑定。
package ioc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

// ---- 生命周期接口:全部可选,按实现了什么来参与对应阶段 ----

// Component 在构造完成后立即被调用一次(依赖先于依赖者)。
type Component interface {
	Setup(ctx context.Context) error
}

// Server 是阻塞式服务:Run 为每个 Server 起一个 goroutine,ctx 取消或 Shutdown 时应返回。
type Server interface {
	Serve(ctx context.Context) error
}

// Shutdowner 在容器 Shutdown 时按 Setup 逆序被调用。
type Shutdowner interface {
	Shutdown(ctx context.Context) error
}

// ---- 错误 ----

var (
	// ErrNotFound 表示没有匹配的绑定。
	ErrNotFound = errors.New("ioc: binding not found")
	// ErrAmbiguous 表示同一类型匹配到多个绑定,需要指定名字。
	ErrAmbiguous = errors.New("ioc: ambiguous binding, specify a name")
	// ErrCycle 表示构造过程中出现循环依赖。
	ErrCycle = errors.New("ioc: dependency cycle")
	// ErrNoContainer 表示 ctx 不是容器派生的(FromContext 返回了 nil)。
	ErrNoContainer = errors.New("ioc: no container in context")
	// ErrShutdownTimeout 表示收尾阶段没能在 shutdownTimeout 内完成。
	// 此时 Run / Exec 已返回,但未退出的 goroutine 仍在运行,容器无法强制终止它们。
	ErrShutdownTimeout = errors.New("ioc: shutdown timed out")
)

// serveError 标记 Serve 返回的错误,用来和信号、ctx 取消等正常退出原因区分。
type serveError struct{ error }

// ---- 内部结构 ----

type key struct {
	typ  reflect.Type
	name string
}

func (k key) String() string {
	if k.name == "" {
		return k.typ.String()
	}
	return k.typ.String() + "#" + k.name
}

type binding struct {
	key   key
	ctor  reflect.Value // 仅 Provide 绑定有效
	value reflect.Value // Instance 绑定预先填好,Provide 绑定构造后填入
	built bool          // 已构造并 Setup;组件自有的实例一登记即为 true
}

var errorType = reflect.TypeFor[error]()

// Container 是 IoC 容器,并发约定见包注释。
type Container struct {
	seq   []*binding      // 注册顺序
	order []*binding      // 构造完成顺序 = Setup 顺序,Shutdown 时逆序
	stack []key           // 正在构造的链,用于循环检测
	ctx   context.Context // Setup 使用的基础 ctx,由 Boot 设置

	shutdownTimeout time.Duration
	signals         []os.Signal
}

// Option 用于配置 New 创建的容器。
type Option func(*Container)

// WithShutdownTimeout 设置收尾阶段(Shutdown + 等待 Serve 返回)的总时限,默认 30s,非正数表示不限时。
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Container) { c.shutdownTimeout = d }
}

// WithSignals 设置 Run / Exec 监听的退出信号,默认 os.Interrupt 和 SIGTERM;不传任何信号即关闭监听。
func WithSignals(sigs ...os.Signal) Option {
	return func(c *Container) { c.signals = sigs }
}

// New 创建一个空容器。
func New(opts ...Option) *Container {
	c := &Container{
		ctx:             context.Background(),
		shutdownTimeout: 30 * time.Second,
		signals:         []os.Signal{os.Interrupt, syscall.SIGTERM},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type ctxKey struct{}

// WithContext 把容器和当前全局 zerolog logger 写入 ctx。
// 容器在每次调用 Setup / Serve / Shutdown 前都会现做一次,所以某个组件替换全局 logger 后,后续组件立即可见。
func (c *Container) WithContext(ctx context.Context) context.Context {
	return log.Logger.WithContext(context.WithValue(ctx, ctxKey{}, c))
}

// FromContext 从 ctx 里取回容器;不是容器派生的 ctx 返回 nil(此时 Get 返回 ErrNoContainer)。
func FromContext(ctx context.Context) *Container {
	c, _ := ctx.Value(ctxKey{}).(*Container)
	return c
}

// ---- 注册 ----

// Provide 注册一个构造函数,签名必须是 func(deps...) T 或 func(deps...) (T, error)。
// 参数按类型从容器解析(不带名字,多个同类型绑定时请改为注入 *Container 后自行 Get)。
func (c *Container) Provide(ctor any, name ...string) *Container {
	if len(c.stack) > 0 {
		panic("ioc: Provide called inside a constructor or Setup")
	}
	v := reflect.ValueOf(ctor)
	if v.Kind() != reflect.Func || v.Type().IsVariadic() {
		panic(fmt.Sprintf("ioc: constructor must be a non-variadic func, got %T", ctor))
	}
	t := v.Type()
	if n := t.NumOut(); n < 1 || n > 2 || n == 2 && t.Out(1) != errorType {
		panic(fmt.Sprintf("ioc: constructor %T must return T or (T, error)", ctor))
	}
	return c.add(&binding{key: key{typ: t.Out(0), name: cmp.Or(name...)}, ctor: v})
}

// Instance 绑定一个现成的实例(对应 Laravel 的 instance),按静态类型 T 登记:
// c.Instance(client) 绑定为 client 的声明类型,c.Instance[redis.UniversalClient](client) 绑定为接口类型。
//
// Boot 之前绑定的实例会在 Boot 时执行 Setup(若实现),并参与 Serve / Shutdown;
// 在构造函数或 Setup 里绑定的实例归当前组件所有,容器只负责存取。
func (c *Container) Instance[T any](v T, name ...string) *Container {
	rv := reflect.ValueOf(&v).Elem() // 经指针取值,T 为接口类型时不会退化成动态类型
	if isNil(rv) {
		panic(fmt.Sprintf("ioc: Instance(nil) for %s", rv.Type()))
	}
	return c.add(&binding{
		key:   key{typ: rv.Type(), name: cmp.Or(name...)},
		value: rv,
		built: len(c.stack) > 0,
	})
}

// add 登记绑定,重复注册直接 panic。
func (c *Container) add(b *binding) *Container {
	if slices.ContainsFunc(c.seq, func(x *binding) bool { return x.key == b.key }) {
		panic(fmt.Sprintf("ioc: duplicate binding %s", b.key))
	}
	c.seq = append(c.seq, b)
	return c
}

// isNil 判断 v 是否为 nil 指针或 nil 接口(含接口里包着的 nil 指针)。
func isNil(v reflect.Value) bool {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return !v.IsValid() || v.Kind() == reflect.Pointer && v.IsNil()
}

// ---- 解析 ----

// Get 按类型(和可选的名字)取实例,未构造时会惰性构造并 Setup。
// 不传名字时,若该类型只有一个绑定(无论是否命名)则直接返回,多个则报 ErrAmbiguous。
// T 可以是接口类型,会按可赋值性匹配。
func (c *Container) Get[T any](name ...string) (T, error) {
	var result T
	v, err := c.resolve(key{typ: reflect.TypeFor[T](), name: cmp.Or(name...)})
	if err != nil {
		return result, err
	}
	// 用反射赋值而不是类型断言:lookup 按可赋值性匹配(命名切片 -> 底层切片、
	// 双向 chan -> 单向 chan 等),这些情况断言会 panic,Set 不会。
	reflect.ValueOf(&result).Elem().Set(v)
	return result, nil
}

// MustGet 是 Get 的 panic 版本。
func (c *Container) MustGet[T any](name ...string) T {
	v, err := c.Get[T](name...)
	if err != nil {
		panic(err)
	}
	return v
}

// Ensure 确保 Owner 已构造并 Setup;容器里没有(唯一匹配的)Owner 绑定时什么都不做。
func (c *Container) Ensure[Owner any]() error {
	if c == nil {
		return nil
	}
	b, err := c.lookup(key{typ: reflect.TypeFor[Owner]()})
	if err != nil {
		return nil
	}
	_, err = c.build(b)
	return err
}

// GetFrom 取组件 Owner 在自己的 Setup 里绑定的实例:先 Ensure[Owner],再按 Get 的规则取 (T, name)。
//
// 组件自有的实例在所属组件 Setup 之前并不存在,直接 Get 只有排在它后面才取得到;
// 经由 GetFrom 取则与注册顺序无关——Owner 即使注册得更靠后也会被提前 Setup(Shutdown 相应更晚),
// 互相依赖会得到 ErrCycle。没有注册 Owner 时等同于 Get,方便测试里手工绑定实例。
//
// 只有 Setup 结果与执行时机无关的组件才适合当 Owner:如果它的 Setup 要读写某个靠注册顺序收集的东西
// (例如拦截器链),提前执行会静默改变结果,这类组件的产物请继续用 Get,让顺序不对时明确报 ErrNotFound。
func (c *Container) GetFrom[T, Owner any](name ...string) (T, error) {
	if err := c.Ensure[Owner](); err != nil {
		var zero T
		return zero, err
	}
	return c.Get[T](name...)
}

// MustGetFrom 是 GetFrom 的 panic 版本。
func (c *Container) MustGetFrom[T, Owner any](name ...string) T {
	v, err := c.GetFrom[T, Owner](name...)
	if err != nil {
		panic(err)
	}
	return v
}

func (c *Container) resolve(k key) (reflect.Value, error) {
	if c == nil {
		return reflect.Value{}, fmt.Errorf("%w: %s", ErrNoContainer, k)
	}
	b, err := c.lookup(k)
	if err != nil {
		return reflect.Value{}, err
	}
	return c.build(b)
}

// lookup 一次扫描:key 完全相等直接命中;否则按"可赋值 + 名字匹配"收集(支持接口类型、省略名字)。
func (c *Container) lookup(k key) (*binding, error) {
	var found []*binding
	for _, b := range c.seq {
		if b.key == k {
			return b, nil
		}
		if (k.name == "" || b.key.name == k.name) && b.key.typ.AssignableTo(k.typ) {
			found = append(found, b)
		}
	}
	switch len(found) {
	case 0:
		return nil, fmt.Errorf("%w: %s", ErrNotFound, k)
	case 1:
		return found[0], nil
	default:
		keys := make([]key, len(found))
		for i, b := range found {
			keys[i] = b.key
		}
		return nil, fmt.Errorf("%w: %s matches %s", ErrAmbiguous, k, joinKeys(keys, ", "))
	}
}

// build 构造 b 并执行 Setup,成功后记入 order;已构造的直接返回。
func (c *Container) build(b *binding) (reflect.Value, error) {
	if b.built {
		return b.value, nil
	}

	// stack 同时覆盖构造函数参数链和构造函数 / Setup 里通过 c.Get 绕回来的链。
	if slices.Contains(c.stack, b.key) {
		return reflect.Value{}, fmt.Errorf("%w: %s", ErrCycle, joinKeys(append(slices.Clone(c.stack), b.key), " -> "))
	}
	c.stack = append(c.stack, b.key)
	defer func() { c.stack = c.stack[:len(c.stack)-1] }()

	v, err := c.construct(b)
	if err != nil {
		return reflect.Value{}, err
	}
	if comp, ok := v.Interface().(Component); ok {
		ctx := c.WithContext(c.ctx)
		log.Ctx(ctx).Info().Stringer("component", b.key).Msg("component setup")
		if err := comp.Setup(ctx); err != nil {
			return reflect.Value{}, fmt.Errorf("ioc: setup %s: %w", b.key, err)
		}
	}
	b.built, b.value = true, v
	c.order = append(c.order, b)
	return v, nil
}

// construct 返回实例:Instance 绑定直接用现成实例,否则解析参数并调用构造函数。
func (c *Container) construct(b *binding) (reflect.Value, error) {
	if !b.ctor.IsValid() {
		return b.value, nil
	}
	t := b.ctor.Type()
	args := make([]reflect.Value, t.NumIn())
	for i := range args {
		if p := t.In(i); p == reflect.TypeFor[*Container]() {
			args[i] = reflect.ValueOf(c)
		} else if v, err := c.resolve(key{typ: p}); err != nil {
			return reflect.Value{}, fmt.Errorf("%s: %w", b.key, err)
		} else {
			args[i] = v
		}
	}
	out := b.ctor.Call(args)
	if len(out) == 2 && !out[1].IsNil() {
		return reflect.Value{}, fmt.Errorf("ioc: construct %s: %w", b.key, out[1].Interface().(error))
	}
	if isNil(out[0]) {
		return reflect.Value{}, fmt.Errorf("ioc: constructor of %s returned nil", b.key)
	}
	return out[0], nil
}

func joinKeys(keys []key, sep string) string {
	s := make([]string, len(keys))
	for i, k := range keys {
		s[i] = k.String()
	}
	return strings.Join(s, sep)
}

// ---- 生命周期 ----

// Boot 按注册顺序构造所有绑定(依赖会被递归提前构造),并逐个 Setup。
// 失败时已 Setup 的部分不会自动回滚,调用方应接着调用 Shutdown(Run / Exec 会代劳)。
func (c *Container) Boot(ctx context.Context) error {
	c.ctx = ctx
	// Setup 里追加的实例归组件所有、登记时已是 built,不需要再遍历到。
	for _, b := range c.seq {
		if _, err := c.build(b); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown 按 Setup 逆序调用所有 Shutdowner,失败不中断,错误用 errors.Join 汇总。
// 重复调用是安全的:每个组件只会被关闭一次。
func (c *Container) Shutdown(ctx context.Context) error {
	ctx = c.WithContext(ctx)
	order := c.order
	c.order = nil

	var errs []error
	for _, b := range slices.Backward(order) {
		if s, ok := b.value.Interface().(Shutdowner); ok {
			log.Ctx(ctx).Info().Stringer("component", b.key).Msg("component shutdown")
			if err := s.Shutdown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("ioc: shutdown %s: %w", b.key, err))
			}
		}
	}
	return errors.Join(errs...)
}

// Run = Boot + 并发运行所有 Server + 等待 + Shutdown。
//
//   - 任一 Server 返回 error 会取消其余 Server 并触发 Shutdown,该 error 作为 Run 的返回值;
//   - 所有 Server 正常返回(或根本没有 Server)时 Run 结束并返回 nil;
//   - ctx 取消、收到 WithSignals 配置的信号都视为正常退出;收尾期间再次收到信号会恢复默认行为(进程立即终止);
//   - 收尾阶段整体受 shutdownTimeout 约束,超时返回 ErrShutdownTimeout。
func (c *Container) Run(ctx context.Context) error {
	ctx, stop := c.notify(ctx)
	defer stop()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	if err := c.Boot(ctx); err != nil {
		return c.finish(ctx, err, func() {}) // 已 Setup 的部分也要收尾
	}

	var wg sync.WaitGroup
	serveCtx := c.WithContext(ctx)
	for _, b := range c.order {
		if s, ok := b.value.Interface().(Server); ok {
			wg.Go(func() {
				log.Ctx(serveCtx).Info().Stringer("component", b.key).Msg("component serve")
				if err := s.Serve(serveCtx); err != nil {
					cancel(serveError{fmt.Errorf("ioc: serve %s: %w", b.key, err)})
				}
			})
		}
	}
	go func() {
		wg.Wait()
		cancel(nil)
	}()

	<-ctx.Done()
	stop() // 收尾期间不再拦截信号,第二次 Ctrl-C 可以直接结束进程

	// 只有 Serve 报错才算失败;ctx 取消后才返回的 Serve 错误不会覆盖已有的 cause。
	var err error
	if e, ok := errors.AsType[serveError](context.Cause(ctx)); ok {
		err = e.error
	}
	return c.finish(ctx, err, wg.Wait)
}

// Exec = Boot + fn + Shutdown,用于 CLI 子命令、批处理等一次性任务;不会启动 Server。
// fn 收到的 ctx 带着容器,收到退出信号时会被取消,由 fn 自行决定是否提前返回。
// fn 的错误与 Shutdown 的错误用 errors.Join 汇总。
func (c *Container) Exec(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, stop := c.notify(ctx)
	defer stop()

	err := c.Boot(ctx)
	if err == nil {
		err = fn(c.WithContext(ctx))
	}
	stop()
	return c.finish(ctx, err, func() {})
}

// notify 让 ctx 在收到配置的信号时取消;未配置信号时原样返回。
func (c *Container) notify(ctx context.Context) (context.Context, context.CancelFunc) {
	if len(c.signals) == 0 {
		return ctx, func() {}
	}
	return signal.NotifyContext(ctx, c.signals...)
}

// finish 在 shutdownTimeout 内执行 Shutdown(先关,让阻塞的 Serve 返回)再等待 wait。
// 超时只让调用方返回,不会终止仍在运行的 Shutdown / Serve goroutine。
func (c *Container) finish(ctx context.Context, cause error, wait func()) error {
	ctx = context.WithoutCancel(ctx)
	if c.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.shutdownTimeout)
		defer cancel()
	}

	done := make(chan error, 1)
	go func() {
		err := c.Shutdown(ctx)
		wait()
		done <- err
	}()
	select {
	case err := <-done:
		return errors.Join(cause, err)
	case <-ctx.Done(): // 不限时的时候 Done 为 nil,这个分支永不触发
		return errors.Join(cause, fmt.Errorf("%w after %s", ErrShutdownTimeout, c.shutdownTimeout))
	}
}
