package ioc

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ---- 测试替身 ----

type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

// comp 实现 Component + Shutdowner,把调用记到 recorder。
type comp struct {
	name     string
	rec      *recorder
	setup    func(ctx context.Context) error
	shutdown func(ctx context.Context) error
}

func (c *comp) Setup(ctx context.Context) error {
	c.rec.add("setup " + c.name)
	if c.setup != nil {
		return c.setup(ctx)
	}
	return nil
}

func (c *comp) Shutdown(ctx context.Context) error {
	c.rec.add("shutdown " + c.name)
	if c.shutdown != nil {
		return c.shutdown(ctx)
	}
	return nil
}

// server 在 comp 之上再实现 Server。
type server struct {
	*comp
	serve func(ctx context.Context) error
}

func (s *server) Serve(ctx context.Context) error {
	s.rec.add("serve " + s.name)
	return s.serve(ctx)
}

// 三层依赖:svc -> db -> cfg,用于构造函数注入。
type (
	cfg struct{ *comp }
	db  struct {
		*comp
		cfg *cfg
	}
	svc struct {
		*comp
		db *db
	}
)

type greeter interface{ Greet() string }

type english struct{}

func (*english) Greet() string { return "hi" }

func newComp(rec *recorder, name string) *comp { return &comp{name: name, rec: rec} }

// blockUntilDone 是最常见的 Serve 实现:阻塞到 ctx 取消。
func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// newTest 创建不监听信号的容器,避免测试之间互相影响。
func newTest(opts ...Option) *Container {
	return New(append([]Option{WithSignals()}, opts...)...)
}

// within 在 2s 内等待 fn 返回,超时直接失败,防止生命周期测试挂死。
func within(t *testing.T, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
		return nil
	}
}

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q", want)
		}
		if msg, _ := r.(string); !strings.Contains(msg, want) {
			t.Fatalf("panic = %v, want containing %q", r, want)
		}
	}()
	fn()
}

func wantEvents(t *testing.T, rec *recorder, want ...string) {
	t.Helper()
	if got := rec.list(); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

// ---- 绑定与解析 ----

func TestProvideInjectsDependenciesAndOrdersLifecycle(t *testing.T) {
	rec := &recorder{}
	c := newTest()
	// 故意倒着注册:依赖仍应先于依赖者构造和 Setup。
	c.Provide(func(d *db) *svc { return &svc{newComp(rec, "svc"), d} }).
		Provide(func(c *cfg) (*db, error) { return &db{newComp(rec, "db"), c}, nil }).
		Provide(func() *cfg { return &cfg{newComp(rec, "cfg")} })

	if err := c.Boot(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := c.MustGet[*svc]()
	if s.db == nil || s.db.cfg == nil || s.db != c.MustGet[*db]() {
		t.Fatal("dependencies are not injected as singletons")
	}
	if err := c.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, rec,
		"setup cfg", "setup db", "setup svc",
		"shutdown svc", "shutdown db", "shutdown cfg",
	)
}

func TestProvideInjectsContainer(t *testing.T) {
	c := newTest()
	c.Instance(42).Provide(func(c *Container) string {
		return strings.Repeat("x", c.MustGet[int]()/14)
	})
	if got := c.MustGet[string](); got != "xxx" {
		t.Fatalf("got %q", got)
	}
}

func TestInstanceBindsStaticType(t *testing.T) {
	c := newTest()
	c.Instance[greeter](&english{})

	if got := c.MustGet[greeter]().Greet(); got != "hi" {
		t.Fatalf("got %q", got)
	}
	// 绑定成接口后,不能再按具体类型取出来。
	if _, err := c.Get[*english](); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestGetMatchesByAssignability(t *testing.T) {
	c := newTest()
	c.Instance(&english{})

	if got := c.MustGet[greeter]().Greet(); got != "hi" {
		t.Fatalf("got %q", got)
	}
	if c.MustGet[*english]() != c.MustGet[greeter]() {
		t.Fatal("interface lookup returned a different instance")
	}
}

func TestNamedBindings(t *testing.T) {
	c := newTest()
	c.Instance("only", "single")
	c.Instance(1, "one").Instance(2, "two")

	// 只有一个绑定时可以省略名字。
	if got := c.MustGet[string](); got != "only" {
		t.Fatalf("got %q", got)
	}
	if got := c.MustGet[int]("two"); got != 2 {
		t.Fatalf("got %d", got)
	}
	if _, err := c.Get[int](); !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("err = %v, want ErrAmbiguous", err)
	}
	if _, err := c.Get[int]("three"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}

}

// owner 是在 Setup 里绑定产物的组件;other 只是另一个类型,用来构造互相依赖。
type (
	owner struct{ *comp }
	other struct{ *comp }
)

func TestGetFromSetsUpOwnerFirst(t *testing.T) {
	rec := &recorder{}
	c := newTest()
	// 使用方排在产物的所有者之前。
	c.Instance(&comp{name: "user", rec: rec, setup: func(ctx context.Context) error {
		c := FromContext(ctx)
		if _, err := c.Get[string](); !errors.Is(err, ErrNotFound) {
			t.Errorf("plain Get before the owner is set up: err = %v, want ErrNotFound", err)
		}
		for range 2 { // 第二次不会重复 Setup
			if got := c.MustGetFrom[string, *owner](); got != "product" {
				t.Errorf("got %q", got)
			}
		}
		return nil
	}})
	c.Instance(&owner{&comp{name: "owner", rec: rec, setup: func(ctx context.Context) error {
		FromContext(ctx).Instance("product")
		return nil
	}}})

	if err := c.Boot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := c.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	// 所有者在使用方 Setup 中途被拉起,先完成,所以后关闭。
	wantEvents(t, rec, "setup user", "setup owner", "shutdown user", "shutdown owner")
}

func TestGetFromWithoutOwner(t *testing.T) {
	c := newTest()
	c.Instance("product")

	// 没有注册 Owner 时等同于 Get。
	if got, err := c.GetFrom[string, *owner](); err != nil || got != "product" {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := c.GetFrom[int, *owner](); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if err := c.Ensure[*owner](); err != nil {
		t.Fatalf("Ensure without owner: %v", err)
	}

	var none *Container
	if err := none.Ensure[*owner](); err != nil {
		t.Fatalf("Ensure on nil container: %v", err)
	}
	if _, err := none.GetFrom[string, *owner](); !errors.Is(err, ErrNoContainer) {
		t.Fatalf("err = %v, want ErrNoContainer", err)
	}
}

// Owner 的 Setup 失败时返回的是它的错误:即使失败原因本身也是 ErrNotFound,
// 也不会被当成"没有注册 Owner"而掩盖成"产物找不到"。
func TestGetFromSurfacesOwnerSetupError(t *testing.T) {
	c := newTest()
	c.Instance(&owner{&comp{rec: &recorder{}, setup: func(ctx context.Context) error {
		_, err := FromContext(ctx).Get[float64]()
		return err
	}}})

	_, err := c.GetFrom[string, *owner]()
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "ioc: setup *ioc.owner: ") ||
		!strings.Contains(err.Error(), "float64") {
		t.Fatalf("err = %v, want the owner setup error", err)
	}
}

func TestGetFromDetectsMutualDependency(t *testing.T) {
	rec := &recorder{}
	c := newTest()
	c.Instance(&owner{&comp{rec: rec, setup: func(ctx context.Context) error {
		_, err := FromContext(ctx).GetFrom[int, *other]()
		return err
	}}})
	c.Instance(&other{&comp{rec: rec, setup: func(ctx context.Context) error {
		_, err := FromContext(ctx).GetFrom[string, *owner]()
		return err
	}}})

	err := c.Boot(t.Context())
	if !errors.Is(err, ErrCycle) || !strings.Contains(err.Error(), "*ioc.owner -> *ioc.other -> *ioc.owner") {
		t.Fatalf("err = %v, want ErrCycle with the full path", err)
	}
}

func TestResolveErrors(t *testing.T) {
	sentinel := errors.New("boom")

	t.Run("缺少依赖", func(t *testing.T) {
		c := newTest().Provide(func(*cfg) *db { return &db{} })
		_, err := c.Get[*db]()
		if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "*ioc.cfg") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("构造函数参数循环", func(t *testing.T) {
		c := newTest().
			Provide(func(*db) *cfg { return &cfg{} }).
			Provide(func(*cfg) *db { return &db{} })
		err := c.Boot(t.Context())
		if !errors.Is(err, ErrCycle) || !strings.Contains(err.Error(), "*ioc.cfg -> *ioc.db -> *ioc.cfg") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("Setup 里 Get 绕回自己", func(t *testing.T) {
		c := newTest()
		c.Instance(&comp{rec: &recorder{}, setup: func(ctx context.Context) error {
			_, err := FromContext(ctx).Get[*comp]()
			return err
		}})
		if err := c.Boot(t.Context()); !errors.Is(err, ErrCycle) {
			t.Fatalf("err = %v, want ErrCycle", err)
		}
	})

	t.Run("构造函数返回 error", func(t *testing.T) {
		c := newTest().Provide(func() (*cfg, error) { return nil, sentinel })
		if _, err := c.Get[*cfg](); !errors.Is(err, sentinel) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("构造函数返回 nil", func(t *testing.T) {
		c := newTest().Provide(func() *cfg { return nil })
		if _, err := c.Get[*cfg](); err == nil || !strings.Contains(err.Error(), "returned nil") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("Setup 失败后可重试且不进入 Shutdown", func(t *testing.T) {
		rec := &recorder{}
		fail := true
		c := newTest()
		c.Instance(&comp{name: "a", rec: rec, setup: func(context.Context) error {
			if fail {
				return sentinel
			}
			return nil
		}})
		if err := c.Boot(t.Context()); !errors.Is(err, sentinel) {
			t.Fatalf("err = %v", err)
		}
		if err := c.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		wantEvents(t, rec, "setup a")

		fail = false
		if err := c.Boot(t.Context()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ctx 里没有容器", func(t *testing.T) {
		c := FromContext(t.Context())
		if c != nil {
			t.Fatal("expected nil container")
		}
		if _, err := c.Get[int](); !errors.Is(err, ErrNoContainer) {
			t.Fatalf("err = %v, want ErrNoContainer", err)
		}
	})
}

func TestRegistrationPanics(t *testing.T) {
	mustPanic(t, "duplicate binding int#n", func() { newTest().Instance(1, "n").Instance(2, "n") })
	mustPanic(t, "duplicate binding *ioc.cfg", func() {
		newTest().Instance(&cfg{}).Provide(func() *cfg { return &cfg{} })
	})
	mustPanic(t, "non-variadic func", func() { newTest().Provide(42) })
	mustPanic(t, "non-variadic func", func() { newTest().Provide(nil) })
	mustPanic(t, "non-variadic func", func() { newTest().Provide(func(...int) *cfg { return nil }) })
	mustPanic(t, "must return T or (T, error)", func() { newTest().Provide(func() {}) })
	mustPanic(t, "must return T or (T, error)", func() { newTest().Provide(func() (*cfg, int) { return nil, 0 }) })
	mustPanic(t, "Instance(nil) for *ioc.cfg", func() { newTest().Instance[*cfg](nil) })
	mustPanic(t, "Instance(nil) for ioc.greeter", func() { newTest().Instance[greeter](nil) })
	mustPanic(t, "Instance(nil) for ioc.greeter", func() { newTest().Instance[greeter]((*english)(nil)) })
	mustPanic(t, "Provide called inside", func() {
		c := newTest()
		c.Instance(&comp{rec: &recorder{}, setup: func(ctx context.Context) error {
			FromContext(ctx).Provide(func() int { return 1 })
			return nil
		}})
		_ = c.Boot(t.Context())
	})
}

// 组件在 Setup 里绑定的实例归组件所有:能取到,但容器不代管它的生命周期。
func TestInstanceInsideSetupIsOwnedByComponent(t *testing.T) {
	rec := &recorder{}
	owned := &server{comp: newComp(rec, "owned"), serve: blockUntilDone}
	c := newTest()
	c.Instance(&comp{name: "owner", rec: rec, setup: func(ctx context.Context) error {
		FromContext(ctx).Instance(owned)
		return nil
	}})

	if err := within(t, func() error { return c.Run(t.Context()) }); err != nil {
		t.Fatal(err)
	}
	if c.MustGet[*server]() != owned {
		t.Fatal("owned instance is not resolvable")
	}
	wantEvents(t, rec, "setup owner", "shutdown owner")
}

func TestContextCarriesContainerAndCurrentLogger(t *testing.T) {
	old := log.Logger
	t.Cleanup(func() { log.Logger = old })

	var buf bytes.Buffer
	rec := &recorder{}
	c := newTest()
	// 第一个组件替换全局 logger,第二个组件的 ctx 应立即带上新 logger。
	c.Instance(&comp{name: "logger", rec: rec, setup: func(context.Context) error {
		log.Logger = zerolog.New(&buf)
		return nil
	}}, "logger")
	c.Instance(&comp{name: "user", rec: rec, setup: func(ctx context.Context) error {
		if FromContext(ctx) != c {
			t.Error("ctx does not carry the container")
		}
		log.Ctx(ctx).Info().Msg("from user")
		return nil
	}}, "user")

	if err := c.Boot(t.Context()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{`"message":"from user"`, `"component":"*ioc.comp#user"`, `"message":"component setup"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log output missing %s:\n%s", want, out)
		}
	}
}

// ---- Shutdown ----

func TestShutdownJoinsErrorsAndIsIdempotent(t *testing.T) {
	errA, errB := errors.New("a failed"), errors.New("b failed")
	rec := &recorder{}
	c := newTest()
	c.Instance(&comp{name: "a", rec: rec, shutdown: func(context.Context) error { return errA }}, "a")
	c.Instance(&comp{name: "b", rec: rec, shutdown: func(context.Context) error { return errB }}, "b")
	if err := c.Boot(t.Context()); err != nil {
		t.Fatal(err)
	}

	err := c.Shutdown(t.Context())
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("err = %v, want both errors joined", err)
	}
	if err := c.Shutdown(t.Context()); err != nil {
		t.Fatalf("second shutdown err = %v", err)
	}
	wantEvents(t, rec, "setup a", "setup b", "shutdown b", "shutdown a")
}

// ---- Run ----

func TestRunWithoutServerReturnsImmediately(t *testing.T) {
	rec := &recorder{}
	c := newTest()
	c.Instance(newComp(rec, "a"))
	if err := within(t, func() error { return c.Run(t.Context()) }); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, rec, "setup a", "shutdown a")
}

func TestRunStartsServersConcurrentlyAndStopsOnCancel(t *testing.T) {
	rec := &recorder{}
	var started sync.WaitGroup
	started.Add(2)
	serve := func(ctx context.Context) error {
		started.Done()
		<-ctx.Done()
		return ctx.Err() // 取消之后才返回的错误不算失败
	}
	c := newTest()
	c.Instance(&server{newComp(rec, "a"), serve}, "a")
	c.Instance(&server{newComp(rec, "b"), serve}, "b")

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		started.Wait() // 两个 Serve 都跑起来才取消,证明是并发启动
		cancel()
	}()
	if err := within(t, func() error { return c.Run(ctx) }); err != nil {
		t.Fatalf("err = %v, want nil on cancel", err)
	}
	if got := rec.list(); !slices.Equal(got[len(got)-2:], []string{"shutdown b", "shutdown a"}) {
		t.Fatalf("events = %v", got)
	}
}

func TestRunReturnsWhenAllServersExit(t *testing.T) {
	rec := &recorder{}
	release := make(chan struct{})
	c := newTest()
	c.Instance(&server{newComp(rec, "quick"), func(context.Context) error { return nil }}, "quick")
	c.Instance(&server{newComp(rec, "slow"), func(context.Context) error {
		<-release
		return nil
	}}, "slow")

	done := make(chan error, 1)
	go func() { done <- c.Run(t.Context()) }()
	select {
	case err := <-done:
		t.Fatalf("Run returned early with %v while a server is still running", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := within(t, func() error { return <-done }); err != nil {
		t.Fatal(err)
	}
}

func TestRunServeErrorCancelsOthersAndIsReturned(t *testing.T) {
	sentinel := errors.New("serve failed")
	rec := &recorder{}
	c := newTest()
	c.Instance(&server{newComp(rec, "bad"), func(context.Context) error { return sentinel }}, "bad")
	c.Instance(&server{newComp(rec, "good"), blockUntilDone}, "good")

	err := within(t, func() error { return c.Run(t.Context()) })
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "ioc: serve *ioc.server#bad") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunBootFailureShutsDownPartial(t *testing.T) {
	sentinel := errors.New("setup failed")
	rec := &recorder{}
	c := newTest()
	c.Instance(newComp(rec, "a"), "a")
	c.Instance(&comp{name: "b", rec: rec, setup: func(context.Context) error { return sentinel }}, "b")
	c.Instance(&server{newComp(rec, "c"), blockUntilDone}, "c")

	err := within(t, func() error { return c.Run(t.Context()) })
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "ioc: setup *ioc.comp#b") {
		t.Fatalf("err = %v", err)
	}
	wantEvents(t, rec, "setup a", "setup b", "shutdown a")
}

func TestRunJoinsServeAndShutdownErrors(t *testing.T) {
	serveErr, shutdownErr := errors.New("serve failed"), errors.New("shutdown failed")
	rec := &recorder{}
	c := newTest()
	c.Instance(&server{
		comp:  &comp{name: "a", rec: rec, shutdown: func(context.Context) error { return shutdownErr }},
		serve: func(context.Context) error { return serveErr },
	})
	err := within(t, func() error { return c.Run(t.Context()) })
	if !errors.Is(err, serveErr) || !errors.Is(err, shutdownErr) {
		t.Fatalf("err = %v, want both errors joined", err)
	}
}

func TestRunShutdownTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c := newTest(WithShutdownTimeout(20 * time.Millisecond))
	c.Instance(&comp{rec: &recorder{}, shutdown: func(context.Context) error {
		<-release
		return nil
	}})
	err := within(t, func() error { return c.Run(t.Context()) })
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("err = %v, want ErrShutdownTimeout", err)
	}
}

func TestRunWithoutShutdownTimeoutWaits(t *testing.T) {
	c := newTest(WithShutdownTimeout(0))
	c.Instance(&comp{rec: &recorder{}, shutdown: func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); ok {
			t.Error("shutdown ctx should have no deadline")
		}
		time.Sleep(20 * time.Millisecond)
		return nil
	}})
	if err := within(t, func() error { return c.Run(t.Context()) }); err != nil {
		t.Fatal(err)
	}
}

func TestRunTreatsSignalAsNormalExit(t *testing.T) {
	self, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Skip(err)
	}
	rec := &recorder{}
	c := New() // 默认监听 os.Interrupt
	c.Instance(&server{newComp(rec, "a"), func(ctx context.Context) error {
		// Serve 已经在跑,说明信号处理已就位,此时给自己发信号是安全的。
		if err := self.Signal(os.Interrupt); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}})
	if err := within(t, func() error { return c.Run(t.Context()) }); err != nil {
		t.Fatalf("err = %v, want nil on signal", err)
	}
	wantEvents(t, rec, "setup a", "serve a", "shutdown a")
}

// ---- Exec ----

func TestExecRunsFnWithoutStartingServers(t *testing.T) {
	rec := &recorder{}
	c := newTest()
	c.Instance(&server{newComp(rec, "a"), blockUntilDone})

	err := within(t, func() error {
		return c.Exec(t.Context(), func(ctx context.Context) error {
			if FromContext(ctx).MustGet[*server]() == nil {
				t.Error("fn ctx does not carry the container")
			}
			rec.add("fn")
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	wantEvents(t, rec, "setup a", "fn", "shutdown a")
}

func TestExecJoinsFnAndShutdownErrors(t *testing.T) {
	fnErr, shutdownErr := errors.New("fn failed"), errors.New("shutdown failed")
	c := newTest()
	c.Instance(&comp{rec: &recorder{}, shutdown: func(context.Context) error { return shutdownErr }})

	err := within(t, func() error {
		return c.Exec(t.Context(), func(context.Context) error { return fnErr })
	})
	if !errors.Is(err, fnErr) || !errors.Is(err, shutdownErr) {
		t.Fatalf("err = %v, want both errors joined", err)
	}
}

func TestExecSkipsFnWhenBootFails(t *testing.T) {
	sentinel := errors.New("setup failed")
	c := newTest()
	c.Instance(&comp{rec: &recorder{}, setup: func(context.Context) error { return sentinel }})

	err := within(t, func() error {
		return c.Exec(t.Context(), func(context.Context) error {
			t.Error("fn must not run after a failed Boot")
			return nil
		})
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v", err)
	}
}
