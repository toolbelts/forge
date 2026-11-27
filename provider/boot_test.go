package provider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// bootProbe 排在所有 Provider 之后，扮演业务方：Setup 里注册路由并检查前面注入的产物，
// Serve 里真实访问一次 http server，然后取消 ctx 触发收尾。
type bootProbe struct {
	t      *testing.T
	http   *HttpProvider
	cancel context.CancelFunc
}

func (p *bootProbe) Setup(ctx context.Context) error {
	if n := len(MustGetInterceptorChain(ctx).unary); n != 2 {
		p.t.Errorf("expected 2 unary interceptors (recovery, error), got %d", n)
	}
	if MustGetGrpcServer(ctx) == nil || MustGetGrpcListener(ctx) == nil {
		p.t.Error("grpc server / listener not bound")
	}
	MustGetHttpMux(ctx).HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})
	return nil
}

func (p *bootProbe) Serve(ctx context.Context) error {
	defer p.cancel()

	base := "http://" + p.http.listener.Addr().String()
	for path, want := range map[string]string{"/ping": "pong", "/debug/pprof/cmdline": ""} {
		resp, err := http.Get(base + path)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			p.t.Errorf("GET %s status = %d", path, resp.StatusCode)
		}
		if want != "" && string(body) != want {
			p.t.Errorf("GET %s body = %q, want %q", path, body, want)
		}
	}
	return nil
}

// TestProvidersRunEndToEnd 用真实容器按推荐顺序跑一遍单阶段生命周期：
// 拦截器先于 GrpcProvider 登记、pprof 挂到 HttpProvider 的 mux、业务方在最后注册路由，
// 然后并发 Serve，ctx 取消后逆序 Shutdown。
func TestProvidersRunEndToEnd(t *testing.T) {
	v := viper.New()
	for key, value := range map[string]any{
		"grpc.enabled":  true,
		"grpc.addr":     "127.0.0.1:0",
		"http.enabled":  true,
		"http.addr":     "127.0.0.1:0",
		"pprof.enabled": true,
	} {
		v.Set(key, value)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	httpProvider := &HttpProvider{}

	c := ioc.New(ioc.WithSignals(), ioc.WithShutdownTimeout(5*time.Second))
	c.Instance(v).Instance(AppName("forge.test")).Instance(&BuildInfo{Version: "v1"})
	c.Instance(&NotifyProvider{}).
		Instance(&RecoveryProvider{}).
		Instance(&ErrorProvider{}).
		Instance(&GrpcProvider{}).
		Instance(httpProvider).
		Instance(&PprofProvider{}).
		Instance(&bootProbe{t: t, http: httpProvider, cancel: cancel})

	if err := c.Run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	// 收尾后 http server 已关闭，再 Serve 应立即得到 ErrServerClosed。
	if err := httpProvider.server.Serve(httpProvider.listener); !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("http server not shut down, err = %v", err)
	}
}

// lifecycleOrder 从生命周期日志里按出现顺序抠出指定阶段的 Provider 名(去掉 "*provider." 前缀)。
// 注意 setup 日志打在 Setup 开始时:被依赖方是在依赖方 Setup 中途被拉起的,所以它不是完成顺序。
func lifecycleOrder(logs, phase string) []string {
	re := regexp.MustCompile(`"component":"\*provider\.(\w+)","message":"component ` + phase + `"`)
	var out []string
	for _, m := range re.FindAllStringSubmatch(logs, -1) {
		out = append(out, m[1])
	}
	return out
}

// TestProvidersBootInAnyOrder 故意把依赖方排在被依赖方之前:访问函数会先拉起产物的所有者,
// 所以 Setup 的完成顺序由依赖关系决定,Shutdown 是它的逆序。
// 仍然靠注册顺序的只有读写拦截器链 / 中间件链的那几个:拦截器在 GrpcProvider 之前,pprof 在 HttpProvider 之后。
func TestProvidersBootInAnyOrder(t *testing.T) {
	redisServer := miniredis.RunT(t)
	dir := t.TempDir()
	config := fmt.Sprintf(`
redis:
  default:
    addr: %s
lock:
  enabled: true
token:
  enabled: true
dbcache:
  users:
    store: redis
grpc:
  enabled: true
  addr: 127.0.0.1:0
http:
  enabled: true
  addr: 127.0.0.1:0
pprof:
  enabled: true
`, redisServer.Addr())
	if err := os.WriteFile(filepath.Join(dir, "forge.yaml"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	old := log.Logger
	log.Logger = zerolog.New(&logs)
	t.Cleanup(func() { log.Logger = old })

	c := ioc.New(ioc.WithSignals(), ioc.WithShutdownTimeout(5*time.Second))
	c.Instance(&LockProvider{})     // 要 RedisProvider 的客户端
	c.Instance(&TokenProvider{})    // 同上,并往拦截器链里加一个
	c.Instance(&RecoveryProvider{}) // 要 NotifyProvider 的 notifier
	c.Instance(&DbcacheProvider{})
	c.Instance(&GrpcProvider{})
	c.Instance(&HttpProvider{})
	c.Instance(&PprofProvider{}) // 读写链的 server 不会被拉起,所以 pprof 仍要排在 HttpProvider 之后
	c.Instance(&RedisProvider{})
	c.Instance(&NotifyProvider{}) // 要 BuildProvider 的 BuildInfo
	c.Instance(&BuildProvider{})
	c.Instance(NewConfigProvider("forge").WithConfigDirs(dir).WithConfigNames("forge").WithEnvFiles())

	if err := c.Boot(t.Context()); err != nil {
		t.Fatalf("boot failed: %v", err)
	}
	ctx := c.WithContext(t.Context())
	if MustGetLockManager(ctx) == nil || MustGetTokenManager(ctx) == nil || MustGetDbcacheStore(ctx, "users") == nil {
		t.Fatal("redis-backed products not bound")
	}
	if n := len(MustGetInterceptorChain(ctx).unary); n != 2 {
		t.Fatalf("expected 2 unary interceptors (token, recovery), got %d", n)
	}

	// 每个 Provider 恰好 Setup 一次(被提前拉起的不会在轮到自己时再跑一遍)。
	setup := lifecycleOrder(logs.String(), "setup")
	slices.Sort(setup)
	if len(setup) != 11 || len(slices.Compact(setup)) != 11 {
		t.Fatalf("expected 11 providers set up exactly once, got %v", setup)
	}

	if err := c.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
	// Shutdown 是 Setup 完成顺序的逆序,所以依赖方先关:用到 redis 客户端的 dbcache 必须早于 RedisProvider。
	shutdown := lifecycleOrder(logs.String(), "shutdown")
	dbcacheAt, redisAt := slices.Index(shutdown, "DbcacheProvider"), slices.Index(shutdown, "RedisProvider")
	if dbcacheAt < 0 || redisAt < 0 || dbcacheAt > redisAt {
		t.Fatalf("DbcacheProvider must shut down before RedisProvider, got %v", shutdown)
	}
}
