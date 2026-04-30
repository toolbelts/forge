package registry

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:         mr.Addr(),
		MaxRetries:   0,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func newTestManager(t *testing.T, opts ...Option) (*miniredis.Miniredis, *Manager) {
	t.Helper()
	mr, client := newTestRedis(t)
	m, err := NewManager(client, opts...)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mr, m
}

func TestNewManager_NilRedis(t *testing.T) {
	if _, err := NewManager(nil); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("expected ErrNilRedisClient, got %v", err)
	}
}

func TestNewManager_HeartbeatGeTtl(t *testing.T) {
	_, client := newTestRedis(t)
	if _, err := NewManager(client, WithTtl(time.Second), WithHeartbeat(2*time.Second)); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("expected ErrInvalidOption when heartbeat>=ttl, got %v", err)
	}
}

func TestOptions_NonPositiveKeepsDefault(t *testing.T) {
	_, client := newTestRedis(t)
	m, err := NewManager(client,
		WithPrefix(""),
		WithTtl(0),
		WithHeartbeat(-time.Second),
		WithResolveInterval(0),
	)
	if err != nil {
		t.Fatalf("expected defaults to apply, got %v", err)
	}
	if m.opt.prefix != "registry" || m.opt.ttl != 15*time.Second || m.opt.heartbeat != 5*time.Second || m.opt.resolveInterval != 5*time.Second {
		t.Fatalf("expected defaults, got %+v", m.opt)
	}
}

func TestRegister_Validation(t *testing.T) {
	_, m := newTestManager(t)
	cases := []struct {
		name string
		inst Instance
		want error
	}{
		{"empty service", Instance{Id: "i", Addr: "a"}, ErrEmptyService},
		{"empty id", Instance{Service: "s", Addr: "a"}, ErrEmptyInstanceId},
		{"empty addr", Instance{Service: "s", Id: "i"}, ErrEmptyAddr},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := m.Register(context.Background(), c.inst); !errors.Is(err, c.want) {
				t.Fatalf("expected %v, got %v", c.want, err)
			}
		})
	}
}

func TestRegister_ResolveDeregister(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second))
	inst := Instance{Service: "svc", Id: "i1", Addr: "10.0.0.1:9090"}
	reg, err := m.Register(context.Background(), inst)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	got, err := m.Resolve(context.Background(), "svc")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 || got[0].Addr != "10.0.0.1:9090" || got[0].Id != "i1" {
		t.Fatalf("unexpected resolve result: %+v", got)
	}

	if err := reg.Deregister(context.Background()); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	got, err = m.Resolve(context.Background(), "svc")
	if err != nil {
		t.Fatalf("resolve after deregister: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty after deregister, got %+v", got)
	}
}

func TestRegister_Conflict(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second))
	a, err := m.Register(context.Background(), Instance{Service: "svc", Id: "dup", Addr: "1.1.1.1:9090"})
	if err != nil {
		t.Fatalf("register a: %v", err)
	}
	defer func() { _ = a.Deregister(context.Background()) }()

	if _, err := m.Register(context.Background(), Instance{Service: "svc", Id: "dup", Addr: "2.2.2.2:9090"}); !errors.Is(err, ErrInstanceConflict) {
		t.Fatalf("expected ErrInstanceConflict, got %v", err)
	}
}

func TestRegister_SameAddrAllowed(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second))
	a, err := m.Register(context.Background(), Instance{Service: "svc", Id: "same", Addr: "1.1.1.1:9090"})
	if err != nil {
		t.Fatalf("register a: %v", err)
	}
	defer func() { _ = a.Deregister(context.Background()) }()

	b, err := m.Register(context.Background(), Instance{Service: "svc", Id: "same", Addr: "1.1.1.1:9090"})
	if err != nil {
		t.Fatalf("register b same addr: %v", err)
	}
	defer func() { _ = b.Deregister(context.Background()) }()
}

func TestResolve_EmptyService(t *testing.T) {
	_, m := newTestManager(t)
	if _, err := m.Resolve(context.Background(), ""); !errors.Is(err, ErrEmptyService) {
		t.Fatalf("expected ErrEmptyService, got %v", err)
	}
}

func TestResolve_TtlExpire(t *testing.T) {
	mr, m := newTestManager(t, WithTtl(200*time.Millisecond), WithHeartbeat(50*time.Millisecond))
	// 直接写入而非走 Register,避免心跳 goroutine 续约影响 TTL 验证。
	key := m.instanceKey("svc", "ghost")
	if err := m.rdb.Set(context.Background(), key, `{"id":"ghost","service":"svc","addr":"1.1.1.1:9090"}`, m.opt.ttl).Err(); err != nil {
		t.Fatalf("seed key: %v", err)
	}

	got, err := m.Resolve(context.Background(), "svc")
	if err != nil || len(got) != 1 {
		t.Fatalf("pre-expire resolve: %+v %v", got, err)
	}

	mr.FastForward(300 * time.Millisecond)

	got, err = m.Resolve(context.Background(), "svc")
	if err != nil {
		t.Fatalf("post-expire resolve: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty after ttl expire, got %+v", got)
	}
}

func TestResolve_MultipleInstancesSorted(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second))
	defer cleanup(m, "svc")

	for _, id := range []string{"c", "a", "b"} {
		if _, err := m.Register(context.Background(), Instance{Service: "svc", Id: id, Addr: "1.1.1.1:9090"}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	got, err := m.Resolve(context.Background(), "svc")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 3 || got[0].Id != "a" || got[1].Id != "b" || got[2].Id != "c" {
		t.Fatalf("expected sorted by id [a b c], got %+v", got)
	}
}

func TestWatch_PushesChanges(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second), WithResolveInterval(30*time.Millisecond))

	ch, err := m.Watch(t.Context(), "svc")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	first := mustRecv(t, ch, time.Second)
	if len(first) != 0 {
		t.Fatalf("expected empty initial snapshot, got %+v", first)
	}

	reg, err := m.Register(context.Background(), Instance{Service: "svc", Id: "i1", Addr: "1.1.1.1:9090"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() { _ = reg.Deregister(context.Background()) }()

	second := mustRecv(t, ch, time.Second)
	if len(second) != 1 || second[0].Id != "i1" {
		t.Fatalf("expected 1 instance i1, got %+v", second)
	}
}

func TestResolverBuilder_SchemeAndBuild(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second), WithResolveInterval(30*time.Millisecond))
	b := m.ResolverBuilder()
	if b.Scheme() != Scheme {
		t.Fatalf("expected scheme %q, got %q", Scheme, b.Scheme())
	}

	target := resolver.Target{URL: url.URL{Scheme: Scheme, Path: "/"}}
	if _, err := b.Build(target, &fakeClientConn{}, resolver.BuildOptions{}); !errors.Is(err, ErrEmptyService) {
		t.Fatalf("expected ErrEmptyService for empty path, got %v", err)
	}
}

func TestResolverBuilder_StatePush(t *testing.T) {
	_, m := newTestManager(t, WithHeartbeat(2*time.Second), WithTtl(10*time.Second), WithResolveInterval(30*time.Millisecond))
	defer cleanup(m, "svc")

	cc := newFakeClientConn()
	target := resolver.Target{URL: url.URL{Scheme: Scheme, Path: "/svc"}}
	r, err := m.ResolverBuilder().Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer r.Close()

	reg, err := m.Register(context.Background(), Instance{Service: "svc", Id: "i1", Addr: "1.1.1.1:9090"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() { _ = reg.Deregister(context.Background()) }()

	state := cc.waitState(t, func(s resolver.State) bool { return len(s.Addresses) == 1 }, 2*time.Second)
	if state.Addresses[0].Addr != "1.1.1.1:9090" {
		t.Fatalf("unexpected addr %q", state.Addresses[0].Addr)
	}
}

func cleanup(m *Manager, service string) {
	keys, _ := m.scanKeys(context.Background(), m.servicePattern(service))
	if len(keys) > 0 {
		_ = m.rdb.Del(context.Background(), keys...).Err()
	}
}

func mustRecv(t *testing.T, ch <-chan []Instance, timeout time.Duration) []Instance {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("recv timeout after %v", timeout)
		return nil
	}
}

// fakeClientConn 实现 resolver.ClientConn,记录全部 UpdateState 调用供测试断言。
type fakeClientConn struct {
	mu     sync.Mutex
	states []resolver.State
	sigCh  chan struct{}
}

func newFakeClientConn() *fakeClientConn {
	return &fakeClientConn{sigCh: make(chan struct{}, 16)}
}

func (f *fakeClientConn) UpdateState(s resolver.State) error {
	f.mu.Lock()
	f.states = append(f.states, s)
	f.mu.Unlock()
	select {
	case f.sigCh <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeClientConn) ReportError(error)                {}
func (f *fakeClientConn) NewAddress([]resolver.Address)    {}
func (f *fakeClientConn) NewServiceConfig(string)          {}
func (f *fakeClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return nil
}

// waitState 阻塞直到出现满足 predicate 的状态或超时,用于断言 resolver 推送的内容。
func (f *fakeClientConn) waitState(t *testing.T, predicate func(resolver.State) bool, timeout time.Duration) resolver.State {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		f.mu.Lock()
		for _, s := range f.states {
			if predicate(s) {
				f.mu.Unlock()
				return s
			}
		}
		f.mu.Unlock()
		select {
		case <-f.sigCh:
		case <-deadline.C:
			t.Fatalf("waitState timeout after %v", timeout)
			return resolver.State{}
		}
	}
}
