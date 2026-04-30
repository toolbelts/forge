package lock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
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

func TestWithTtl_NonPositiveKeepsDefault(t *testing.T) {
	_, client := newTestRedis(t)
	m, err := NewManager(client, WithTtl(0), WithTtl(-time.Second))
	if err != nil {
		t.Fatalf("expected default ttl to apply, got %v", err)
	}
	if m.opt.ttl != 30*time.Second {
		t.Fatalf("expected default 30s ttl, got %v", m.opt.ttl)
	}
}

func TestWithPrefix_EmptyKeepsDefault(t *testing.T) {
	_, client := newTestRedis(t)
	m, err := NewManager(client, WithPrefix(""))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if m.opt.prefix != "lock" {
		t.Fatalf("expected default prefix lock, got %q", m.opt.prefix)
	}
}

func TestLocker_LockUnlock(t *testing.T) {
	_, m := newTestManager(t)
	locker := m.NewLocker("normal")
	if err := locker.TryLock(context.Background()); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := locker.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLocker_EmptyKey(t *testing.T) {
	_, m := newTestManager(t)
	locker := m.NewLocker("")
	if err := locker.TryLock(context.Background()); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
}

func TestLocker_LockBusy(t *testing.T) {
	_, m := newTestManager(t)
	a := m.NewLocker("busy")
	b := m.NewLocker("busy")
	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a lock: %v", err)
	}
	if err := b.TryLock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("b expected ErrLocked, got %v", err)
	}
}

func TestLocker_UnlockNotHeld(t *testing.T) {
	_, m := newTestManager(t)
	locker := m.NewLocker("orphan")
	if err := locker.Unlock(context.Background()); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("expected ErrNotHeld, got %v", err)
	}
}

func TestLocker_TtlExpire(t *testing.T) {
	mr, m := newTestManager(t, WithTtl(100*time.Millisecond))
	locker := m.NewLocker("expire")
	if err := locker.TryLock(context.Background()); err != nil {
		t.Fatalf("lock: %v", err)
	}
	mr.FastForward(200 * time.Millisecond)
	if err := locker.Unlock(context.Background()); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("expected ErrNotHeld after expire, got %v", err)
	}
}

// TestLocker_NoCrossDelete 验证持锁安全:A 持锁,锁过期后 B 抢到,
// A 再调用 Unlock 不会误删 B 的锁。
func TestLocker_NoCrossDelete(t *testing.T) {
	mr, m := newTestManager(t, WithTtl(100*time.Millisecond))
	a := m.NewLocker("safety")
	b := m.NewLocker("safety")

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a lock: %v", err)
	}
	mr.FastForward(200 * time.Millisecond)
	if err := b.TryLock(context.Background()); err != nil {
		t.Fatalf("b lock after a expired: %v", err)
	}
	if err := a.Unlock(context.Background()); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("a unlock should report ErrNotHeld, got %v", err)
	}
	val, err := mr.Get(b.Key())
	if err != nil {
		t.Fatalf("b key missing after a unlock: %v", err)
	}
	if val == "" {
		t.Fatal("b token should still be present, but value is empty")
	}
}

func TestLocker_Reuse(t *testing.T) {
	_, m := newTestManager(t)
	locker := m.NewLocker("reuse")
	if err := locker.TryLock(context.Background()); err != nil {
		t.Fatalf("lock 1: %v", err)
	}
	if err := locker.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock 1: %v", err)
	}
	if err := locker.TryLock(context.Background()); err != nil {
		t.Fatalf("lock 2: %v", err)
	}
	if err := locker.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock 2: %v", err)
	}
}

// TestLocker_LockRetrySucceed 验证 Lock 在重试期间另一持有者释放锁后能成功获取。
func TestLocker_LockRetrySucceed(t *testing.T) {
	_, m := newTestManager(t, WithRetry(5), WithRetryInterval(50*time.Millisecond))
	a := m.NewLocker("retry_succeed")
	b := m.NewLocker("retry_succeed")

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a try lock: %v", err)
	}
	go func() {
		time.Sleep(120 * time.Millisecond)
		_ = a.Unlock(context.Background())
	}()

	start := time.Now()
	if err := b.Lock(context.Background()); err != nil {
		t.Fatalf("b lock: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("expected at least 100ms wait before acquire, got %v", elapsed)
	}
}

// TestLocker_LockRetryExhaust 验证 Lock 重试用尽仍被占时返回 ErrLocked。
func TestLocker_LockRetryExhaust(t *testing.T) {
	_, m := newTestManager(t, WithRetry(2), WithRetryInterval(20*time.Millisecond))
	a := m.NewLocker("exhaust")
	b := m.NewLocker("exhaust")

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a try lock: %v", err)
	}
	if err := b.Lock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}

// TestLocker_LockCtxCancel 验证 Lock 在重试等待期间响应 ctx 取消。
func TestLocker_LockCtxCancel(t *testing.T) {
	_, m := newTestManager(t, WithRetry(100), WithRetryInterval(50*time.Millisecond))
	a := m.NewLocker("ctx_cancel")
	b := m.NewLocker("ctx_cancel")

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a try lock: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := b.Lock(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// TestLocker_LockNoRetry 验证 retry=0 时 Lock 等同 TryLock,被占立即返回 ErrLocked。
func TestLocker_LockNoRetry(t *testing.T) {
	_, m := newTestManager(t, WithRetry(0))
	a := m.NewLocker("no_retry")
	b := m.NewLocker("no_retry")

	if err := a.TryLock(context.Background()); err != nil {
		t.Fatalf("a try lock: %v", err)
	}
	start := time.Now()
	if err := b.Lock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Millisecond {
		t.Fatalf("expected immediate return, took %v", elapsed)
	}
}

func TestManager_WithPrefix(t *testing.T) {
	mr, m := newTestManager(t, WithPrefix("myapp"))
	locker := m.NewLocker("k")
	if !strings.HasPrefix(locker.Key(), "myapp:") {
		t.Fatalf("expected key prefixed with myapp:, got %q", locker.Key())
	}
	if err := locker.TryLock(context.Background()); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if _, err := mr.Get("myapp:k"); err != nil {
		t.Fatalf("expected redis key myapp:k to exist: %v", err)
	}
}
