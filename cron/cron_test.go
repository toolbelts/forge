package cron

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 备注:robfig/cron 的 ConstantDelaySchedule(`@every`) 把 <1s 的间隔向上取整到 1s,
// 测试用例只能用 1s 粒度,所以总耗时偏长。

// TestCron_AddJobAndExecute 验证 @every 1s 表达式注册并多次触发。
func TestCron_AddJobAndExecute(t *testing.T) {
	t.Parallel()

	c := New(time.UTC)
	var count atomic.Int32
	if _, err := c.AddJob("ping", "@every 1s", func(ctx context.Context) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("AddJob: %v", err)
	}

	c.Start(context.Background())
	defer waitStop(t, c, 2*time.Second)

	if !waitFor(func() bool { return count.Load() >= 2 }, 4*time.Second) {
		t.Fatalf("expected >=2 executions, got %d", count.Load())
	}
}

// TestCron_SkipIfStillRunning 上一次没跑完时本次触发应被跳过。
func TestCron_SkipIfStillRunning(t *testing.T) {
	t.Parallel()

	c := New(time.UTC)
	var (
		entered  atomic.Int32
		finished atomic.Int32
	)
	hold := make(chan struct{})
	if _, err := c.AddJob("slow", "@every 1s", func(ctx context.Context) error {
		entered.Add(1)
		<-hold
		finished.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("AddJob: %v", err)
	}

	c.Start(context.Background())

	// 等首次进入,再观察 ~2.5s。期间还会有 ~2 次触发,但都应被 SkipIfStillRunning 丢弃。
	if !waitFor(func() bool { return entered.Load() >= 1 }, 3*time.Second) {
		close(hold)
		waitStop(t, c, 2*time.Second)
		t.Fatalf("first execution did not start in time")
	}
	time.Sleep(2500 * time.Millisecond)
	if got := entered.Load(); got != 1 {
		close(hold)
		waitStop(t, c, 2*time.Second)
		t.Fatalf("expected 1 entered execution while first holds, got %d", got)
	}

	close(hold)
	waitStop(t, c, 2*time.Second)
	if finished.Load() < 1 {
		t.Fatalf("first execution did not finish")
	}
}

// TestCron_PanicDoesNotKillScheduler panic 任务被 Recover 中间件兜住,后续触发仍正常执行。
func TestCron_PanicDoesNotKillScheduler(t *testing.T) {
	t.Parallel()

	c := New(time.UTC)
	var panics, oks atomic.Int32
	if _, err := c.AddJob("boom", "@every 1s", func(ctx context.Context) error {
		panics.Add(1)
		panic("boom")
	}); err != nil {
		t.Fatalf("AddJob boom: %v", err)
	}
	if _, err := c.AddJob("ok", "@every 1s", func(ctx context.Context) error {
		oks.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("AddJob ok: %v", err)
	}

	c.Start(context.Background())
	defer waitStop(t, c, 2*time.Second)

	if !waitFor(func() bool { return panics.Load() >= 2 && oks.Load() >= 2 }, 4*time.Second) {
		t.Fatalf("expected both jobs to fire >=2 times, got panics=%d oks=%d", panics.Load(), oks.Load())
	}
}

// TestCron_StopCancelsJobCtx Stop 应取消任务函数收到的 ctx,长任务能感知到。
func TestCron_StopCancelsJobCtx(t *testing.T) {
	t.Parallel()

	c := New(time.UTC)
	var (
		started = make(chan struct{}, 1)
		seenErr error
		mu      sync.Mutex
	)
	if _, err := c.AddJob("watcher", "@every 1s", func(ctx context.Context) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		mu.Lock()
		seenErr = ctx.Err()
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("AddJob: %v", err)
	}

	c.Start(context.Background())
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		waitStop(t, c, 2*time.Second)
		t.Fatal("job did not start in time")
	}

	waitStop(t, c, 2*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(seenErr, context.Canceled) {
		t.Fatalf("expected ctx.Err() == Canceled, got %v", seenErr)
	}
}

// TestCron_BadSpecReturnsError 非法表达式应返回错误。
func TestCron_BadSpecReturnsError(t *testing.T) {
	t.Parallel()

	c := New(nil) // nil → time.Local
	if _, err := c.AddJob("bad", "not a cron", func(context.Context) error { return nil }); err == nil {
		t.Fatal("expected error for invalid spec, got nil")
	}
}

// TestCron_NilFuncRejected 防御性检查。
func TestCron_NilFuncRejected(t *testing.T) {
	t.Parallel()

	c := New(time.UTC)
	if _, err := c.AddJob("nil", "@every 1s", nil); err == nil {
		t.Fatal("expected error for nil fn, got nil")
	}
}

// waitFor 在 timeout 内轮询条件,直到为真或超时。
func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// waitStop 调用 Cron.Stop 并在 timeout 内等待 ctx Done。
func waitStop(t *testing.T, c *Cron, timeout time.Duration) {
	t.Helper()
	stopCtx := c.Stop()
	select {
	case <-stopCtx.Done():
	case <-time.After(timeout):
		t.Logf("warning: cron Stop did not finish within %s", timeout)
	}
}
