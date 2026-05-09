package jobqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 注:miniredis v2.37.0 不实现 BLMPOP,本文件只覆盖 BRPOP 路径与
// Subscribe/Publish/Start/Stop 的生命周期、反射 dispatch 行为。
// BLMPOP 路径通过 internal/provider 层的 smoke 测试覆盖。

func init() {
	// 把 BRPOP 阻塞超时改小,Stop 时 worker 能更快退出。
	popTimeout = 200 * time.Millisecond
}

func newTestQueue(t *testing.T) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	q, err := New(client, "jq:")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q, mr
}

// stopAndWait 调用 Stop 并在 timeout 内等待 done,避免 worker goroutine 泄漏。
func stopAndWait(t *testing.T, q *Queue, timeout time.Duration) {
	t.Helper()
	select {
	case <-q.Stop():
	case <-time.After(timeout):
		t.Fatalf("Stop timeout after %s", timeout)
	}
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// TestQueue_PublishAndConsume 正常路径:Subscribe + Publish + Start,handler 拿到正确参数。
func TestQueue_PublishAndConsume(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	type got struct {
		msg string
		n   int
	}
	ch := make(chan got, 1)
	if err := q.Subscribe("greet", func(ctx context.Context, msg string, n int) error {
		ch <- got{msg, n}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	if err := q.Publish(context.Background(), "greet", "hello", 42); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case g := <-ch:
		if g.msg != "hello" || g.n != 42 {
			t.Fatalf("got %+v, want {hello 42}", g)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked in time")
	}
}

// TestQueue_PublishBeforeStart 先 Publish 再 Start,消息仍能被消费。
func TestQueue_PublishBeforeStart(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	var count atomic.Int32
	if err := q.Subscribe("pre", func(ctx context.Context) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for i := range 3 {
		if err := q.Publish(context.Background(), "pre"); err != nil {
			t.Fatalf("Publish #%d: %v", i, err)
		}
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	if !waitFor(func() bool { return count.Load() == 3 }, 2*time.Second) {
		t.Fatalf("expected 3 invocations, got %d", count.Load())
	}
}

// TestQueue_PointerArg Subscribe 接受指针类型参数,Publish 传入指针后应正确解码。
func TestQueue_PointerArg(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	type Foo struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	got := make(chan *Foo, 1)
	if err := q.Subscribe("foo", func(ctx context.Context, f *Foo) error {
		got <- f
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	if err := q.Publish(context.Background(), "foo", &Foo{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case f := <-got:
		if f == nil || f.Name != "alice" || f.Age != 30 {
			t.Fatalf("got %+v, want {alice 30}", f)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked")
	}
}

// TestQueue_PanicRecovered handler 内 panic 不应让 worker 退出,后续消息仍被处理。
func TestQueue_PanicRecovered(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	var (
		boomCount atomic.Int32
		okCount   atomic.Int32
	)
	if err := q.Subscribe("boom", func(ctx context.Context, kind string) error {
		if kind == "panic" {
			boomCount.Add(1)
			panic("boom")
		}
		okCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	for _, k := range []string{"panic", "ok", "panic", "ok"} {
		if err := q.Publish(context.Background(), "boom", k); err != nil {
			t.Fatalf("Publish %s: %v", k, err)
		}
	}
	if !waitFor(func() bool { return boomCount.Load() == 2 && okCount.Load() == 2 }, 2*time.Second) {
		t.Fatalf("expected boom=2 ok=2, got boom=%d ok=%d", boomCount.Load(), okCount.Load())
	}
}

// TestQueue_HandlerErrorDropped handler 返回 err 仅打日志,不会让 worker 退出。
func TestQueue_HandlerErrorDropped(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	var count atomic.Int32
	if err := q.Subscribe("err", func(ctx context.Context) error {
		count.Add(1)
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	for range 3 {
		if err := q.Publish(context.Background(), "err"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if !waitFor(func() bool { return count.Load() == 3 }, 2*time.Second) {
		t.Fatalf("expected 3 invocations, got %d", count.Load())
	}
}

// TestQueue_DecodeMismatchDropped Publish 参数个数与 fn 不匹配时,消息被丢弃,worker 继续工作。
func TestQueue_DecodeMismatchDropped(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	got := make(chan string, 1)
	if err := q.Subscribe("mismatch", func(ctx context.Context, msg string) error {
		got <- msg
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	// 第一条:多传一个参数 → 解码 mismatch,丢弃
	if err := q.Publish(context.Background(), "mismatch", "hello", 999); err != nil {
		t.Fatalf("Publish bad: %v", err)
	}
	// 第二条:正常 → 应被消费
	if err := q.Publish(context.Background(), "mismatch", "world"); err != nil {
		t.Fatalf("Publish good: %v", err)
	}
	select {
	case s := <-got:
		if s != "world" {
			t.Fatalf("got %q, want world", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("good msg not consumed")
	}
}

// TestQueue_Concurrency WithConcurrency(N) 应起 N 个 worker 并行处理。
func TestQueue_Concurrency(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	const n = 4
	var inflight, max atomic.Int32
	var done atomic.Int32
	var mu sync.Mutex

	if err := q.Subscribe("conc", func(ctx context.Context) error {
		cur := inflight.Add(1)
		mu.Lock()
		if cur > max.Load() {
			max.Store(cur)
		}
		mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		inflight.Add(-1)
		done.Add(1)
		return nil
	}, WithConcurrency(n)); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 3*time.Second)

	for range n {
		if err := q.Publish(context.Background(), "conc"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if !waitFor(func() bool { return done.Load() == n }, 3*time.Second) {
		t.Fatalf("expected %d done, got %d", n, done.Load())
	}
	if got := max.Load(); got < 2 {
		t.Fatalf("expected concurrent inflight >=2, got %d", got)
	}
}

// TestQueue_SubscribeAfterStart Start 之后 Subscribe 应被拒绝。
func TestQueue_SubscribeAfterStart(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)
	if err := q.Subscribe("a", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)

	if err := q.Subscribe("b", func(context.Context) error { return nil }); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("expected ErrAlreadyStarted, got %v", err)
	}
}

// TestQueue_DuplicateTopic 同 topic 重复 Subscribe 应返回 ErrTopicExists。
func TestQueue_DuplicateTopic(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)
	fn := func(context.Context) error { return nil }
	if err := q.Subscribe("dup", fn); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	if err := q.Subscribe("dup", fn); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("expected ErrTopicExists, got %v", err)
	}
}

// TestQueue_StopBeforeStart Stop 在 Start 之前调用应安全(立即 Done)。
func TestQueue_StopBeforeStart(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)
	select {
	case <-q.Stop():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop did not return done chan promptly")
	}
}

// TestQueue_StopCancelsHandlerCtx Stop 应让 handler 收到 ctx.Done。
func TestQueue_StopCancelsHandlerCtx(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)

	started := make(chan struct{}, 1)
	var seenErr error
	var mu sync.Mutex
	if err := q.Subscribe("watch", func(ctx context.Context) error {
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
		t.Fatalf("Subscribe: %v", err)
	}
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := q.Publish(context.Background(), "watch"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		stopAndWait(t, q, 2*time.Second)
		t.Fatal("handler did not start")
	}
	stopAndWait(t, q, 2*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(seenErr, context.Canceled) {
		t.Fatalf("expected ctx.Err == Canceled, got %v", seenErr)
	}
}

// TestQueue_DoubleStart 重复 Start 应返回 ErrAlreadyStarted。
func TestQueue_DoubleStart(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer stopAndWait(t, q, 2*time.Second)
	if err := q.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("expected ErrAlreadyStarted, got %v", err)
	}
}

// TestNewHandler_Validation newHandler 校验各种非法 fn 形态。
func TestNewHandler_Validation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fn   any
	}{
		{"nil", nil},
		{"not a func", 42},
		{"no params", func() error { return nil }},
		{"first not ctx", func(s string) error { return nil }},
		{"no return", func(ctx context.Context) {}},
		{"two returns", func(ctx context.Context) (int, error) { return 0, nil }},
		{"return not error", func(ctx context.Context) int { return 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newHandler("t", tc.fn); !errors.Is(err, ErrInvalidFunc) {
				t.Fatalf("expected ErrInvalidFunc, got %v", err)
			}
		})
	}
}

// TestNew_NilClient New 不接受 nil 客户端。
func TestNew_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := New(nil, ""); !errors.Is(err, ErrNilRedisClient) {
		t.Fatal("expected ErrNilRedisClient")
	}
}

// TestQueue_EmptyTopic Subscribe / Publish 空 topic 应被拒。
func TestQueue_EmptyTopic(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueue(t)
	if err := q.Subscribe("", func(context.Context) error { return nil }); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("expected ErrEmptyTopic, got %v", err)
	}
	if err := q.Publish(context.Background(), ""); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("expected ErrEmptyTopic, got %v", err)
	}
}

// stubMetrics 单测用的 Metrics 实现,记录调用次数与最后一次参数。
type stubMetrics struct {
	mu               sync.Mutex
	publishTotal     map[string]int
	publishDropped   map[string]int64 // 累计 dropped 数
	publishDroppedN  int              // 调用次数
	queueLen         map[string]int64 // 最后一次值
	queueLenCalls    int
}

func newStubMetrics() *stubMetrics {
	return &stubMetrics{
		publishTotal:   make(map[string]int),
		publishDropped: make(map[string]int64),
		queueLen:       make(map[string]int64),
	}
}

func (s *stubMetrics) PublishTotal(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishTotal[topic]++
}

func (s *stubMetrics) PublishDropped(topic string, n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishDropped[topic] += n
	s.publishDroppedN++
}

func (s *stubMetrics) QueueLength(topic string, n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queueLen[topic] = n
	s.queueLenCalls++
}

// newTestQueueWith 用自定义 option 构造测试队列。
func newTestQueueWith(t *testing.T, opts ...QueueOption) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	q, err := New(client, "jq:", opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q, mr
}

// TestQueue_PublishMaxLen_TrimsOldest max_len=4,LPUSH 5 条,触发裁剪后保留 max/2=2 条。
// FIFO 角度:消费顺序应为 m4,m5(m1..m3 被丢)。
func TestQueue_PublishMaxLen_TrimsOldest(t *testing.T) {
	t.Parallel()
	q, mr := newTestQueueWith(t, WithDefaultMaxLen(4))

	ctx := context.Background()
	for _, m := range []string{"m1", "m2", "m3", "m4", "m5"} {
		if err := q.Publish(ctx, "cap", m); err != nil {
			t.Fatalf("Publish %s: %v", m, err)
		}
	}
	got, err := mr.List("jq:cap")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// 前 4 条:list=[m4,m3,m2,m1] (LPUSH 头插)。
	// 第 5 条:n=5>4 → keep=2,trim 后 list=[m5,m4]。BRPOP 尾部 → m4,m5。
	if len(got) != 2 || got[0] != `["m5"]` || got[1] != `["m4"]` {
		t.Fatalf("LIST contents = %v, want [m5, m4]", got)
	}
}

// TestQueue_PublishMaxLen_PerTopicOverride per-topic 配置覆盖 default。
func TestQueue_PublishMaxLen_PerTopicOverride(t *testing.T) {
	t.Parallel()
	q, mr := newTestQueueWith(t,
		WithDefaultMaxLen(4),
		WithTopicMaxLen("big", 10),
	)
	ctx := context.Background()

	// big 用 per-topic max=10,push 5 条不触发裁剪。
	for i := range 5 {
		if err := q.Publish(ctx, "big", i); err != nil {
			t.Fatalf("Publish big: %v", err)
		}
	}
	if !mr.Exists("jq:big") {
		t.Fatal("jq:big missing")
	}
	if got, _ := mr.List("jq:big"); len(got) != 5 {
		t.Fatalf("big LIST len = %d, want 5 (no trim)", len(got))
	}

	// small 用 default max=4,push 5 条触发一次裁剪到 keep=2。
	for i := range 5 {
		if err := q.Publish(ctx, "small", i); err != nil {
			t.Fatalf("Publish small: %v", err)
		}
	}
	if got, _ := mr.List("jq:small"); len(got) != 2 {
		t.Fatalf("small LIST len = %d, want 2 (default max=4 → keep max/2=2)", len(got))
	}
}

// TestQueue_PublishMaxLen_Zero default=0 表示不限,行为完全等同改造前。
func TestQueue_PublishMaxLen_Zero(t *testing.T) {
	t.Parallel()
	q, mr := newTestQueueWith(t)
	ctx := context.Background()
	for i := range 50 {
		if err := q.Publish(ctx, "free", i); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	if got, _ := mr.List("jq:free"); len(got) != 50 {
		t.Fatalf("LIST len = %d, want 50", len(got))
	}
}

// TestQueue_PublishMaxLen_TopicZeroOverridesDefault per-topic 显式 0 应禁用该 topic 的裁剪。
func TestQueue_PublishMaxLen_TopicZeroOverridesDefault(t *testing.T) {
	t.Parallel()
	q, mr := newTestQueueWith(t,
		WithDefaultMaxLen(3),
		WithTopicMaxLen("unbounded", 0),
	)
	ctx := context.Background()
	for i := range 10 {
		if err := q.Publish(ctx, "unbounded", i); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if got, _ := mr.List("jq:unbounded"); len(got) != 10 {
		t.Fatalf("LIST len = %d, want 10", len(got))
	}
}

// TestMetrics_PublishCounters 验证 Metrics 在各路径的回调。
// 用 max=4 (keep=2),前 4 条不触发裁剪,第 5 条触发一次裁剪丢 3 条。
func TestMetrics_PublishCounters(t *testing.T) {
	t.Parallel()
	m := newStubMetrics()
	q, _ := newTestQueueWith(t, WithDefaultMaxLen(4), WithMetrics(m))

	ctx := context.Background()
	// 前 4 条:n 从 1 到 4,均 <= max,不触发 dropped
	for i := range 4 {
		if err := q.Publish(ctx, "t", i); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	m.mu.Lock()
	if m.publishTotal["t"] != 4 {
		t.Fatalf("PublishTotal = %d, want 4", m.publishTotal["t"])
	}
	if m.publishDroppedN != 0 {
		t.Fatalf("PublishDropped should not be called yet, got %d", m.publishDroppedN)
	}
	if m.queueLen["t"] != 4 {
		t.Fatalf("QueueLength = %d, want 4", m.queueLen["t"])
	}
	m.mu.Unlock()

	// 第 5 条:n=5>4 → keep=2,一次裁掉 3 条
	if err := q.Publish(ctx, "t", 4); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishTotal["t"] != 5 {
		t.Fatalf("PublishTotal = %d, want 5", m.publishTotal["t"])
	}
	if m.publishDropped["t"] != 3 {
		t.Fatalf("PublishDropped sum = %d, want 3 (5-keep=2)", m.publishDropped["t"])
	}
	if m.publishDroppedN != 1 {
		t.Fatalf("PublishDropped call count = %d, want 1 (一次裁到 keep)", m.publishDroppedN)
	}
	if m.queueLen["t"] != 2 {
		t.Fatalf("QueueLength = %d, want 2 (max/2)", m.queueLen["t"])
	}
}

// TestMetrics_NoopDefault 默认 Metrics 是 NoopMetrics,不传 WithMetrics 也不应 panic。
func TestMetrics_NoopDefault(t *testing.T) {
	t.Parallel()
	q, _ := newTestQueueWith(t, WithDefaultMaxLen(1))
	ctx := context.Background()
	for i := range 5 {
		if err := q.Publish(ctx, "t", i); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
}

// TestQueue_PublishMaxLen_HalfWatermark 验证稳态下裁剪频率 ~ 1/(max/2),
// 而不是裁到 max 的"每条 Publish 都触发一次"。max=10,push 21 条:
//   - push #1..#10: 不触发裁剪
//   - push #11: n=11>10 → keep=5,丢 6 条 (1 次裁剪)
//   - push #12..#15: 不触发 (n 从 6 到 9)
//   - push #16: n=11>10 → keep=5,丢 6 条 (1 次裁剪)
//   - push #17..#21: 不触发 (n 从 6 到 9)
//
// 共 2 次裁剪、丢 12 条;旧策略 (裁到 max) 下 push #11..#21 每条都触发 → 11 次。
func TestQueue_PublishMaxLen_HalfWatermark(t *testing.T) {
	t.Parallel()
	m := newStubMetrics()
	q, _ := newTestQueueWith(t, WithDefaultMaxLen(10), WithMetrics(m))

	ctx := context.Background()
	for i := range 21 {
		if err := q.Publish(ctx, "h", i); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishDroppedN != 2 {
		t.Fatalf("PublishDropped call count = %d, want 2 (half-watermark, 旧策略应为 11)", m.publishDroppedN)
	}
	if m.publishDropped["h"] != 12 {
		t.Fatalf("PublishDropped sum = %d, want 12 (2 次 × 6)", m.publishDropped["h"])
	}
}
