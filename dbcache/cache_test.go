package dbcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type user struct {
	ID   int
	Name string
}

func TestCache_GetCacheAside(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var calls atomic.Int64
	loader := func(ctx context.Context, id int) (user, error) {
		calls.Add(1)
		return user{ID: id, Name: fmt.Sprintf("u%d", id)}, nil
	}
	c := New(loader, WithTtl(time.Minute))

	// 第一次:miss + loader
	u, err := c.Get(ctx, 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if u.ID != 1 || u.Name != "u1" {
		t.Fatalf("unexpected user: %+v", u)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 loader call, got %d", calls.Load())
	}

	// 第二次:命中,loader 不再被调用
	u, err = c.Get(ctx, 1)
	if err != nil || u.ID != 1 {
		t.Fatalf("second get: u=%+v err=%v", u, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected still 1 loader call, got %d", calls.Load())
	}
}

func TestCache_NotFound_NegativeCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var calls atomic.Int64
	loader := func(ctx context.Context, id int) (user, error) {
		calls.Add(1)
		return user{}, ErrNotFound
	}
	c := New(loader, WithNegativeTtl(time.Minute))

	_, err := c.Get(ctx, 99)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 loader call")
	}

	// 再次 Get 走负缓存,loader 不被调用
	_, err = c.Get(ctx, 99)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected loader cached as negative, got %d calls", calls.Load())
	}
}

func TestCache_LoaderRealError_NotCached(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dbErr := errors.New("connection refused")
	var calls atomic.Int64
	loader := func(ctx context.Context, id int) (user, error) {
		calls.Add(1)
		return user{}, dbErr
	}
	c := New(loader)

	_, err := c.Get(ctx, 1)
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected wrapped dbErr, got %v", err)
	}
	// 真错不进缓存,第二次还会触 loader
	_, _ = c.Get(ctx, 1)
	if calls.Load() != 2 {
		t.Fatalf("real error should not be cached, expected 2 calls, got %d", calls.Load())
	}
}

func TestCache_Singleflight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var calls atomic.Int64
	loader := func(ctx context.Context, id int) (user, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return user{ID: id, Name: "u"}, nil
	}
	c := New(loader)

	const N = 100
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			_, _ = c.Get(ctx, 42)
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("singleflight failed: expected 1 loader call, got %d", calls.Load())
	}
}

func TestCache_Set_Delete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	loader := func(ctx context.Context, id int) (user, error) {
		return user{ID: id, Name: "loader"}, nil
	}
	c := New(loader)

	if err := c.Set(ctx, 1, user{ID: 1, Name: "manual"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	u, _ := c.Get(ctx, 1)
	if u.Name != "manual" {
		t.Fatalf("expected manual set value, got %+v", u)
	}

	if err := c.Delete(ctx, 1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	u, _ = c.Get(ctx, 1)
	if u.Name != "loader" {
		t.Fatalf("after delete, should refetch: got %+v", u)
	}
}

func TestCache_MGet_PerKeyLoader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var calls atomic.Int64
	loader := func(ctx context.Context, id int) (user, error) {
		calls.Add(1)
		if id == 99 {
			return user{}, ErrNotFound
		}
		return user{ID: id, Name: fmt.Sprintf("u%d", id)}, nil
	}
	c := New(loader)

	got, err := c.MGet(ctx, 1, 2, 99)
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 hits, got %d: %+v", len(got), got)
	}
	if got[1].Name != "u1" || got[2].Name != "u2" {
		t.Fatalf("unexpected: %+v", got)
	}
	if _, ok := got[99]; ok {
		t.Fatal("99 should not be in result (NotFound)")
	}
}

func TestCache_MGet_BatchLoader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var batchCalls atomic.Int64
	var receivedKeys []int
	var mu sync.Mutex

	loader := func(ctx context.Context, id int) (user, error) {
		t.Fatalf("single loader should not be called when batch is set")
		return user{}, nil
	}
	batch := func(ctx context.Context, ids []int) (map[int]user, error) {
		batchCalls.Add(1)
		mu.Lock()
		receivedKeys = append(receivedKeys, ids...)
		mu.Unlock()
		out := map[int]user{}
		for _, id := range ids {
			if id == 99 {
				continue // 模拟 DB 中没有
			}
			out[id] = user{ID: id, Name: "u"}
		}
		return out, nil
	}

	c := New(loader, WithBatchLoader(BatchLoaderFunc[int, user](batch)))

	got, err := c.MGet(ctx, 1, 2, 99)
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if batchCalls.Load() != 1 {
		t.Fatalf("expected 1 batch call, got %d", batchCalls.Load())
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(got))
	}

	// 99 应被负缓存,再次 MGet 不会触 batch
	got2, _ := c.MGet(ctx, 1, 2, 99)
	if batchCalls.Load() != 1 {
		t.Fatalf("99 should be negatively cached, got %d batch calls", batchCalls.Load())
	}
	if len(got2) != 2 {
		t.Fatalf("expected 2 hits second time, got %d", len(got2))
	}
}

func TestCache_KeyPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStubStore()
	loader := func(ctx context.Context, id int) (user, error) {
		return user{ID: id}, nil
	}
	c := New(loader, WithStore(store), WithKeyPrefix("u:"))
	_, _ = c.Get(ctx, 7)
	if _, ok := store.data["u:7"]; !ok {
		t.Fatalf("expected store key u:7, got keys: %v", keysOf(store.data))
	}
}

func TestCache_Closed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := New(func(ctx context.Context, k int) (int, error) { return k, nil })
	_ = c.Close()

	if _, err := c.Get(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := c.Set(ctx, 1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on set, got %v", err)
	}
	if err := c.Delete(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on delete, got %v", err)
	}
	// Close 二次幂等
	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestCache_NilLoaderPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil loader")
		}
	}()
	New[int, int](nil)
}

func TestCache_BatchLoaderTypeMismatch(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on batch loader type mismatch")
		}
	}()
	loader := func(ctx context.Context, k int) (user, error) { return user{}, nil }
	// 类型错配:Cache 是 [int, user],BatchLoader 是 [string, user]
	wrongBatch := func(ctx context.Context, ks []string) (map[string]user, error) { return nil, nil }
	New(loader, WithBatchLoader(BatchLoaderFunc[string, user](wrongBatch)))
}

func TestCache_Jitter(t *testing.T) {
	t.Parallel()
	c := &Cache[int, int]{ttl: 100 * time.Millisecond, jitter: 0.5}
	for range 100 {
		d := c.jitterDuration(c.ttl)
		// 应在 [50ms, 150ms] 内
		if d < 50*time.Millisecond || d > 150*time.Millisecond {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}

func TestCache_Warm(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var calls atomic.Int64
	loader := func(ctx context.Context, id int) (user, error) {
		calls.Add(1)
		return user{ID: id}, nil
	}
	c := New(loader)
	if err := c.Warm(ctx, []int{1, 2, 3}); err != nil {
		t.Fatalf("warm: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 calls during warm, got %d", calls.Load())
	}
	// Warm 后 Get 应零触 loader
	prev := calls.Load()
	for _, id := range []int{1, 2, 3} {
		_, _ = c.Get(ctx, id)
	}
	if calls.Load() != prev {
		t.Fatal("Get after Warm should not trigger loader")
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
