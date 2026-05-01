package dbcache

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryStore_GetSetDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(10)

	// 未命中
	if _, hit, err := s.Get(ctx, "k"); err != nil || hit {
		t.Fatalf("expected miss, got hit=%v err=%v", hit, err)
	}

	// 写后命中
	if err := s.Set(ctx, "k", Item{Value: []byte("v")}, time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	item, hit, err := s.Get(ctx, "k")
	if err != nil || !hit {
		t.Fatalf("expected hit, got hit=%v err=%v", hit, err)
	}
	if string(item.Value) != "v" {
		t.Fatalf("expected value v, got %q", item.Value)
	}

	// 删除后未命中
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, hit, _ := s.Get(ctx, "k"); hit {
		t.Fatal("expected miss after delete")
	}
}

func TestMemoryStore_NotFoundItem(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(10)

	if err := s.Set(ctx, "k", Item{NotFound: true}, time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	item, hit, _ := s.Get(ctx, "k")
	if !hit || !item.NotFound {
		t.Fatalf("expected hit + NotFound, got hit=%v item=%+v", hit, item)
	}
}

func TestMemoryStore_TTLExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(10)

	if err := s.Set(ctx, "k", Item{Value: []byte("v")}, 30*time.Millisecond); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, hit, _ := s.Get(ctx, "k"); !hit {
		t.Fatal("expected immediate hit")
	}
	time.Sleep(60 * time.Millisecond)
	if _, hit, _ := s.Get(ctx, "k"); hit {
		t.Fatal("expected miss after TTL")
	}
}

func TestMemoryStore_ZeroTTLNeverExpires(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(10)

	_ = s.Set(ctx, "k", Item{Value: []byte("v")}, 0)
	time.Sleep(20 * time.Millisecond)
	if _, hit, _ := s.Get(ctx, "k"); !hit {
		t.Fatal("ttl=0 should not expire")
	}
}

func TestMemoryStore_LRUEviction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(2)

	_ = s.Set(ctx, "a", Item{Value: []byte("1")}, time.Minute)
	_ = s.Set(ctx, "b", Item{Value: []byte("2")}, time.Minute)
	// 触访 a 让 b 成为 LRU
	_, _, _ = s.Get(ctx, "a")
	_ = s.Set(ctx, "c", Item{Value: []byte("3")}, time.Minute)

	if _, hit, _ := s.Get(ctx, "b"); hit {
		t.Fatal("expected b evicted")
	}
	if _, hit, _ := s.Get(ctx, "a"); !hit {
		t.Fatal("expected a still present")
	}
	if _, hit, _ := s.Get(ctx, "c"); !hit {
		t.Fatal("expected c present")
	}
}

func TestMemoryStore_MGetMSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(10)

	items := map[string]Item{
		"a": {Value: []byte("1")},
		"b": {Value: []byte("2")},
	}
	if err := s.MSet(ctx, items, time.Minute); err != nil {
		t.Fatalf("mset: %v", err)
	}
	got, err := s.MGet(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if len(got) != 2 || string(got["a"].Value) != "1" || string(got["b"].Value) != "2" {
		t.Fatalf("unexpected mget result: %+v", got)
	}
	if _, ok := got["c"]; ok {
		t.Fatal("c should be missing")
	}
}

func TestMemoryStore_ConcurrentSafety(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewMemoryStore(1000)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i % 10)}
			_ = s.Set(ctx, string(key), Item{Value: key}, time.Minute)
			_, _, _ = s.Get(ctx, string(key))
			_ = s.Delete(ctx, string(key))
		}(i)
	}
	wg.Wait()
}

func TestMemoryStore_DefaultSize(t *testing.T) {
	t.Parallel()
	// size<=0 应当走默认值,不 panic
	s := NewMemoryStore(0)
	if s == nil {
		t.Fatal("got nil store")
	}
}
