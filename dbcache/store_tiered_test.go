package dbcache

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// stubStore 是测试用 Store,允许注入错误和观察调用次数。
type stubStore struct {
	data    map[string]Item
	getErr  error
	setErr  error
	delErr  error
	mgetErr error
	msetErr error
	gets    int
	sets    int
	dels    int
	mgets   int
	msets   int
}

func newStubStore() *stubStore {
	return &stubStore{data: map[string]Item{}}
}

func (s *stubStore) Get(ctx context.Context, key string) (Item, bool, error) {
	s.gets++
	if s.getErr != nil {
		return Item{}, false, s.getErr
	}
	item, ok := s.data[key]
	return item, ok, nil
}

func (s *stubStore) Set(ctx context.Context, key string, item Item, ttl time.Duration) error {
	s.sets++
	if s.setErr != nil {
		return s.setErr
	}
	s.data[key] = item
	return nil
}

func (s *stubStore) Delete(ctx context.Context, keys ...string) error {
	s.dels++
	if s.delErr != nil {
		return s.delErr
	}
	for _, k := range keys {
		delete(s.data, k)
	}
	return nil
}

func (s *stubStore) MGet(ctx context.Context, keys []string) (map[string]Item, error) {
	s.mgets++
	if s.mgetErr != nil {
		return nil, s.mgetErr
	}
	out := map[string]Item{}
	for _, k := range keys {
		if v, ok := s.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (s *stubStore) MSet(ctx context.Context, items map[string]Item, ttl time.Duration) error {
	s.msets++
	if s.msetErr != nil {
		return s.msetErr
	}
	maps.Copy(s.data, items)
	return nil
}

func (s *stubStore) Close() error { return nil }

func TestTieredStore_L1HitNoL2(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	tier := NewTieredStore(l1, l2)

	_ = tier.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute)
	l2.gets = 0 // reset

	item, hit, _ := tier.Get(ctx, "k")
	if !hit || string(item.Value) != "v" {
		t.Fatalf("expected l1 hit")
	}
	if l2.gets != 0 {
		t.Fatalf("l2 should not be touched on l1 hit, got %d", l2.gets)
	}
}

func TestTieredStore_L1MissL2HitRefill(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	tier := NewTieredStore(l1, l2)

	// 只在 l2 写入(模拟其它实例已写过共享 redis)
	l2.data["k"] = Item{Value: []byte("v")}

	item, hit, _ := tier.Get(ctx, "k")
	if !hit || string(item.Value) != "v" {
		t.Fatalf("expected hit via l2")
	}
	// l1 应被回填
	if _, ok := l1.data["k"]; !ok {
		t.Fatal("l1 should be refilled after l2 hit")
	}
	// 再次 Get,应直接 l1 命中
	l2.gets = 0
	tier.Get(ctx, "k")
	if l2.gets != 0 {
		t.Fatal("l2 should not be queried again after refill")
	}
}

func TestTieredStore_BothMiss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	tier := NewTieredStore(l1, l2)
	if _, hit, _ := tier.Get(ctx, "k"); hit {
		t.Fatal("expected miss")
	}
}

func TestTieredStore_SetDualWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	tier := NewTieredStore(l1, l2)

	_ = tier.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute)
	if _, ok := l1.data["k"]; !ok {
		t.Fatal("l1 not written")
	}
	if _, ok := l2.data["k"]; !ok {
		t.Fatal("l2 not written")
	}
}

func TestTieredStore_SetL2FailureNotFatal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	l2.setErr = errors.New("redis down")
	tier := NewTieredStore(l1, l2)

	if err := tier.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute); err != nil {
		t.Fatalf("l2 failure should not abort: %v", err)
	}
	if _, ok := l1.data["k"]; !ok {
		t.Fatal("l1 should still hold value when l2 fails")
	}
}

func TestTieredStore_DeleteDualClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	tier := NewTieredStore(l1, l2)
	_ = tier.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute)

	if err := tier.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := l1.data["k"]; ok {
		t.Fatal("l1 not cleared")
	}
	if _, ok := l2.data["k"]; ok {
		t.Fatal("l2 not cleared")
	}
}

func TestTieredStore_MGetMixedSources(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, l2 := newStubStore(), newStubStore()
	l1.data["a"] = Item{Value: []byte("1")}
	l2.data["b"] = Item{Value: []byte("2")}
	// c 都不存在
	tier := NewTieredStore(l1, l2)

	got, err := tier.MGet(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 hits, got %d: %+v", len(got), got)
	}
	if string(got["a"].Value) != "1" || string(got["b"].Value) != "2" {
		t.Fatalf("unexpected: %+v", got)
	}
	// b 应该被回填到 l1
	if _, ok := l1.data["b"]; !ok {
		t.Fatal("l1 not refilled for b")
	}
}

func TestTieredStore_NilPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewTieredStore(nil, newStubStore())
}

func TestTieredStore_RedisInvalidationClearsRemoteL1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mr := miniredis.RunT(t)
	clientA := newTestRedisClient(t, mr.Addr())
	clientB := newTestRedisClient(t, mr.Addr())
	const channel = "dbcache:users:invalidate"

	l1A := NewMemoryStore(10)
	l1B := NewMemoryStore(10)
	tierA := NewTieredStore(l1A, NewRedisStore(clientA, WithRedisInvalidation(channel)))
	tierB := NewTieredStore(l1B, NewRedisStore(clientB, WithRedisInvalidation(channel)))
	t.Cleanup(func() {
		_ = tierA.Close()
		_ = tierB.Close()
	})
	waitForRedisSubscribers(t, clientA, channel, 2)

	items := map[string]Item{
		"a": {Value: []byte("1")},
		"b": {Value: []byte("2")},
	}
	if err := tierB.MSet(ctx, items, time.Minute); err != nil {
		t.Fatalf("prime B: %v", err)
	}
	if err := tierA.Delete(ctx, "a", "b"); err != nil {
		t.Fatalf("delete A: %v", err)
	}

	waitForStoreMiss(t, l1B, "a", "b")
	if got, err := tierB.MGet(ctx, []string{"a", "b"}); err != nil || len(got) != 0 {
		t.Fatalf("B should miss both L1 and L2 after invalidation, got=%v err=%v", got, err)
	}
}

func TestTieredStore_InvalidationIgnoresSelfAndMalformedMessages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := newTestRedisClient(t, mr.Addr())
	const channel = "dbcache:self:invalidate"

	l1 := NewMemoryStore(10)
	l2 := NewRedisStore(client, WithRedisInvalidation(channel))
	tier := NewTieredStore(l1, l2)
	t.Cleanup(func() { _ = tier.Close() })
	waitForRedisSubscribers(t, client, channel, 1)

	if err := tier.MSet(ctx, map[string]Item{
		"self":  {Value: []byte("keep")},
		"other": {Value: []byte("drop")},
	}, time.Minute); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if err := client.Publish(ctx, channel, "not-json").Err(); err != nil {
		t.Fatalf("publish malformed: %v", err)
	}
	selfPayload, _ := json.Marshal(redisInvalidationMessage{
		Version: redisInvalidationVersion,
		Source:  l2.(*redisStore).instanceId,
		Keys:    []string{"self"},
	})
	if err := client.Publish(ctx, channel, selfPayload).Err(); err != nil {
		t.Fatalf("publish self: %v", err)
	}
	otherPayload, _ := json.Marshal(redisInvalidationMessage{
		Version: redisInvalidationVersion,
		Source:  "another-process",
		Keys:    []string{"other"},
	})
	if err := client.Publish(ctx, channel, otherPayload).Err(); err != nil {
		t.Fatalf("publish other: %v", err)
	}

	waitForStoreMiss(t, l1, "other")
	if item, hit, err := l1.Get(ctx, "self"); err != nil || !hit || string(item.Value) != "keep" {
		t.Fatalf("self invalidation should be ignored: hit=%v item=%+v err=%v", hit, item, err)
	}
}

func TestTieredStore_CloseStopsInvalidationSubscriber(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := newTestRedisClient(t, mr.Addr())
	const channel = "dbcache:close:invalidate"

	tier := NewTieredStore(
		NewMemoryStore(10),
		NewRedisStore(client, WithRedisInvalidation(channel)),
	)
	waitForRedisSubscribers(t, client, channel, 1)
	if err := tier.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := tier.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	waitForRedisSubscribers(t, client, channel, 0)
}

func waitForStoreMiss(t *testing.T, store Store, keys ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		allMissing := true
		for _, key := range keys {
			_, hit, err := store.Get(ctx, key)
			if err != nil {
				t.Fatalf("get %q: %v", key, err)
			}
			if hit {
				allMissing = false
			}
		}
		if allMissing {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for local invalidation timed out: keys=%v", keys)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
