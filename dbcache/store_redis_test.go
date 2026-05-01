package dbcache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, mr
}

func TestRedisStore_GetSetDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := newTestRedis(t)
	s := NewRedisStore(client, WithRedisKeyPrefix("test:"))

	if _, hit, _ := s.Get(ctx, "k"); hit {
		t.Fatal("expected miss")
	}
	if err := s.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	item, hit, err := s.Get(ctx, "k")
	if err != nil || !hit || string(item.Value) != "v" {
		t.Fatalf("unexpected get: hit=%v err=%v item=%+v", hit, err, item)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, hit, _ := s.Get(ctx, "k"); hit {
		t.Fatal("expected miss after delete")
	}
}

func TestRedisStore_NotFoundItem(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := newTestRedis(t)
	s := NewRedisStore(client)

	_ = s.Set(ctx, "k", Item{NotFound: true}, time.Minute)
	item, hit, _ := s.Get(ctx, "k")
	if !hit || !item.NotFound {
		t.Fatalf("expected NotFound hit, got hit=%v item=%+v", hit, item)
	}
}

func TestRedisStore_KeyPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, mr := newTestRedis(t)
	s := NewRedisStore(client, WithRedisKeyPrefix("app:cache:"))

	_ = s.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute)

	if !mr.Exists("app:cache:k") {
		t.Fatal("expected key with prefix in redis")
	}
	if mr.Exists("k") {
		t.Fatal("plain key should not exist")
	}
}

func TestRedisStore_Ttl(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, mr := newTestRedis(t)
	s := NewRedisStore(client)

	_ = s.Set(ctx, "k", Item{Value: []byte("v")}, 100*time.Millisecond)
	mr.FastForward(200 * time.Millisecond)
	if _, hit, _ := s.Get(ctx, "k"); hit {
		t.Fatal("expected miss after ttl")
	}
}

func TestRedisStore_MGetMSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := newTestRedis(t)
	s := NewRedisStore(client)

	items := map[string]Item{
		"a": {Value: []byte("1")},
		"b": {Value: []byte("2")},
		"c": {NotFound: true},
	}
	if err := s.MSet(ctx, items, time.Minute); err != nil {
		t.Fatalf("mset: %v", err)
	}

	got, err := s.MGet(ctx, []string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 hits (a,b,c), got %d: %+v", len(got), got)
	}
	if string(got["a"].Value) != "1" || string(got["b"].Value) != "2" {
		t.Fatalf("unexpected values: %+v", got)
	}
	if !got["c"].NotFound {
		t.Fatal("c should be NotFound")
	}
	if _, ok := got["d"]; ok {
		t.Fatal("d should be missing")
	}
}

func TestRedisStore_NilClientPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil client")
		}
	}()
	NewRedisStore(nil)
}

func TestRedisStore_EmptyMSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := newTestRedis(t)
	s := NewRedisStore(client)
	if err := s.MSet(ctx, map[string]Item{}, time.Minute); err != nil {
		t.Fatalf("empty mset should be no-op, got %v", err)
	}
	if err := s.Delete(ctx); err != nil {
		t.Fatalf("empty delete should be no-op, got %v", err)
	}
}

func TestRedisStore_PayloadEncoding(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		item Item
	}{
		{"value", Item{Value: []byte("hello")}},
		{"empty value", Item{Value: nil}},
		{"not found", Item{NotFound: true}},
		{"binary value", Item{Value: []byte{0x00, 0x01, 0xff}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := encodeRedisPayload(tc.item)
			got := decodeRedisPayload(payload)
			if got.NotFound != tc.item.NotFound {
				t.Fatalf("NotFound mismatch: want %v got %v", tc.item.NotFound, got.NotFound)
			}
			if !tc.item.NotFound && string(got.Value) != string(tc.item.Value) {
				t.Fatalf("Value mismatch: want %q got %q", tc.item.Value, got.Value)
			}
		})
	}
}
