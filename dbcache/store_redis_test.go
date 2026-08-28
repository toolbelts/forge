package dbcache

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// commandErrorHook 可按命令名注入错误,用于分别验证 DEL 与 PUBLISH 的失败语义。
type commandErrorHook struct {
	command string
	err     error
	calls   atomic.Int64
}

func (h *commandErrorHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *commandErrorHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), h.command) {
			h.calls.Add(1)
			return h.err
		}
		return next(ctx, cmd)
	}
}

func (h *commandErrorHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, mr
}

func newTestRedisClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitForRedisSubscribers(t *testing.T, client redis.UniversalClient, channel string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		counts, err := client.PubSubNumSub(ctx, channel).Result()
		if err == nil && counts[channel] == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait subscribers channel=%q want=%d: counts=%v err=%v", channel, want, counts, err)
		case <-time.After(5 * time.Millisecond):
		}
	}
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

func TestRedisStore_InvalidationPublish(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, mr := newTestRedis(t)
	const channel = "dbcache:test:invalidate"

	pubsub := client.Subscribe(ctx, channel)
	t.Cleanup(func() { _ = pubsub.Close() })
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	s := NewRedisStore(client,
		WithRedisKeyPrefix("cache:"),
		WithRedisInvalidation(channel),
	)
	if err := s.MSet(ctx, map[string]Item{
		"a": {Value: []byte("1")},
		"b": {Value: []byte("2")},
	}, time.Minute); err != nil {
		t.Fatalf("mset: %v", err)
	}
	if err := s.Delete(ctx, "a", "b"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive invalidation: %v", err)
	}
	var event redisInvalidationMessage
	if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
		t.Fatalf("unmarshal invalidation: %v", err)
	}
	if event.Version != redisInvalidationVersion || event.Source == "" {
		t.Fatalf("unexpected envelope: %+v", event)
	}
	if want := []string{"a", "b"}; !slices.Equal(event.Keys, want) {
		t.Fatalf("expected logical keys %v, got %v", want, event.Keys)
	}
	if mr.Exists("cache:a") || mr.Exists("cache:b") {
		t.Fatal("redis keys should be deleted before invalidation is published")
	}
}

func TestRedisStore_InvalidationPublishFailureReturnedAfterDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, mr := newTestRedis(t)
	s := NewRedisStore(client, WithRedisInvalidation("dbcache:test:invalidate"))
	if err := s.Set(ctx, "k", Item{Value: []byte("v")}, time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	publishErr := errors.New("publish unavailable")
	hook := &commandErrorHook{command: "publish", err: publishErr}
	client.AddHook(hook)
	err := s.Delete(ctx, "k")
	if !errors.Is(err, publishErr) {
		t.Fatalf("expected publish error, got %v", err)
	}
	if mr.Exists("k") {
		t.Fatal("key should already be deleted when publish fails")
	}
	if hook.calls.Load() != 1 {
		t.Fatalf("expected one publish attempt, got %d", hook.calls.Load())
	}
}

func TestRedisStore_DeleteFailureSkipsInvalidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := newTestRedis(t)
	s := NewRedisStore(client, WithRedisInvalidation("dbcache:test:invalidate"))

	delErr := errors.New("delete unavailable")
	delHook := &commandErrorHook{command: "del", err: delErr}
	publishHook := &commandErrorHook{command: "publish", err: errors.New("unexpected publish")}
	client.AddHook(delHook)
	client.AddHook(publishHook)
	err := s.Delete(ctx, "k")
	if !errors.Is(err, delErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if publishHook.calls.Load() != 0 {
		t.Fatalf("publish should be skipped after delete failure, got %d calls", publishHook.calls.Load())
	}
}

func TestRedisStore_InvalidationDisabledDoesNotPublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := newTestRedis(t)
	s := NewRedisStore(client)
	publishHook := &commandErrorHook{command: "publish", err: errors.New("unexpected publish")}
	client.AddHook(publishHook)

	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete without invalidation: %v", err)
	}
	if publishHook.calls.Load() != 0 {
		t.Fatalf("disabled invalidation should not publish, got %d calls", publishHook.calls.Load())
	}
}

func TestRedisStore_EmptyInvalidationChannelPanics(t *testing.T) {
	t.Parallel()
	client, _ := newTestRedis(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for empty invalidation channel")
		}
	}()
	NewRedisStore(client, WithRedisInvalidation("  "))
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
