package token

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis 创建一组 miniredis 与 go-redis 客户端用于测试。
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

// newTestManager 用更短的 Ttl 配置出测试用 Manager,便于覆盖过期路径。
func newTestManager(t *testing.T, opts ...Option) (*miniredis.Miniredis, *Manager) {
	t.Helper()
	mr, client := newTestRedis(t)
	base := []Option{
		WithAccessTtl(50 * time.Millisecond),
		WithAccessSaveTtl(5 * time.Second),
		WithRefreshTtl(200 * time.Millisecond),
		WithRefreshSaveTtl(10 * time.Second),
	}
	base = append(base, opts...)
	mgr, err := NewManager(client, base...)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mr, mgr
}

// mustSMembers 读取 set 成员并强制 fail 处理 err,简化断言。
func mustSMembers(t *testing.T, mr *miniredis.Miniredis, key string) []string {
	t.Helper()
	members, err := mr.SMembers(key)
	if err != nil && !mr.Exists(key) {
		return nil
	}
	if err != nil {
		t.Fatalf("smembers %q: %v", key, err)
	}
	return members
}

// TestNewManager_InvalidOption 验证 Ttl 顺序违反或 nil 客户端会被拒绝。
func TestNewManager_InvalidOption(t *testing.T) {
	_, client := newTestRedis(t)
	cases := []struct {
		name string
		opts []Option
	}{
		{"access save not greater", []Option{WithAccessTtl(time.Second), WithAccessSaveTtl(time.Second)}},
		{"refresh save not greater", []Option{WithRefreshTtl(time.Hour), WithRefreshSaveTtl(time.Hour)}},
		{"refresh shorter than access", []Option{
			WithAccessTtl(time.Hour),
			WithAccessSaveTtl(2 * time.Hour),
			WithRefreshTtl(30 * time.Minute),
			WithRefreshSaveTtl(time.Hour),
		}},
	}
	for _, tc := range cases {
		if _, err := NewManager(client, tc.opts...); !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("%s: expected ErrInvalidOption, got %v", tc.name, err)
		}
	}
	if _, err := NewManager(nil); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("nil rdb: expected ErrNilRedisClient, got %v", err)
	}
}

// TestCreate 验证 Create 会写入三处 entry 并保留正确的过期时长。
func TestCreate(t *testing.T) {
	mr, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1001, map[string]string{"device": "web"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if token.AccessToken == "" || token.RefreshToken == "" {
		t.Fatal("expected non-empty tokens")
	}
	if token.UserId != 1001 {
		t.Fatalf("expected user id 1001, got %d", token.UserId)
	}
	if !mr.Exists(mgr.accessKey(token.AccessToken)) {
		t.Fatal("expected access entry written to redis")
	}
	if !mr.Exists(mgr.refreshKey(token.RefreshToken)) {
		t.Fatal("expected refresh entry written to redis")
	}
	members := mustSMembers(t, mr, mgr.userKey(1001))
	if len(members) != 1 || members[0] != token.AccessToken {
		t.Fatalf("expected user index to contain access token, got %v", members)
	}
	if ttl := mr.TTL(mgr.accessKey(token.AccessToken)); ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("expected access save ttl ~5s, got %s", ttl)
	}
	if ttl := mr.TTL(mgr.refreshKey(token.RefreshToken)); ttl <= 5*time.Second || ttl > 10*time.Second {
		t.Fatalf("expected refresh save ttl ~10s, got %s", ttl)
	}
}

// TestCreate_InvalidUserId 验证非法 userId 会被拒绝。
func TestCreate_InvalidUserId(t *testing.T) {
	_, mgr := newTestManager(t)
	if _, err := mgr.Create(context.Background(), 0, nil); !errors.Is(err, ErrInvalidUserId) {
		t.Fatalf("expected ErrInvalidUserId, got %v", err)
	}
}

// TestValidate_OK 验证 Validate 在 token 存活期内返回原始信息。
func TestValidate_OK(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := mgr.Validate(context.Background(), token.AccessToken)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got.UserId != token.UserId {
		t.Fatalf("expected user id %d, got %d", token.UserId, got.UserId)
	}
	if got.Metadata["k"] != "v" {
		t.Fatalf("expected metadata round-trip, got %v", got.Metadata)
	}
}

// TestValidate_NotFound 验证未知 token 会返回 ErrTokenNotFound。
func TestValidate_NotFound(t *testing.T) {
	_, mgr := newTestManager(t)
	if _, err := mgr.Validate(context.Background(), "non-existent"); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("expected ErrTokenNotFound, got %v", err)
	}
}

// TestValidate_EmptyToken 验证空字符串入参会返回 ErrEmptyToken。
func TestValidate_EmptyToken(t *testing.T) {
	_, mgr := newTestManager(t)
	if _, err := mgr.Validate(context.Background(), ""); !errors.Is(err, ErrEmptyToken) {
		t.Fatalf("expected ErrEmptyToken, got %v", err)
	}
}

// TestValidate_Expired 验证逻辑过期但 Redis 仍保留数据时返回 ErrTokenExpired。
func TestValidate_Expired(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, err := mgr.Validate(context.Background(), token.AccessToken); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
}

// TestValidate_Corrupted 验证 Redis 中存在但无法反序列化的载荷会返回 ErrTokenCorrupted。
func TestValidate_Corrupted(t *testing.T) {
	mr, mgr := newTestManager(t)
	if err := mr.Set(mgr.accessKey("garbage"), "{not json"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := mgr.Validate(context.Background(), "garbage"); !errors.Is(err, ErrTokenCorrupted) {
		t.Fatalf("expected ErrTokenCorrupted, got %v", err)
	}
}

// TestSaveTtlBuffer 验证 saveTtl 提供的排查缓冲:逻辑过期后仍能在 Redis 中查到原始记录。
func TestSaveTtlBuffer(t *testing.T) {
	mr, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, err := mgr.Validate(context.Background(), token.AccessToken); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
	if !mr.Exists(mgr.accessKey(token.AccessToken)) {
		t.Fatal("expected access entry to remain in redis within save ttl buffer")
	}
}

// TestRenew_OK 验证 Renew 会刷新 access entry 的 PX 与 Token 内的 AccessExpires,但不延长 RefreshExpires。
func TestRenew_OK(t *testing.T) {
	mr, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	originalRefresh := token.RefreshExpires
	time.Sleep(20 * time.Millisecond)
	renewed, err := mgr.Renew(context.Background(), token.AccessToken)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !renewed.AccessExpires.After(token.AccessExpires) {
		t.Fatalf("expected AccessExpires to advance, was %s now %s", token.AccessExpires, renewed.AccessExpires)
	}
	if !renewed.RefreshExpires.Equal(originalRefresh) {
		t.Fatalf("expected RefreshExpires unchanged, was %s now %s", originalRefresh, renewed.RefreshExpires)
	}
	if ttl := mr.TTL(mgr.accessKey(token.AccessToken)); ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("expected access save ttl reset to ~5s, got %s", ttl)
	}
}

// TestRenew_Expired 验证逻辑过期 token 不允许续期。
func TestRenew_Expired(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, err := mgr.Renew(context.Background(), token.AccessToken); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
}

// TestRenew_NotFound 验证未知 token 会返回 ErrTokenNotFound。
func TestRenew_NotFound(t *testing.T) {
	_, mgr := newTestManager(t)
	if _, err := mgr.Renew(context.Background(), "non-existent"); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("expected ErrTokenNotFound, got %v", err)
	}
}

// TestRefresh_OK_WithRotation 验证默认旋转模式下旧 access 与旧 refresh 都失效,新两者生效。
func TestRefresh_OK_WithRotation(t *testing.T) {
	mr, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, map[string]string{"device": "web"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	next, err := mgr.Refresh(context.Background(), token.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if next.AccessToken == token.AccessToken {
		t.Fatal("expected new access token to differ")
	}
	if next.RefreshToken == token.RefreshToken {
		t.Fatal("expected new refresh token to differ under rotation")
	}
	if next.Metadata["device"] != "web" {
		t.Fatalf("expected metadata preserved, got %v", next.Metadata)
	}
	if mr.Exists(mgr.accessKey(token.AccessToken)) {
		t.Fatal("expected old access entry deleted")
	}
	if mr.Exists(mgr.refreshKey(token.RefreshToken)) {
		t.Fatal("expected old refresh entry deleted under rotation")
	}
	if !mr.Exists(mgr.accessKey(next.AccessToken)) {
		t.Fatal("expected new access entry written")
	}
	if !mr.Exists(mgr.refreshKey(next.RefreshToken)) {
		t.Fatal("expected new refresh entry written")
	}
	members := mustSMembers(t, mr, mgr.userKey(1))
	if len(members) != 1 || members[0] != next.AccessToken {
		t.Fatalf("expected user index to point to new access, got %v", members)
	}
}

// TestRefresh_OK_NoRotation 验证关闭旋转时 refresh_token 字符串保持不变,但旧 access 仍失效。
func TestRefresh_OK_NoRotation(t *testing.T) {
	mr, mgr := newTestManager(t, WithRefreshRotation(false))
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	next, err := mgr.Refresh(context.Background(), token.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if next.RefreshToken != token.RefreshToken {
		t.Fatal("expected refresh token unchanged when rotation disabled")
	}
	if next.AccessToken == token.AccessToken {
		t.Fatal("expected access token to be replaced")
	}
	if mr.Exists(mgr.accessKey(token.AccessToken)) {
		t.Fatal("expected old access entry deleted")
	}
	if !mr.Exists(mgr.refreshKey(token.RefreshToken)) {
		t.Fatal("expected refresh entry preserved under no rotation")
	}
}

// TestRefresh_NoRotationConsecutive 验证关闭 refresh 旋转时,refresh entry 会更新为最新 access,
// 连续 Refresh 不会留下上一代 access_token。
func TestRefresh_NoRotationConsecutive(t *testing.T) {
	mr, mgr := newTestManager(t, WithRefreshRotation(false))
	first, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	second, err := mgr.Refresh(context.Background(), first.RefreshToken)
	if err != nil {
		t.Fatalf("refresh second: %v", err)
	}
	third, err := mgr.Refresh(context.Background(), first.RefreshToken)
	if err != nil {
		t.Fatalf("refresh third: %v", err)
	}

	if first.RefreshToken != second.RefreshToken || second.RefreshToken != third.RefreshToken {
		t.Fatal("expected refresh token to remain stable when rotation is disabled")
	}
	if mr.Exists(mgr.accessKey(first.AccessToken)) {
		t.Fatal("expected first access entry deleted")
	}
	if mr.Exists(mgr.accessKey(second.AccessToken)) {
		t.Fatal("expected second access entry deleted after third refresh")
	}
	if !mr.Exists(mgr.accessKey(third.AccessToken)) {
		t.Fatal("expected latest access entry to exist")
	}
	if _, err := mgr.Validate(context.Background(), third.AccessToken); err != nil {
		t.Fatalf("expected latest access token to validate: %v", err)
	}
	members := mustSMembers(t, mr, mgr.userKey(1))
	if len(members) != 1 || members[0] != third.AccessToken {
		t.Fatalf("expected user index to contain only latest access, got %v", members)
	}
}

// TestRefresh_Replay 验证旋转模式下重放旧 refresh_token 会失败。
func TestRefresh_Replay(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mgr.Refresh(context.Background(), token.RefreshToken); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if _, err := mgr.Refresh(context.Background(), token.RefreshToken); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("expected ErrTokenNotFound on replay, got %v", err)
	}
}

// TestRefresh_ExpiredRefresh 验证 refresh_token 逻辑过期时返回 ErrTokenExpired。
func TestRefresh_ExpiredRefresh(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(250 * time.Millisecond)
	if _, err := mgr.Refresh(context.Background(), token.RefreshToken); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
}

// TestRefresh_NotFound 验证未知 refresh_token 会返回 ErrTokenNotFound。
func TestRefresh_NotFound(t *testing.T) {
	_, mgr := newTestManager(t)
	if _, err := mgr.Refresh(context.Background(), "non-existent"); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("expected ErrTokenNotFound, got %v", err)
	}
}

// TestDelete 验证 Delete 会同时清掉 access entry、refresh entry 与用户索引引用。
func TestDelete(t *testing.T) {
	mr, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := mgr.Delete(context.Background(), token.AccessToken); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if mr.Exists(mgr.accessKey(token.AccessToken)) {
		t.Fatal("expected access entry deleted")
	}
	if mr.Exists(mgr.refreshKey(token.RefreshToken)) {
		t.Fatal("expected refresh entry deleted")
	}
	members := mustSMembers(t, mr, mgr.userKey(1))
	if len(members) != 0 {
		t.Fatalf("expected user index empty, got %v", members)
	}
}

// TestDelete_Idempotent 验证删除不存在的 token 不会报错。
func TestDelete_Idempotent(t *testing.T) {
	_, mgr := newTestManager(t)
	if err := mgr.Delete(context.Background(), "non-existent"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

// TestDeleteByUser 验证用户下所有 token 全部清理,用户索引本身也被删除。
func TestDeleteByUser(t *testing.T) {
	mr, mgr := newTestManager(t)
	created := make([]*Token, 0, 3)
	for i := range 3 {
		token, err := mgr.Create(context.Background(), 42, nil)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		created = append(created, token)
	}
	if err := mgr.DeleteByUser(context.Background(), 42); err != nil {
		t.Fatalf("delete by user: %v", err)
	}
	for _, token := range created {
		if mr.Exists(mgr.accessKey(token.AccessToken)) {
			t.Fatal("expected access entry deleted")
		}
		if mr.Exists(mgr.refreshKey(token.RefreshToken)) {
			t.Fatal("expected refresh entry deleted")
		}
	}
	if mr.Exists(mgr.userKey(42)) {
		t.Fatal("expected user key removed")
	}
}

// TestListByUser 验证返回该用户当前持有的全部 token。
func TestListByUser(t *testing.T) {
	_, mgr := newTestManager(t)
	created := make([]*Token, 0, 3)
	for i := range 3 {
		token, err := mgr.Create(context.Background(), 7, map[string]string{"device": fmt.Sprintf("dev-%d", i)})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		created = append(created, token)
	}
	list, err := mgr.ListByUser(context.Background(), 7)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(list))
	}
	seen := make(map[string]struct{}, len(list))
	for _, token := range list {
		seen[token.AccessToken] = struct{}{}
	}
	for _, token := range created {
		if _, ok := seen[token.AccessToken]; !ok {
			t.Fatalf("expected access token %q in list", token.AccessToken)
		}
	}
}

// TestListByUser_LazyCleanup 验证用户索引中残留的幽灵成员会被惰性清理。
func TestListByUser_LazyCleanup(t *testing.T) {
	mr, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 9, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mr.SetAdd(mgr.userKey(9), "ghost-token"); err != nil {
		t.Fatalf("seed ghost: %v", err)
	}
	list, err := mgr.ListByUser(context.Background(), 9)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].AccessToken != token.AccessToken {
		t.Fatalf("expected only valid token returned, got %+v", list)
	}
	for _, member := range mustSMembers(t, mr, mgr.userKey(9)) {
		if member == "ghost-token" {
			t.Fatal("expected ghost member removed from user index")
		}
	}
}

// TestToken_AccessValid 验证 AccessValid 在过期前后返回正确的布尔值。
func TestToken_AccessValid(t *testing.T) {
	now := time.Now()
	live := &Token{AccessExpires: now.Add(time.Hour)}
	if !live.AccessValid() {
		t.Fatal("expected access valid before expiry")
	}
	dead := &Token{AccessExpires: now.Add(-time.Hour)}
	if dead.AccessValid() {
		t.Fatal("expected access invalid after expiry")
	}
}

// TestToken_RefreshValid 验证 RefreshValid 在过期前后返回正确的布尔值。
func TestToken_RefreshValid(t *testing.T) {
	now := time.Now()
	live := &Token{RefreshExpires: now.Add(time.Hour)}
	if !live.RefreshValid() {
		t.Fatal("expected refresh valid before expiry")
	}
	dead := &Token{RefreshExpires: now.Add(-time.Hour)}
	if dead.RefreshValid() {
		t.Fatal("expected refresh invalid after expiry")
	}
}

// TestToken_GetMeta 验证 GetMeta 在 nil、缺失、命中三种场景下的返回值。
func TestToken_GetMeta(t *testing.T) {
	empty := &Token{}
	if val := empty.GetMeta("device"); val != "" {
		t.Fatalf("expected empty on nil metadata, got %q", val)
	}
	withMeta := &Token{Metadata: map[string]string{"device": "web"}}
	if val := withMeta.GetMeta("device"); val != "web" {
		t.Fatalf("expected device=web, got %q", val)
	}
	if val := withMeta.GetMeta("missing"); val != "" {
		t.Fatalf("expected miss to return empty, got %q", val)
	}
}

// TestToken_SetMeta 验证 SetMeta 会自动初始化 nil map 并覆盖已有值。
func TestToken_SetMeta(t *testing.T) {
	token := &Token{}
	token.SetMeta("device", "web")
	if val := token.GetMeta("device"); val != "web" {
		t.Fatalf("expected device=web after set, got %q", val)
	}
	token.SetMeta("device", "ios")
	if val := token.GetMeta("device"); val != "ios" {
		t.Fatalf("expected overwrite to ios, got %q", val)
	}
}

// TestToken_SetMetaPersistsThroughRefresh 验证 metadata 经 Create + Refresh 后仍可通过 GetMeta 读出。
func TestToken_SetMetaPersistsThroughRefresh(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, map[string]string{"device": "web"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	next, err := mgr.Refresh(context.Background(), token.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if val := next.GetMeta("device"); val != "web" {
		t.Fatalf("expected metadata to round-trip via refresh, got %q", val)
	}
}

// TestConcurrentRefresh 验证旋转模式下并发 Refresh 同一 refresh_token 仅有一个成功。
func TestConcurrentRefresh(t *testing.T) {
	_, mgr := newTestManager(t)
	token, err := mgr.Create(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	const workers = 50
	var (
		wg      sync.WaitGroup
		success atomic.Int64
	)
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			if _, err := mgr.Refresh(context.Background(), token.RefreshToken); err == nil {
				success.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if got := success.Load(); got != 1 {
		t.Fatalf("expected exactly 1 successful refresh, got %d", got)
	}
}
