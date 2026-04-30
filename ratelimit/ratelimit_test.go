package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/toolbelts/forge/meta"
)

// TestCompileRuleExactPath 验证 exact 模式会按字面量匹配路径。
func TestCompileRuleExactPath(t *testing.T) {
	rule := Rule{
		Name:      "users.dot",
		Key:       "ip",
		Path:      "/v1/users.json",
		PathMatch: PathMatchExact,
		Total:     1,
		Window:    "1s",
	}

	compiled, err := compileRule(rule)
	if err != nil {
		t.Fatalf("compile rule: %v", err)
	}

	if !compiled.matchPath("/v1/users.json") {
		t.Fatal("expected exact path to match")
	}
	if compiled.matchPath("/v1/usersXjson") {
		t.Fatal("expected exact path not to be treated as regex")
	}
}

// TestCompileRuleRegex 验证 regex 模式会正确编译并匹配。
func TestCompileRuleRegex(t *testing.T) {
	rule := Rule{
		Name:      "users.regex",
		Key:       "ip",
		Path:      "/v1/users/\\d+",
		PathMatch: PathMatchRegex,
		Total:     2,
		Window:    "2s",
	}

	compiled, err := compileRule(rule)
	if err != nil {
		t.Fatalf("compile rule: %v", err)
	}

	if !compiled.matchPath("/v1/users/100") {
		t.Fatal("expected regex path to match")
	}
	if compiled.matchPath("/v1/users/abc") {
		t.Fatal("expected regex path not to match")
	}
}

// TestCompileRuleInvalid 验证非法规则会被拒绝。
func TestCompileRuleInvalid(t *testing.T) {
	tests := []Rule{
		{Name: "", Key: "ip", Path: "/v1", PathMatch: PathMatchExact, Total: 1, Window: "1s"},
		{Name: "bad_key", Key: "", Path: "/v1", PathMatch: PathMatchExact, Total: 1, Window: "1s"},
		{Name: "bad_total", Key: "ip", Path: "/v1", PathMatch: PathMatchExact, Total: 0, Window: "1s"},
		{Name: "bad_window", Key: "ip", Path: "/v1", PathMatch: PathMatchExact, Total: 1, Window: "bad"},
		{Name: "bad_match", Key: "ip", Path: "/v1", PathMatch: "prefix", Total: 1, Window: "1s"},
		{Name: "bad_regex", Key: "ip", Path: "(", PathMatch: PathMatchRegex, Total: 1, Window: "1s"},
	}

	for _, rule := range tests {
		if _, err := compileRule(rule); err == nil {
			t.Fatalf("expected invalid rule %q to fail", rule.Name)
		}
	}
}

// TestCompileRuleNormalizeMethods 验证方法会统一转大写并去重。
func TestCompileRuleNormalizeMethods(t *testing.T) {
	rule := Rule{
		Name:      "methods",
		Key:       "user_id",
		Path:      "/v1/users",
		PathMatch: PathMatchExact,
		Methods:   []string{"get", "GET", " post "},
		Total:     1,
		Window:    "1s",
	}

	compiled, err := compileRule(rule)
	if err != nil {
		t.Fatalf("compile rule: %v", err)
	}

	if compiled.metaKey != meta.MetaUserId {
		t.Fatalf("expected meta key %q, got %q", meta.MetaUserId, compiled.metaKey)
	}
	if len(compiled.rule.Methods) != 2 {
		t.Fatalf("expected 2 normalized methods, got %d", len(compiled.rule.Methods))
	}
	if !compiled.matchMethod("GET") || !compiled.matchMethod("POST") {
		t.Fatal("expected normalized methods to match")
	}
}

// TestLimiterAllowNoRules 验证没有规则时直接放行。
func TestLimiterAllowNoRules(t *testing.T) {
	_, client := newTestRedis(t)
	limiter := NewLimiter(client)

	quota, err := limiter.Allow(newTestContext("/v1/users", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !quota.Allowed {
		t.Fatal("expected request to be allowed")
	}
	if quota.Remaining != -1 {
		t.Fatalf("expected remaining -1, got %d", quota.Remaining)
	}
}

// TestLimiterAllowExactAndMethod 验证精确路径和方法过滤按预期生效。
func TestLimiterAllowExactAndMethod(t *testing.T) {
	_, client := newTestRedis(t)
	limiter := NewLimiter(client)

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "ip-login",
			Key:       "ip",
			Path:      "/v1/login",
			PathMatch: PathMatchExact,
			Methods:   []string{"POST"},
			Total:     1,
			Window:    "2s",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	quota, err := limiter.Allow(newTestContext("/v1/login", "POST", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow first: %v", err)
	}
	if !quota.Allowed || quota.Remaining != 0 {
		t.Fatalf("expected first request to be allowed with remaining 0, got %+v", quota)
	}
	if quota.ResetAfter <= 0 || quota.ResetAfter > 2*time.Second {
		t.Fatalf("expected reset after within (0, 2s], got %s", quota.ResetAfter)
	}

	quota, err = limiter.Allow(newTestContext("/v1/login", "POST", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow second: %v", err)
	}
	if quota.Allowed {
		t.Fatalf("expected second request to be rejected, got %+v", quota)
	}
}

// TestLimiterAllowWindowReset 验证窗口到期后计数会自动重置。
func TestLimiterAllowWindowReset(t *testing.T) {
	mr, client := newTestRedis(t)
	limiter := NewLimiter(client)

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "ip-login",
			Key:       "ip",
			Path:      "/v1/login",
			PathMatch: PathMatchExact,
			Total:     1,
			Window:    "2s",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	if _, err := limiter.Allow(newTestContext("/v1/login", "POST", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	})); err != nil {
		t.Fatalf("allow first: %v", err)
	}

	mr.FastForward(2 * time.Second)

	quota, err := limiter.Allow(newTestContext("/v1/login", "POST", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow after reset: %v", err)
	}
	if !quota.Allowed || quota.Remaining != 0 {
		t.Fatalf("expected quota to reset after window, got %+v", quota)
	}
}

// TestLimiterAllowStrictestRule 验证多条规则命中时返回最严格配额。
func TestLimiterAllowStrictestRule(t *testing.T) {
	_, client := newTestRedis(t)
	limiter := NewLimiter(client)

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "ip-wide",
			Key:       "ip",
			Path:      "/v1/users",
			PathMatch: PathMatchExact,
			Total:     10,
			Window:    "1m",
		},
		{
			Name:      "token-tight",
			Key:       "token",
			Path:      "/v1/users",
			PathMatch: PathMatchExact,
			Total:     1,
			Window:    "1m",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	quota, err := limiter.Allow(newTestContext("/v1/users", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
		meta.MetaToken:  "token-1",
	}))
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if quota.RuleName != "token-tight" {
		t.Fatalf("expected strictest rule token-tight, got %q", quota.RuleName)
	}
	if quota.Remaining != 0 {
		t.Fatalf("expected remaining 0, got %d", quota.Remaining)
	}
}

// TestLimiterAllowRegexSharedBucket 验证正则规则会按规则维度共享计数桶。
func TestLimiterAllowRegexSharedBucket(t *testing.T) {
	_, client := newTestRedis(t)
	limiter := NewLimiter(client)

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "user-detail",
			Key:       "ip",
			Path:      "/v1/users/\\d+",
			PathMatch: PathMatchRegex,
			Total:     1,
			Window:    "5s",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	quota, err := limiter.Allow(newTestContext("/v1/users/1", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow first: %v", err)
	}
	if !quota.Allowed {
		t.Fatalf("expected first request to be allowed, got %+v", quota)
	}

	quota, err = limiter.Allow(newTestContext("/v1/users/2", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow second: %v", err)
	}
	if quota.Allowed {
		t.Fatalf("expected second request to share the same bucket, got %+v", quota)
	}
}

// TestLimiterAllowSkipMissingSubject 验证缺少限速标识时会跳过规则。
func TestLimiterAllowSkipMissingSubject(t *testing.T) {
	_, client := newTestRedis(t)
	limiter := NewLimiter(client)

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "token-only",
			Key:       "token",
			Path:      "/v1/users",
			PathMatch: PathMatchExact,
			Total:     1,
			Window:    "1s",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	quota, err := limiter.Allow(newTestContext("/v1/users", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !quota.Allowed || quota.Remaining != -1 {
		t.Fatalf("expected skipped rule to allow request, got %+v", quota)
	}
}

// TestLimiterAllowFailOpen 验证 Redis 失败时默认按 fail-open 处理。
func TestLimiterAllowFailOpen(t *testing.T) {
	mr, client := newTestRedis(t)
	limiter := NewLimiter(client)

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "ip-users",
			Key:       "ip",
			Path:      "/v1/users",
			PathMatch: PathMatchExact,
			Total:     1,
			Window:    "1s",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	mr.Close()

	quota, allowErr := limiter.Allow(newTestContext("/v1/users", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if allowErr == nil {
		t.Fatal("expected allow error")
	}
	if !quota.Allowed || quota.Remaining != -1 {
		t.Fatalf("expected fail-open result, got %+v", quota)
	}
}

// TestLimiterAllowFailClosed 验证 fail-closed 策略会在 Redis 失败时拒绝请求。
func TestLimiterAllowFailClosed(t *testing.T) {
	mr, client := newTestRedis(t)
	limiter := NewLimiter(client, WithFailurePolicy(FailurePolicyFailClosed))

	err := limiter.Reload(context.Background(), []Rule{
		{
			Name:      "ip-users",
			Key:       "ip",
			Path:      "/v1/users",
			PathMatch: PathMatchExact,
			Total:     1,
			Window:    "1s",
		},
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	mr.Close()

	quota, allowErr := limiter.Allow(newTestContext("/v1/users", "GET", map[string]any{
		meta.MetaUserIp: "127.0.0.1",
	}))
	if allowErr == nil {
		t.Fatal("expected allow error")
	}
	if quota.Allowed {
		t.Fatalf("expected fail-closed result, got %+v", quota)
	}
}

// TestRedisRuleStoreLoadRules 验证会跳过坏数据并返回有效规则。
func TestRedisRuleStoreLoadRules(t *testing.T) {
	_, client := newTestRedis(t)

	if err := client.HSet(context.Background(), "ratelimit:rules",
		"bad", "{",
		"good1", `{"name":"rule-1","key":"ip","path":"/v1/a","path_match":"exact","total":1,"window":"1s"}`,
		"good2", `{"name":"rule-2","key":"token","path":"/v1/b","path_match":"exact","total":2,"window":"2s"}`,
	).Err(); err != nil {
		t.Fatalf("seed rules: %v", err)
	}

	store := NewRedisRuleStore(client)
	rules, err := store.LoadRules(context.Background())
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected 2 valid rules, got %d", len(rules))
	}
	if rules[0].Name != "rule-1" || rules[1].Name != "rule-2" {
		t.Fatalf("expected sorted valid rules, got %+v", rules)
	}
}

// TestRedisRuleStoreSetAndDelete 验证规则写入和删除会更新 Redis Hash。
func TestRedisRuleStoreSetAndDelete(t *testing.T) {
	_, client := newTestRedis(t)
	store := NewRedisRuleStore(client)

	rule := Rule{
		Name:      "rule-1",
		Key:       "ip",
		Path:      "/v1/users",
		PathMatch: PathMatchExact,
		Total:     1,
		Window:    "1s",
	}

	if err := store.SetRule(context.Background(), rule); err != nil {
		t.Fatalf("set rule: %v", err)
	}

	values, err := client.HGetAll(context.Background(), "ratelimit:rules").Result()
	if err != nil {
		t.Fatalf("read rules after set: %v", err)
	}
	if _, ok := values["rule-1"]; !ok {
		t.Fatal("expected rule to be written into redis hash")
	}

	if err := store.DeleteRule(context.Background(), "rule-1"); err != nil {
		t.Fatalf("delete rule: %v", err)
	}

	values, err = client.HGetAll(context.Background(), "ratelimit:rules").Result()
	if err != nil {
		t.Fatalf("read rules after delete: %v", err)
	}
	if _, ok := values["rule-1"]; ok {
		t.Fatal("expected rule to be deleted from redis hash")
	}
}

// newTestContext 构造一份带请求元数据的测试上下文。
func newTestContext(path string, method string, values map[string]any) context.Context {
	ctx := context.Background()
	ctx = meta.Set(ctx, meta.MetaRequestPath, path)
	ctx = meta.Set(ctx, meta.MetaRequestMethod, method)
	for key, value := range values {
		ctx = meta.Set(ctx, key, value)
	}
	return ctx
}

// newTestRedis 创建一组 miniredis 与 go-redis 客户端用于测试。
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:         mr.Addr(),
		MaxRetries:   0,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return mr, client
}
