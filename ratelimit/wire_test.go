package ratelimit

import (
	"encoding/json/v2"
	"testing"
)

// TestRuleWireFormat 固定限流规则在 Redis 里的 JSON 形状。
func TestRuleWireFormat(t *testing.T) {
	// 升级前写入的规则:没有限定方法时 methods 是 null。
	const stored = `{"name":"login","key":"ip","path":"/v1/login","path_match":"exact","methods":null,"total":5,"window":"1m"}`

	var rule Rule
	if err := json.Unmarshal([]byte(stored), &rule); err != nil {
		t.Fatalf("unmarshal stored rule: %v", err)
	}
	if rule.Name != "login" || rule.Total != 5 || rule.Window != "1m" || len(rule.Methods) != 0 {
		t.Fatalf("unexpected rule %+v", rule)
	}

	// 现在写出的是 [](json/v2 的默认),旧实例读到的同样是"不限方法"。
	const now = `{"name":"login","key":"ip","path":"/v1/login","path_match":"exact","methods":[],"total":5,"window":"1m"}`
	if out, err := json.Marshal(rule); err != nil || string(out) != now {
		t.Fatalf("wire format changed (err=%v):\n got %s\nwant %s", err, out, now)
	}

	// 字段名大小写敏感:键写错大小写的规则读出来是零值,由 Reload 的语义校验拦下。
	var wrong Rule
	if err := json.Unmarshal([]byte(`{"Name":"login","Total":5}`), &wrong); err != nil || wrong.Name != "" || wrong.Total != 0 {
		t.Fatalf("expected mismatched-case keys to be ignored, got %+v (err=%v)", wrong, err)
	}
}
