package token

import (
	"encoding/json/v2"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestTokenWireFormat 固定 Token 在 Redis 里的 JSON 形状:换 JSON 库不能让新旧实例读不了对方写的 token。
func TestTokenWireFormat(t *testing.T) {
	// 升级前写入的 payload。
	const stored = `{"access_token":"a","refresh_token":"r","user_id":7,` +
		`"access_expires":"2026-01-02T03:04:05.000000006Z","refresh_expires":"2026-02-02T03:04:05Z",` +
		`"created_at":"2026-01-01T00:00:00Z","metadata":{"device":"ios"}}`

	var got Token
	if err := json.Unmarshal([]byte(stored), &got); err != nil {
		t.Fatalf("unmarshal stored token: %v", err)
	}
	want := Token{
		AccessToken: "a", RefreshToken: "r", UserId: 7,
		AccessExpires:  time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		RefreshExpires: time.Date(2026, 2, 2, 3, 4, 5, 0, time.UTC),
		CreatedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Metadata:       map[string]string{"device": "ios"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	out, err := json.Marshal(&got)
	if err != nil || string(out) != stored {
		t.Fatalf("wire format changed (err=%v):\n got %s\nwant %s", err, out, stored)
	}

	// 没有 metadata 时整个字段省略,而不是写成 {} 或 null。
	got.Metadata = nil
	if out, _ := json.Marshal(&got); strings.Contains(string(out), "metadata") {
		t.Fatalf("empty metadata must be omitted, got %s", out)
	}
}
