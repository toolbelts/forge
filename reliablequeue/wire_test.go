package reliablequeue

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestDecodeEntryNullMetadata 验证旧版本把 nil metadata 写成 "null" 的消息仍能消费,
// 且 Metadata 是可写的空 map(json/v2 会把 null 解成 nil map)。
func TestDecodeEntryNullMetadata(t *testing.T) {
	message, err := decodeEntry(redis.XMessage{Values: map[string]any{
		messageIdField: "command-1", payloadField: `{}`, metadataField: `null`,
	}})
	if err != nil {
		t.Fatalf("decode entry: %v", err)
	}
	if message.Metadata == nil {
		t.Fatal("metadata must be a non-nil map")
	}
	message.Metadata["k"] = "v" // nil map 在这里会 panic
}

// TestMessageDecodeDuration 验证业务消息体里的 time.Duration 既能读字符串,也能读旧的纳秒整数;nil 切片仍是 nil。
func TestMessageDecodeDuration(t *testing.T) {
	var target struct {
		Delay time.Duration `json:"delay"`
		Tags  []string      `json:"tags"`
	}
	for _, payload := range []string{`{"delay":"1.5s","tags":null}`, `{"delay":1500000000,"tags":null}`} {
		message := Message{Id: "m-1", Payload: []byte(payload)}
		if err := message.Decode(&target); err != nil {
			t.Fatalf("decode %s: %v", payload, err)
		}
		if target.Delay != 1500*time.Millisecond || target.Tags != nil {
			t.Fatalf("decode %s: unexpected target %+v", payload, target)
		}
	}
}
