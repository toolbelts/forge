package jobqueue

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	"github.com/toolbelts/forge/internal/jsonopt"
)

// TestArgsWireFormat 固定任务参数的 JSON 形状,并验证旧版本发布的任务新版本仍能消费。
func TestArgsWireFormat(t *testing.T) {
	type job struct {
		Delay time.Duration `json:"delay"`
		Tags  []string      `json:"tags"`
	}
	h, err := newHandler("t", func(ctx context.Context, j job, retries int) error { return nil })
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	// 升级前发布的 payload:time.Duration 是纳秒整数,nil 切片是 null。
	const stored = `[{"delay":1500000000,"tags":null},3]`
	values, err := h.decode([]byte(stored))
	if err != nil {
		t.Fatalf("decode stored payload: %v", err)
	}
	got := values[0].Interface().(job)
	if got.Delay != 1500*time.Millisecond || got.Tags != nil || values[1].Int() != 3 {
		t.Fatalf("unexpected args %+v, %v", got, values[1])
	}

	// 现在发布出去的 payload:time.Duration 是字符串。
	const now = `[{"delay":"1.5s","tags":null},3]`
	out, err := json.Marshal([]any{got, 3}, jsonopt.Payload)
	if err != nil || string(out) != now {
		t.Fatalf("wire format changed (err=%v): got %s, want %s", err, out, now)
	}
	values, err = h.decode(out)
	if err != nil || values[0].Interface().(job).Delay != 1500*time.Millisecond {
		t.Fatalf("decode current payload: %+v (err=%v)", values, err)
	}
}
