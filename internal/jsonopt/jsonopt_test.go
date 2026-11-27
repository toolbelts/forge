package jsonopt

import (
	"encoding/json/v2"
	"reflect"
	"strings"
	"testing"
	"time"
)

// model 模拟业务方的缓存对象 / 队列参数:标签是按 v1 语义写的。
type model struct {
	Id      int64             `json:"id"`
	Name    string            `json:"name,omitempty"`
	Count   int               `json:"count,omitempty"`
	Tags    []string          `json:"tags"`
	Meta    map[string]string `json:"meta"`
	Raw     []byte            `json:"raw"`
	Hash    [4]byte           `json:"hash"`
	Timeout time.Duration     `json:"timeout"`
	At      time.Time         `json:"at"`
	Note    *string           `json:"note,omitempty"`
}

// TestPayloadReadsLegacyData 验证升级前(v1 语义)写进 Redis 的数据仍能读回:
// 那时 time.Duration 是纳秒整数,[N]byte 是数字数组,nil 切片 / map 是 null。
func TestPayloadReadsLegacyData(t *testing.T) {
	const stored = `{"id":1,"tags":null,"meta":null,"raw":null,"hash":[1,2,3,4],"timeout":1500000000,"at":"2026-01-02T03:04:05.000000006Z","note":""}`

	var got model
	if err := json.Unmarshal([]byte(stored), &got, Payload); err != nil {
		t.Fatalf("unmarshal legacy data: %v", err)
	}
	empty := ""
	want := model{
		Id: 1, Hash: [4]byte{1, 2, 3, 4}, Timeout: 1500 * time.Millisecond,
		At: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), Note: &empty,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// 不带 Payload 时这份数据读不回来,说明选项确实在起作用。
	if err := json.Unmarshal([]byte(stored), &got); err == nil {
		t.Fatal("expected v2 defaults to reject the legacy representation")
	}
}

// TestPayloadRoundTripKeepsValue 验证往返后值不变:nil 仍是 nil,指向空串的指针不会变成 nil。
func TestPayloadRoundTripKeepsValue(t *testing.T) {
	empty := ""
	in := model{Id: 1, Timeout: time.Second, At: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Note: &empty}

	data, err := json.Marshal(in, Payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"id":1,"tags":null,"meta":null,"raw":null,"hash":"AAAAAA==","timeout":"1s","at":"2026-01-02T03:04:05Z","note":""}`
	if string(data) != want {
		t.Fatalf("wire format changed:\n got %s\nwant %s", data, want)
	}

	var out model
	if err := json.Unmarshal(data, &out, Payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip changed the value: got %+v, want %+v", out, in)
	}

	// 对照:不带 Payload 时 v2 没有 time.Duration 的默认表示,直接报错。
	if _, err := json.Marshal(in); err == nil {
		t.Fatal("expected v2 defaults to reject time.Duration")
	}
}

// TestPayloadDurationAsString 验证 time.Duration 在各种位置上都编码为字符串,并且读的时候足够宽容。
func TestPayloadDurationAsString(t *testing.T) {
	type schedule struct {
		Every   time.Duration            `json:"every"`
		Timeout *time.Duration           `json:"timeout"`
		Grace   *time.Duration           `json:"grace"`
		Backoff []time.Duration          `json:"backoff"`
		Limits  map[string]time.Duration `json:"limits"`
	}
	timeout := 1500 * time.Millisecond
	in := schedule{
		Every: 90 * time.Minute, Timeout: &timeout,
		Backoff: []time.Duration{time.Second, 0}, Limits: map[string]time.Duration{"read": time.Minute},
	}

	data, err := json.Marshal(in, Payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"every":"1h30m0s","timeout":"1.5s","grace":null,"backoff":["1s","0s"],"limits":{"read":"1m0s"}}`
	if string(data) != want {
		t.Fatalf("wire format changed:\n got %s\nwant %s", data, want)
	}
	var out schedule
	if err := json.Unmarshal(data, &out, Payload); err != nil || !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip changed the value: got %+v, want %+v (err=%v)", out, in, err)
	}

	for name, tc := range map[string]struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		"手写的简写":  {in: `"1h30m"`, want: 90 * time.Minute},
		"旧的纳秒整数": {in: `1500000000`, want: 1500 * time.Millisecond},
		"null":   {in: `null`, want: 0},
		"非法字符串":  {in: `"soon"`, wantErr: true},
		"其它类型":   {in: `true`, wantErr: true},
	} {
		var got struct {
			Every time.Duration `json:"every"`
		}
		err := json.Unmarshal([]byte(`{"every":`+tc.in+`}`), &got, Payload)
		if (err != nil) != tc.wantErr || got.Every != tc.want {
			t.Errorf("%s: got %v, err=%v; want %v, wantErr=%v", name, got.Every, err, tc.want, tc.wantErr)
		}
	}
}

// TestPayloadAllowsInvalidUtf8 验证业务字符串里的非法 UTF-8 不会让编码失败。
func TestPayloadAllowsInvalidUtf8(t *testing.T) {
	data, err := json.Marshal(map[string]string{"name": "a\xffb"}, Payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), "a\ufffdb") {
		t.Fatalf("expected the invalid byte to be replaced, got %s", data)
	}
}

// TestPayloadKeepsV2Strictness 记录仍然沿用 v2 语义的两处:字段名大小写敏感、重复键报错。
func TestPayloadKeepsV2Strictness(t *testing.T) {
	var m model
	if err := json.Unmarshal([]byte(`{"ID":7,"Name":"x"}`), &m, Payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.Id != 0 || m.Name != "" {
		t.Fatalf("names must match case-sensitively, got %+v", m)
	}
	if err := json.Unmarshal([]byte(`{"id":1,"id":2}`), &m, Payload); err == nil {
		t.Fatal("expected duplicate names to be rejected")
	}
}
