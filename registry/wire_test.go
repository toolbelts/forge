package registry

import (
	"encoding/json/v2"
	"reflect"
	"testing"
)

// TestInstanceWireFormat 固定注册实例在 Redis 里的 JSON 形状:滚动发布时新旧实例要能互相发现。
func TestInstanceWireFormat(t *testing.T) {
	const stored = `{"id":"i-1","service":"user","addr":"10.0.0.1:9000"}`

	var got Instance
	if err := json.Unmarshal([]byte(stored), &got); err != nil {
		t.Fatalf("unmarshal stored instance: %v", err)
	}
	if want := (Instance{Id: "i-1", Service: "user", Addr: "10.0.0.1:9000"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if out, err := json.Marshal(&got); err != nil || string(out) != stored {
		t.Fatalf("wire format changed (err=%v): got %s, want %s", err, out, stored)
	}
}
