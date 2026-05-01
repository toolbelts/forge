package dbcache

import json "github.com/goccy/go-json"

// Codec 是值的序列化抽象。Memory 后端不会用到(直接持泛型值),
// Redis 后端必须用 Codec 把 V 编为字节再写入。
//
// 默认实现 jsonCodec 走 goccy/go-json,与 stdlib encoding/json 兼容、性能更好。
// 业务方需要 msgpack / proto 等编码,实现该接口后通过 WithCodec 注入即可。
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// jsonCodec 默认 Codec,基于 goccy/go-json。
type jsonCodec struct{}

// Marshal 走 go-json。
func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal 走 go-json。
func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// defaultCodec 由 Cache 默认引用。
var defaultCodec Codec = jsonCodec{}
