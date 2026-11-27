package dbcache

import (
	"encoding/json/v2"

	"github.com/toolbelts/forge/internal/jsonopt"
)

// Codec 是值的序列化抽象。Memory 后端不会用到(直接持泛型值),
// Redis 后端必须用 Codec 把 V 编为字节再写入。
//
// 默认实现 jsonCodec 走标准库 encoding/json/v2,选项见 internal/jsonopt.Payload:
// 在 v2 语义上保留 v1 的几处行为(nil 切片写成 null、omitempty 的判定等),time.Duration 编码为 "1h30m0s" 这样的字符串,
// 保证缓存对象往返后与数据库查出来的一致,也能读回升级前写入 Redis 的数据。
// 与 v1 仍不同的地方:字段名大小写敏感、重复键报错、[N]byte 写成 base64(旧的数组形式仍可读)。
// 业务方需要 msgpack / proto 等编码,实现该接口后通过 WithCodec 注入即可。
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// jsonCodec 默认 Codec,基于 encoding/json/v2。
type jsonCodec struct{}

// Marshal 用 jsonopt.Payload 编码。
func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v, jsonopt.Payload)
}

// Unmarshal 用 jsonopt.Payload 解码。
func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v, jsonopt.Payload)
}

// defaultCodec 由 Cache 默认引用。
var defaultCodec Codec = jsonCodec{}
