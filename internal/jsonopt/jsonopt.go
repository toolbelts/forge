// Package jsonopt 集中 forge 使用 encoding/json/v2 时的选项约定。
//
// forge 内部自己定义、自己读写的结构(token、注册中心实例、限流规则等)直接用 v2 的默认语义。
// 只有"类型由业务方决定"的数据才用 Payload:缓存里的业务对象、任务队列的参数、可靠队列的消息体。
// 这些类型是按 v1 的语义写的,Redis 里也已经有 v1 写下的数据,所以要显式抹平几处会丢数据或直接报错的差异。
package jsonopt

import (
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"reflect"
	"time"
)

// Payload 是编解码业务方类型时使用的选项。除下面几项外,其余都是 v2 的默认语义
// (字段名大小写敏感、拒绝重复键、不转义 HTML 字符、map 的键不排序)。
var Payload = json.JoinOptions(
	// nil 切片 / map 写成 null 而不是 [] / {}:读回来仍是 nil,缓存里的对象才和数据库查出来的一致。
	json.FormatNilSliceAsNull(true),
	json.FormatNilMapAsNull(true),
	// time.Duration 在 v2 里没有默认表示,直接报错(这个版本也不接受 format 标签);
	// 统一编码为 "1h30m0s" 这样的字符串,读的时候同时接受升级前的纳秒整数。
	json.WithMarshalers(json.MarshalToFunc(marshalDuration)),
	json.WithUnmarshalers(json.UnmarshalFromFunc(unmarshalDuration)),
	// 下面两项只在 v1 包里提供,所以这里是全仓库唯一引用 encoding/json 的地方。
	// [N]byte 在 v1 里是数字数组,v2 默认只认 base64 字符串;打开后旧数据的数组形式仍能读回。
	jsonv1.FormatBytesWithLegacySemantics(true),
	// v2 的 omitempty 按"编码结果是否为空"判断:指向空串的 *string 会被省略,读回来变成 nil,
	// 等于把数据库里的空串变成了 NULL。沿用 v1 的定义(零值、nil、空容器才省略)。
	jsonv1.OmitEmptyWithLegacySemantics(true),
	// 业务字符串里的非法 UTF-8 在 v2 默认会让整个编码失败;放宽为替换成 U+FFFD,与 v1 一致。
	jsontext.AllowInvalidUTF8(true),
)

// marshalDuration 把 time.Duration 编码为 time.Duration.String 的格式,例如 "1h30m0s"。
func marshalDuration(enc *jsontext.Encoder, d time.Duration) error {
	return enc.WriteToken(jsontext.String(d.String()))
}

// unmarshalDuration 接受 time.ParseDuration 能解析的字符串,以及升级前写下的纳秒整数。
func unmarshalDuration(dec *jsontext.Decoder, d *time.Duration) error {
	tok, err := dec.ReadToken()
	if err != nil {
		return err
	}
	switch tok.Kind() {
	case '"':
		*d, err = time.ParseDuration(tok.String())
		return err
	case '0':
		nanos, err := tok.Int()
		*d = time.Duration(nanos)
		return err
	case 'n':
		*d = 0
		return nil
	default:
		return &json.SemanticError{JSONKind: tok.Kind(), GoType: reflect.TypeFor[time.Duration]()}
	}
}
