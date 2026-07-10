package meta

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"google.golang.org/grpc/metadata"
)

type metaContextKey struct{}

type store struct {
	data map[string]any
}

// RequestMeta 是 context 中请求元数据的类型化快照。
type RequestMeta struct {
	UserAgent   string
	Method      string
	Path        string
	Uri         string
	Host        string
	Token       string
	UserId      int64
	UserType    int8
	UserIp      string
	UserCountry string
	DeviceId    string
	Language    string
	Version     string
	Platform    string
}

// Attach 将传入的 gRPC metadata 解析到 context 本地元数据存储。
func Attach(ctx context.Context) context.Context {
	if _, ok := fromContext(ctx); ok {
		return ctx
	}
	return context.WithValue(ctx, metaContextKey{}, newFromIncoming(ctx))
}

// Set 将值写入 context 本地元数据，并返回写入后的 context。
func Set(ctx context.Context, key string, value any) context.Context {
	s, ctx := ensure(ctx)
	if s.data == nil {
		s.data = make(map[string]any, 8)
	}
	s.data[key] = value
	return ctx
}

// Delete 从 context 本地元数据中删除指定值，并返回删除后的 context。
func Delete(ctx context.Context, key string) context.Context {
	s, ctx := ensure(ctx)
	delete(s.data, key)
	return ctx
}

// Has 判断当前元数据中是否存在指定 key。
func Has(ctx context.Context, key string) bool {
	_, ok := Raw(ctx, key)
	return ok
}

// Raw 返回指定 key 对应的原始值。
func Raw(ctx context.Context, key string) (any, bool) {
	return current(ctx).rawValue(key)
}

// String 将指定 key 的值读取为 string；key 缺失或值为 nil 时返回 ""。
func String(ctx context.Context, key string) string {
	return current(ctx).stringFrom(key)
}

// Int64 将指定 key 的值读取为 int64；key 缺失或转换失败时返回 0。
func Int64(ctx context.Context, key string) int64 {
	return current(ctx).int64From(key)
}

// Float64 将指定 key 的值读取为 float64；key 缺失或转换失败时返回 0。
func Float64(ctx context.Context, key string) float64 {
	return current(ctx).float64From(key)
}

// Bool 将指定 key 的值读取为 bool；key 缺失或转换失败时返回 false。
func Bool(ctx context.Context, key string) bool {
	return current(ctx).boolFrom(key)
}

// OutgoingContext 将当前元数据合并到 outgoing gRPC metadata。
func OutgoingContext(ctx context.Context) context.Context {
	s := current(ctx)
	if len(s.data) == 0 {
		return ctx
	}

	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	changed := false
	for k, v := range s.data {
		if v == nil {
			continue
		}
		md.Set(k, stringValue(v))
		changed = true
	}
	if !changed {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// Request 返回请求元数据的类型化快照。
func Request(ctx context.Context) RequestMeta {
	s := current(ctx)
	return RequestMeta{
		UserAgent:   s.stringFrom(MetaUserAgent),
		Method:      s.stringFrom(MetaRequestMethod),
		Path:        s.stringFrom(MetaRequestPath),
		Uri:         s.stringFrom(MetaRequestUri),
		Host:        s.stringFrom(MetaRequestHost),
		Token:       s.stringFrom(MetaToken),
		UserId:      s.int64From(MetaUserId),
		UserType:    s.int8From(MetaUserType),
		UserIp:      s.stringFrom(MetaUserIp),
		UserCountry: s.stringFrom(MetaUserCountry),
		DeviceId:    s.stringFrom(MetaDeviceId),
		Language:    s.stringFrom(MetaLanguage),
		Version:     s.stringFrom(MetaVersion),
		Platform:    s.stringFrom(MetaPlatform),
	}
}

// UserId 从元数据中返回当前用户 ID。
func UserId(ctx context.Context) int64 {
	return Int64(ctx, MetaUserId)
}

// Token 从元数据中返回当前访问令牌。
func Token(ctx context.Context) string {
	return String(ctx, MetaToken)
}

func fromContext(ctx context.Context) (*store, bool) {
	s, ok := ctx.Value(metaContextKey{}).(*store)
	return s, ok && s != nil
}

func current(ctx context.Context) *store {
	if s, ok := fromContext(ctx); ok {
		return s
	}
	return newFromIncoming(ctx)
}

func ensure(ctx context.Context) (*store, context.Context) {
	if s, ok := fromContext(ctx); ok {
		return s, ctx
	}
	s := newFromIncoming(ctx)
	return s, context.WithValue(ctx, metaContextKey{}, s)
}

func newFromIncoming(ctx context.Context) *store {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md) == 0 {
		return &store{}
	}
	s := &store{data: make(map[string]any, len(md))}
	for k, vs := range md {
		if len(vs) > 0 {
			s.data[k] = vs[0]
		}
	}
	return s
}

func (s *store) rawValue(key string) (any, bool) {
	if s == nil {
		return nil, false
	}
	v, ok := s.data[key]
	return v, ok
}

func (s *store) stringFrom(key string) string {
	v, ok := s.rawValue(key)
	if !ok {
		return ""
	}
	return stringValue(v)
}

func (s *store) int64From(key string) int64 {
	v, ok := s.rawValue(key)
	if !ok {
		return 0
	}
	n, _ := int64Value(v)
	return n
}

func (s *store) int8From(key string) int8 {
	v, ok := s.rawValue(key)
	if !ok {
		return 0
	}
	n, ok := int64Value(v)
	if !ok || n < -128 || n > 127 {
		return 0
	}
	return int8(n)
}

func (s *store) float64From(key string) float64 {
	v, ok := s.rawValue(key)
	if !ok {
		return 0
	}
	f, _ := float64Value(v)
	return f
}

func (s *store) boolFrom(key string) bool {
	v, ok := s.rawValue(key)
	if !ok {
		return false
	}
	b, _ := boolValue(v)
	return b
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func int64Value(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		n := uint64(val)
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		if val > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case uintptr:
		n := uint64(val)
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	case string:
		n, err := strconv.ParseInt(val, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func float64Value(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uintptr:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func boolValue(v any) (bool, bool) {
	switch val := v.(type) {
	case bool:
		return val, true
	case int:
		return val != 0, true
	case int8:
		return val != 0, true
	case int16:
		return val != 0, true
	case int32:
		return val != 0, true
	case int64:
		return val != 0, true
	case uint:
		return val != 0, true
	case uint8:
		return val != 0, true
	case uint16:
		return val != 0, true
	case uint32:
		return val != 0, true
	case uint64:
		return val != 0, true
	case uintptr:
		return val != 0, true
	case float32:
		return val != 0, true
	case float64:
		return val != 0, true
	case string:
		b, err := strconv.ParseBool(val)
		return b, err == nil
	default:
		return false, false
	}
}
