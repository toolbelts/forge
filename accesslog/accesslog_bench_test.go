package accesslog

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
)

// BenchmarkMarshalPayload 对比 payload 序列化各路径的成本:
// large_1m_skip vs large_1m_full 体现 wire size 预检对大消息的收益(应有数量级差);
// mask_miss vs mask_hit 体现 probe 预检短路 JSON 树往返的收益。
func BenchmarkMarshalPayload(b *testing.B) {
	small := &statuspb.Status{Code: 42, Message: strings.Repeat("x", 1000)}
	large := &statuspb.Status{Code: 42, Message: strings.Repeat("x", 1<<20)}

	cases := []struct {
		name string
		msg  *statuspb.Status
		opts []Option
	}{
		{"small_2k", small, nil},
		// 默认自动阈值 8KB,1MB 消息被预检拦截,只付 proto.Size 成本。
		{"large_1m_skip", large, nil},
		// maxBytes<0 关闭预检与截断,对照旧实现的全量序列化成本。
		{"large_1m_full", large, []Option{WithPayload(true, -1)}},
		// mask 配置了但 payload 不含该 key,probe 预检短路,免去 Unmarshal/Marshal 往返。
		{"mask_miss", small, []Option{WithMaskFields([]string{"password"})}},
		// mask 命中,走完整 JSON 树往返,是 mask_miss 的成本对照。
		{"mask_hit", small, []Option{WithMaskFields([]string{"message"})}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			o := buildOptions(tc.opts...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = marshalPayload(tc.msg, &o)
			}
		})
	}
}

// BenchmarkUnaryInterceptor_ProtoPayload 端到端成本(含 zerolog 编码输出到 io.Discard),
// 体现 req/resp 从 Str 双重转义换成 RawJSON 直插后的整行收益。
func BenchmarkUnaryInterceptor_ProtoPayload(b *testing.B) {
	logger := zerolog.New(io.Discard)
	ctx := logger.WithContext(context.Background())
	interceptor := UnaryInterceptor(WithPayload(true, 0))
	info := &grpc.UnaryServerInfo{FullMethod: "/svc.Foo/Bar"}
	msg := &statuspb.Status{Code: 42, Message: strings.Repeat("x", 1000)}
	handler := func(ctx context.Context, _ any) (any, error) { return msg, nil }

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = interceptor(ctx, msg, info, handler)
	}
}
