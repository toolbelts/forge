package provider

import (
	"errors"
	"testing"

	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// TestChainProvidersAreNotPulledUp 验证读写拦截器链 / 中间件链的 Provider 不会被访问函数提前拉起：
// 它们的 Setup 结果取决于执行时机，顺序不对时应明确报找不到，而不是静默丢中间件。
func TestChainProvidersAreNotPulledUp(t *testing.T) {
	v := viper.New()
	v.Set("http.enabled", true)
	v.Set("http.addr", "127.0.0.1:0")
	httpProvider := &HttpProvider{}

	c := ioc.New()
	c.Instance(v).Instance(httpProvider).Instance(&GrpcProvider{}).Instance(&TokenProvider{})
	ctx := c.WithContext(t.Context())

	if _, err := GetHttpMux(ctx); !errors.Is(err, ioc.ErrNotFound) {
		t.Fatalf("GetHttpMux err = %v, want ErrNotFound", err)
	}
	if _, err := GetGrpcServer(ctx); !errors.Is(err, ioc.ErrNotFound) {
		t.Fatalf("GetGrpcServer err = %v, want ErrNotFound", err)
	}
	if _, err := GetTokenManager(ctx); !errors.Is(err, ioc.ErrNotFound) {
		t.Fatalf("GetTokenManager err = %v, want ErrNotFound", err)
	}
	if httpProvider.listener != nil {
		t.Fatal("HttpProvider must not be set up by an accessor")
	}
}
