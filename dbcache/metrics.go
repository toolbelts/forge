package dbcache

import "time"

// Metrics 用于上报运行期统计,业务方可注入自家实现接 prometheus / otel 等。
// 所有方法应保持快速且不 panic;实现需自行处理并发。
//
// dbcache.New 默认用 NoopMetrics 不上报任何数据;欲接入 OTel 显式
// WithMetrics(dbcache.NewOTelMetrics())。
type Metrics interface {
	// Hit 缓存命中(Store 中存在,无论是真值还是负缓存)。
	Hit(name string)
	// Miss 缓存未命中,触发 Loader 回源。
	Miss(name string)
	// LoadDuration 单次 Loader 调用耗时。err 非空表示 loader 报错(含 ErrNotFound)。
	LoadDuration(name string, d time.Duration, err error)
}

// NoopMetrics 不上报任何指标的 Metrics 实现,是 dbcache.New 的默认值,
// 业务也可显式 WithMetrics(NoopMetrics{}) 覆盖外部注入回到无指标状态。
type NoopMetrics struct{}

func (NoopMetrics) Hit(string)                                {}
func (NoopMetrics) Miss(string)                               {}
func (NoopMetrics) LoadDuration(string, time.Duration, error) {}
