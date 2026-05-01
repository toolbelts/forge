package dbcache

import "time"

// Metrics 用于上报运行期统计,业务方可注入自家实现接 prometheus / otel 等。
// 所有方法应保持快速且不 panic;实现需自行处理并发。
type Metrics interface {
	// Hit 缓存命中(Store 中存在,无论是真值还是负缓存)。
	Hit(name string)
	// Miss 缓存未命中,触发 Loader 回源。
	Miss(name string)
	// LoadDuration 单次 Loader 调用耗时。err 非空表示 loader 报错(含 ErrNotFound)。
	LoadDuration(name string, d time.Duration, err error)
}

// noopMetrics 默认实现,不做任何事。
type noopMetrics struct{}

func (noopMetrics) Hit(string)                                {}
func (noopMetrics) Miss(string)                               {}
func (noopMetrics) LoadDuration(string, time.Duration, error) {}
