package lock

import "time"

type options struct {
	prefix        string
	ttl           time.Duration
	retry         int
	retryInterval time.Duration
}

// defaultOptions 给出 Manager 在未显式配置时的默认参数。
var defaultOptions = options{
	prefix:        "lock",
	ttl:           30 * time.Second,
	retry:         3,
	retryInterval: 100 * time.Millisecond,
}

// Option 定义 Manager 的可选配置。
type Option func(*options)

// WithPrefix 设置 Redis key 前缀,空串保留默认值 "lock"。
func WithPrefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// WithTtl 设置锁的 TTL,非正值保留默认值 30s。
func WithTtl(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.ttl = ttl
		}
	}
}

// WithRetry 设置 Lock 在被占时的最大额外重试次数,
// 0 表示不重试(此时 Lock 等同 TryLock),负值保留默认值 3。
func WithRetry(n int) Option {
	return func(o *options) {
		if n >= 0 {
			o.retry = n
		}
	}
}

// WithRetryInterval 设置 Lock 每次重试前的等待间隔,非正值保留默认值 100ms。
func WithRetryInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.retryInterval = d
		}
	}
}
