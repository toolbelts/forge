package registry

import "time"

type options struct {
	prefix          string
	ttl             time.Duration
	heartbeat       time.Duration
	resolveInterval time.Duration
}

// defaultOptions 给出 Manager 在未显式配置时的默认参数。
// heartbeat 严格小于 ttl 的 1/3,确保单次心跳失败仍有重试机会。
var defaultOptions = options{
	prefix:          "registry",
	ttl:             15 * time.Second,
	heartbeat:       5 * time.Second,
	resolveInterval: 5 * time.Second,
}

// Option 定义 Manager 的可选配置。
type Option func(*options)

// WithPrefix 设置 Redis key 前缀,空串保留默认值 "registry"。
func WithPrefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// WithTtl 设置实例 TTL,非正值保留默认值 15s。
func WithTtl(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.ttl = ttl
		}
	}
}

// WithHeartbeat 设置心跳/续约周期,非正值保留默认值 5s。
// 调用方应保证 heartbeat < ttl/3,否则单次心跳失败即判死。
func WithHeartbeat(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.heartbeat = d
		}
	}
}

// WithResolveInterval 设置 Resolve/Watch 周期轮询间隔,非正值保留默认值 5s。
func WithResolveInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.resolveInterval = d
		}
	}
}
