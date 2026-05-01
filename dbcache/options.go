package dbcache

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTtl         = 5 * time.Minute
	defaultNegativeTtl = 30 * time.Second
	defaultJitter      = 0.1
	defaultMemSize     = 100_000
)

// options Cache 的内部配置,只读,所有字段在 New 阶段被锁定。
type options struct {
	store       Store
	batchLoader any // 由 Cache[K,V] 在构造时类型化绑定
	ttl         time.Duration
	negativeTtl time.Duration
	jitter      float64
	codec       Codec
	keyPrefix   string
	logger      zerolog.Logger
	metrics     Metrics
	tracer      trace.Tracer
}

// defaultOptions 给 New 用的默认值集合。store 留空由 New 时按需补 NewMemoryStore;
// metrics / tracer 留 nil 由 New 时按需补 NoopMetrics / NoopTracer ——
// 不在包级 var 上直接挂 noop 实例是为了让"未初始化"和"显式传 noop"两种状态都能识别。
var defaultOptions = options{
	ttl:         defaultTtl,
	negativeTtl: defaultNegativeTtl,
	jitter:      defaultJitter,
	codec:       defaultCodec,
	logger:      log.Logger,
}

// Option 调整单个 Cache 的行为。
type Option func(*options)

// WithStore 指定后端 Store。默认 NewMemoryStore(100000)。
// 跨 Cache 共享同一 Store 时,务必同时设置 WithKeyPrefix 避免 key 撞车。
func WithStore(store Store) Option {
	return func(o *options) {
		if store != nil {
			o.store = store
		}
	}
}

// WithBatchLoader 提供批量 Loader,MGet 在缺失多个 key 时优先调用它。
// 不设时 MGet 会降级为多次单 Loader 调用(并发执行)。
//
// 类型必须与 Cache[K,V] 的 BatchLoaderFunc[K,V] 一致;由 Cache 构造时校验。
// 直接调用 New 时,通过该 Option 注入;NewBun 内部已自动设置基于 ?PKs IN (?) 的批量加载。
func WithBatchLoader[K comparable, V any](fn BatchLoaderFunc[K, V]) Option {
	return func(o *options) {
		if fn != nil {
			o.batchLoader = fn
		}
	}
}

// WithTtl 设置正向缓存条目的基础 TTL。<=0 时保持默认 5min。
// 实际过期时间会按 jitter 抖动 ±jitter*ttl,以打散同批写入。
func WithTtl(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.ttl = d
		}
	}
}

// WithNegativeTtl 设置负缓存(已知不存在)条目的 TTL,通常远小于正向 TTL。
// <=0 时保持默认 30s。
func WithNegativeTtl(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.negativeTtl = d
		}
	}
}

// WithJitter 设置 TTL 抖动比例,范围 [0, 1]。默认 0.1 即 ±10%。
// 0 表示完全关闭抖动(不推荐,易触发雪崩);>1 会被裁剪到 1。
func WithJitter(ratio float64) Option {
	return func(o *options) {
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		o.jitter = ratio
	}
}

// WithCodec 替换默认 Codec(go-json),用于 Redis 后端写入前的序列化。
// Memory 后端不会调用 Codec。
func WithCodec(c Codec) Option {
	return func(o *options) {
		if c != nil {
			o.codec = c
		}
	}
}

// WithKeyPrefix 给所有 store key 加固定前缀,用于多 Cache 共享同一 Store(尤其 Redis)时的隔离。
// 末尾不会自动加分隔符,业务方按需自带 ":"。
func WithKeyPrefix(s string) Option {
	return func(o *options) {
		o.keyPrefix = s
	}
}

// WithLogger 替换默认 zerolog logger,用于运行期 warn/error 输出。
func WithLogger(l zerolog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithMetrics 注入指标采集器,记录命中/未命中/loader 耗时。
// 默认 NoopMetrics{}(不上报)。接 OTel:WithMetrics(dbcache.NewOTelMetrics())。
func WithMetrics(m Metrics) Option {
	return func(o *options) {
		if m != nil {
			o.metrics = m
		}
	}
}

// WithTracer 注入 OTel Tracer,Cache 的 Get/MGet/Set/Delete/Warm/loader 会创建 span。
// 默认 NoopTracer()(不产 span)。接 OTel:WithTracer(dbcache.NewOTelTracer())。
func WithTracer(t trace.Tracer) Option {
	return func(o *options) {
		if t != nil {
			o.tracer = t
		}
	}
}
