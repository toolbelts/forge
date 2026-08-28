// Package dbcache 提供数据库缓存(cache-aside / read-through):
//
//   - Get(ctx, k) 先查 Store,miss 时调 Loader 回源 DB 并写回,
//     相同 key 的并发 miss 通过 singleflight 去重(只打一次 DB)。
//   - MGet(ctx, ks...) 先批量查 Store,缺失部分调 BatchLoader(若有)或并发 Loader。
//   - Set / Delete 主动写或失效;Delete 同时清掉 in-flight singleflight,
//     避免 DB 已更新但本进程仍读到旧 loader 结果。
//   - Warm 主动预热(内部走 MGet),用于启动后填充热数据。
//
// 后端三选一:
//   - NewMemoryStore(size):进程内 LRU,基于 hashicorp/golang-lru/v2/expirable。
//   - NewRedisStore(client):跨实例共享,基于 go-redis/v9;可用
//     WithRedisInvalidation(channel) 在 Delete 后广播逻辑 key。
//   - NewTieredStore(l1, l2):L1 + L2 组合,Get 走 L1 → L2 回填 L1;
//     当 Redis L2 开启失效广播时自动订阅,收到其它实例通知后清理本进程 L1。
//
// 错误语义:
//   - Loader 在数据源不存在该 key 时应返回 dbcache.ErrNotFound;
//     Cache 据此写负缓存(短 TTL)防穿透。
//   - 临时错误(连接断、超时等)原样传播,不进缓存,业务自行重试。
//
// bun 集成:
//   - dbcache.NewBun[K, V](db, opts...) 自动反射主键,用 ?PKs 占位符拼 SQL,
//     无需用户手写 Loader。要求 V 是单主键模型且已注册到 bun。
//   - 业务侧失效仍在 model 的 AfterUpdate/AfterDelete hook 里调 cache.Delete,
//     bun hook 是 model 上的方法,本包无法非侵入挂载。
//
// 多实例提示:Redis Pub/Sub 失效广播默认关闭且是 best-effort 语义;
// 断线期间的消息不会补发,其它进程的 L1 仍通过 TTL 最终收敛,不提供严格一致性。
//
// 可观测性(OTel,默认 noop,显式接入):
//   - dbcache.New 默认装 NoopMetrics{} 与 NoopTracer(),不上报任何数据。
//     业务想接 OTel:WithMetrics(dbcache.NewOTelMetrics()) / WithTracer(dbcache.NewOTelTracer()),
//     forge MetricsProvider/TraceProvider 的 metrics.enabled / trace.enabled 控制全局 noop,
//     未启用时这两个工厂返回的实现也是 noop,零开销。
//   - 指标:dbcache.hits / dbcache.misses(Counter,标签 dbcache.name);
//     dbcache.load.duration(Histogram,单位 ms,标签 dbcache.name + dbcache.status=ok|not_found|error)。
//   - 追踪:Get/MGet/Set/Delete/Warm 各启 span dbcache.<Op>;loader 回源启子 span dbcache.Loader。
//     attribute 仅记 cache 名与 key 数量,不记 key 值(防泄露)。
package dbcache
