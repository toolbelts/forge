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
//   - NewRedisStore(client):跨实例共享,基于 go-redis/v9。
//   - NewTieredStore(l1, l2):L1 + L2 组合,Get 走 L1 → L2 回填 L1;
//     不做跨进程广播,其它实例 L1 靠 TTL 自然收敛。
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
// 多实例提示:本包不实装跨进程失效广播。Set/Delete 只影响本进程 + 共享 Store(L2);
// 其它进程的 L1 通过 TTL 自然过期。需要严格一致时另行加 Invalidator 扩展。
package dbcache
