# forge/provider

把 forge 各子库 (redis、gRPC、限速、token 等) 封装成 `Provider`,由 `ioc.App` 统一编排:配置驱动、生命周期一致、按需启用。

接入只需三步:写 yaml → `App.Use(...)` 按推荐顺序串起来 → `App.Run(ctx, nil)` 起服务。

---

## 1. 概述

每个 Provider 实现 `Register(ctx) error` + `Setup(ctx) error`,按需再实现 `Serve(ctx) error` / `Shutdown(ctx) error`。`ioc.App.Run(ctx, fn)` 内部按以下顺序跑:

1. `RegisterAll` — 按 `Use` 顺序逐个 Register,只读 viper / 不依赖其它 Provider 已 Setup 的产物
2. `SetupAll` — 同序逐个 Setup,可从容器取上一阶段已注入的实例
3. `Serve`(`fn == nil`) 或 `fn(ctx)`(CLI 模式) — Serve 模式下所有 `Server` 并发跑,任一退出即触发收敛
4. `ShutdownAll` — 已成功 Setup 的 Provider 反向(LIFO)执行 Shutdown

任一阶段返回 error 立即收敛:Register 失败放弃启动,Setup 失败对已 Setup 的依赖反向 Shutdown,避免半启动状态。`SIGINT` / `SIGTERM` 通过 ctx 传给 fn / Serve(详见 `ioc/app.go`)。

**统一约定**

- 配置键全部从单一 viper 实例读取(`ConfigProvider` 注入)
- 多数 Provider 用 `<name>.enabled` 控制总开关。`enabled=false` 时:
  - **不向容器注入实例** — 业务方调 `MustGet*` 会 panic(`TokenProvider`、`RateLimitProvider`、`LockProvider`、`CronProvider`、`JobQueueProvider`、`MetricsProvider`、`TraceProvider`、`GrpcProvider`、`HttpProvider`、`GatewayProvider`、`PprofProvider`、`MessageProvider`)。这是有意为之 —— 关闭某能力时不应允许业务方依赖它
  - **拦截器 Provider** 不挂拦截器(`AccessLogProvider`、`RateLimitProvider`、`TokenProvider`)
  - **始终启用、无 `enabled` 字段**:`BuildProvider`、`ConfigProvider`、`LoggerProvider`、`RecoveryProvider`、`ErrorProvider`、`ValidateProvider`、`MigrationProvider`;`RedisProvider` / `DatabaseProvider` 按 yaml `redis.<name>` / `database.<name>` map 是否非空自动判断
  - `TcpProvider` 有 `enabled` 但不向容器注入任何实例,业务方反过来用 `MustSetTcpHandler` 注入 `TcpHandler`
  - **`NotifyProvider`**:无 `enabled`,所有通道留空时使用 noop 实现,`MustGetNotifier` 仍可获取
- Redis 派生 Provider 默认从名为 `default` 的 redis 实例取客户端,可通过 `<name>.redis` 切换其它实例

---

## 2. 推荐编排顺序

`App.Use(...)` 调用顺序决定两件事:

1. **Register 阶段顺序** — 所有 Register 完成后才进入 Setup,但 Register 内部读取的容器实例必须在更早的 Provider 已注入。例:启用 instrumentation 时,`MetricsProvider`/`TraceProvider` 必须在 `RedisProvider` / `DatabaseProvider` 之前 — 后两者在 Register 时挂 OTel hook,需要全局 MeterProvider/TracerProvider 已就绪
2. **Setup 阶段顺序** — `InterceptorChain.Use` 调用是 Setup 阶段的 side effect,而 `GrpcProvider.Setup` 读 chain 来构造 `*grpc.Server`。所以**所有拦截器 Provider 必须在 `GrpcProvider` 之前 Use**,否则 chain 在 Grpc 构造 server 时还是空的

LIFO Shutdown 自动反向,无需手动管理。

### A. 基础(必装,一开始就 Use)

| # | Provider | 说明 |
|---|---|---|
| 1 | `BuildProvider` | 注入 `*BuildInfo`。**必须在** Trace/Metrics/Notify 之前(它们的 Register 读 `MustGetBuildInfo`) |
| 2 | `ConfigProvider` | viper + .env 加载,所有后续 Provider 的前置 |
| 3 | `LoggerProvider` | zerolog 全局 logger,读 `log.level` 并热重载 |
| 4 | `MetricsProvider` | OTel MeterProvider。启用 metrics instrumentation 时,必须早于 Redis/Database/Gateway/JobQueue 等会挂 hook 的 Provider |
| 5 | `TraceProvider` | OTel TracerProvider + Propagator。启用 trace instrumentation 时,必须早于 Redis/Database/Gateway 等会挂 hook 的 Provider |
| 6 | `NotifyProvider` | 通知器,被 `RecoveryProvider.Setup` 引用。本身实现 `Server`:所有 Provider Setup 完成后发 "started",Shutdown 阶段(LIFO 最末)发 "stopped" — 所以位置要靠前 |

### B. 数据层(按需)

| # | Provider | 说明 |
|---|---|---|
| 7 | `RedisProvider` | 多实例 redis 客户端。`Register` 阶段就把客户端注入容器;按 instrumentation 配置决定是否挂 redisotel hook |
| 8 | `DatabaseProvider` | 多实例 `*bun.DB`;按 instrumentation 配置决定是否挂 bunotel query hook |
| 9 | `MigrationProvider` | 仅迁移子命令路径下注册,正常 serve 不挂(参见 `migration.go` 顶部注释) |
| 10 | `LockProvider` | 分布式锁工厂(Setup 时取 Redis 客户端,顺序无强约束) |
| 11 | `CronProvider` | 定时任务调度器 |
| 12 | `JobQueueProvider` | redis 任务队列。**Register** 阶段就调 `MustGetRedis`,所以必须在 `RedisProvider` 之后 |
| 13 | `DbcacheProvider` | 数据库缓存 Store 工厂。redis / tiered 后端 **Register** 阶段调 `GetRedis`,所以必须在 `RedisProvider` 之后 |
| 14 | `MessageProvider` | 邮件 / 短信路由,无外部依赖 |

### C. gRPC 拦截器(按链外→内顺序 Use,**全部放在 GrpcProvider 之前**)

`InterceptorChain.Use` 是追加,**Use 顺序 = 外层执行顺序**(见 `provider/interceptor.go:13`)。把下列 Provider 严格按此序 Use:

| # | Provider | 拦截器位置 | 说明 |
|---|---|---|---|
| 15 | `RecoveryProvider` | 最外层 | 必须最先 recover panic,异步推送告警。Setup 读 `MustGetNotifier` |
| 16 | `AccessLogProvider` | 内一层 | 看到的 err 已被 Recovery 兜底成 `errkit.Error` |
| 17 | `ErrorProvider` | 内二层 | 把裸 error / `context.*` / `status.Status` 归一化为 `errkit.Error` |
| 18 | `RateLimitProvider` | 内三层 | Setup 同时构造 `*RedisRateLimiter` 实例 + 挂拦截器。在 protovalidate 之前砍 CPU |
| 19 | `ValidateProvider` | 内四层 | protovalidate,纯内存 CEL |
| 20 | `TokenProvider` | 最内层 | Setup 同时构造 `*token.Manager` 实例 + 挂拦截器。鉴权要查 Redis,放最后让畸形请求先被前面砍掉 |

> 即使不启用 gRPC,`InterceptorChain` 也是在 `GrpcProvider.Register` 阶段创建的 — 所以 `GrpcProvider` 必须 Use(可以 `enabled=false`),否则 ioc.MustGet 会 panic。

### D. 服务端(按需)

| # | Provider | 说明 |
|---|---|---|
| 21 | `GrpcProvider` | `Register` 抢端口 + 注入 `*InterceptorChain`;`Setup` 阶段读 chain 构造 `*grpc.Server`。**所有拦截器 Provider 必须 Use 在它之前** |
| 22 | `HttpProvider` | `*http.ServeMux` 注入容器 |
| 23 | `PprofProvider` | 复用 HttpProvider 的 mux,仅当 `pprof.enabled && http.enabled` 才挂 |
| 24 | `GatewayProvider` | grpc-gateway,反向 dial gRPC 后端 |
| 25 | `TcpProvider` | 通用 TCP accept loop,业务方通过 `MustSetTcpHandler(ctx, h)` 注入 `TcpHandler` |

### E. 注册中心(按需)

| # | Provider | 说明 |
|---|---|---|
| 26 | `RegistryProvider` | `Setup` 阶段读 `MustGetGrpcListener` 的实际端口注册当前实例。同时全局注册 `redis://` resolver,供 client 端 `grpc.NewClient("redis:///<service>")` 解析 |

---

## 3. 拦截器链顺序速查

```
Recovery → AccessLog → Error → RateLimit → Validate → Token → 业务
(最外层)                                                  (最内层)
```

`Use` 顺序 = 链表追加顺序 = 拦截器外→内执行顺序。原文见 `provider/interceptor.go:13`。

---

## 4. 配置参考

每条罗列:键、类型、默认值、是否必填。以源码 `v.GetXxx("...")` 调用为准。

### `app.name`(顶层)

`RegistryProvider.Setup` 用于自注册的 `service` 名(`v.GetString("app.name")`),与 `ConfigProvider` 的 `appName` 参数无关 — 后者只决定加载哪个 yaml,前者才进入注册中心。建议两者保持一致。

### ConfigProvider(代码层 Option)

| 项 | 默认值 | 说明 |
|---|---|---|
| `WithConfigDirs(...)` | `["./configs", "."]` | 配置搜索目录列表 |
| `WithConfigNames(...)` | `["common", "message", appName]` | yaml 文件名(不含扩展名),按顺序 merge,后者覆盖前者 |
| `WithEnvFiles(...)` | `[".env", ".env.local"]` | godotenv 加载顺序,后者覆盖前者;文件不存在不报错 |

`AutomaticEnv` + `.` → `_` 的 key 替换始终启用。例:`redis.default.addr` 可被 `REDIS_DEFAULT_ADDR` 环境变量覆盖。

### LoggerProvider — `log.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `level` | string | `info` | `trace` / `debug` / `info` / `warn` / `error` / `fatal` / `panic`。无效或为空回退到 `info`。配置文件改动会触发 `WatchConfig` 热更新 |

### MetricsProvider — `metrics.*`

| 键 | 类型 | 默认值 | 必填 | 说明 |
|---|---|---|---|---|
| `enabled` | bool | `false` | — | 关闭时全局保留 OTel noop meter,业务代码与开关解耦 |
| `endpoint` | string | — | ✓ | OTLP HTTP collector URL |
| `timeout` | duration | `5s` | — | exporter 单次请求超时 |
| `interval` | duration | `30s` | — | PeriodicReader 上报周期 |
| `headers` | map | — | — | OTLP 请求头(认证 token 等) |
| `env` | string | — | — | resource 上的 `env` 标签 |
| `shutdown_timeout` | duration | `5s` | — | 关停时等待 reader 清空缓冲 |

metrics 自动插桩配置:

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `instrumentation.enabled` | bool | `metrics.enabled` | metrics 自动插桩总开关 |
| `instrumentation.redis` | bool | 上一行结果 | 覆盖 RedisProvider metrics hook |
| `instrumentation.database` | bool | 上一行结果 | 覆盖 DatabaseProvider bunotel metrics hook |
| `instrumentation.grpc` | bool | 上一行结果 | 覆盖 GrpcProvider otelgrpc server metrics hook |
| `instrumentation.gateway` | bool | 上一行结果 | 覆盖 GatewayProvider otelgrpc client metrics hook |
| `instrumentation.jobqueue` | bool | 上一行结果 | 覆盖 JobQueueProvider metrics |

### TraceProvider — `trace.*`

| 键 | 类型 | 默认值 | 必填 | 说明 |
|---|---|---|---|---|
| `enabled` | bool | `false` | — | 关闭时全局保留 noop tracer |
| `endpoint` | string | — | ✓ | OTLP HTTP collector URL |
| `timeout` | duration | `5s` | — | exporter 超时 |
| `sample_ratio` | float | `1.0` | — | `ParentBased(TraceIDRatioBased)` |
| `headers` | map | — | — | OTLP 请求头 |
| `env` | string | — | — | resource 上的 `env` 标签 |
| `shutdown_timeout` | duration | `5s` | — | 关停超时 |

trace 自动插桩配置:

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `instrumentation.enabled` | bool | `trace.enabled` | trace 自动插桩总开关 |
| `instrumentation.redis` | bool | 上一行结果 | 覆盖 RedisProvider trace hook |
| `instrumentation.database` | bool | 上一行结果 | 覆盖 DatabaseProvider bunotel trace hook |
| `instrumentation.grpc` | bool | 上一行结果 | 覆盖 GrpcProvider otelgrpc server trace hook |
| `instrumentation.gateway` | bool | 上一行结果 | 覆盖 GatewayProvider otelgrpc client trace hook 与 HTTP traceparent 提取 |

> Metrics / Trace 的 `service.name`、`service.version`、`service.instance.id` 均来自 `BuildInfo` + `AppName`,无需重复在 yaml 配。

### NotifyProvider — `notify.*`

| 键 | 类型 | 说明 |
|---|---|---|
| `telegram.token` | string | Bot token,空字符串视为关闭 telegram 通道 |
| `telegram.chat_id` | string | 必须与 token 配对 |
| `lark.webhook` | string | webhook URL,空字符串视为关闭 lark 通道 |

两个通道全空时使用 noop notifier,`MustGetNotifier` 仍可获取(返回 noop 实现,Send 直接成功)。

### RedisProvider — `redis.<name>.*`

每个 `<name>` 是一份独立的客户端实例,通过 `MustGetRedis(ctx, "<name>")` 获取。约定使用 `default` 作为主实例名。

| 键 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `addr` | string | ✓ | — | `host:port` |
| `password` | string | — | — | |
| `db` | int | — | `0` | |
| `pool_size` | int | — | go-redis 默认 | `>0` 时覆盖 |
| `min_idle_conns` | int | — | go-redis 默认 | |
| `max_idle_conns` | int | — | go-redis 默认 | |
| `dial_timeout` | duration | — | go-redis 默认 | |
| `read_timeout` | duration | — | go-redis 默认 | |
| `write_timeout` | duration | — | go-redis 默认 | |
| `pool_timeout` | duration | — | go-redis 默认 | |

启动时会 `Ping`,失败立即返回错误。Redis trace/metrics hook 默认分别跟随 `trace.enabled` / `metrics.enabled`,也可用 `trace.instrumentation.redis` / `metrics.instrumentation.redis` 单独覆盖。trace hook 默认关闭 Redis DB statement 和 caller file/line,避免每条命令采集调用栈。

### DatabaseProvider — `database.<name>.*`

每个 `<name>` 是一份独立 `*bun.DB`。

| 键 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `dsn` | string | ✓ | PostgreSQL DSN |
| `max_open_conns` | int | — | |
| `max_idle_conns` | int | — | |
| `conn_max_lifetime` | duration | — | |
| `conn_max_idle_time` | duration | — | |
| `slow` | duration | — | 超过阈值的查询用 zerolog warn 记录 |

启动时 `PingContext`,失败立即返回错误。bunotel query hook 默认在 `trace.enabled` 或 `metrics.enabled` 任一开启时挂载,也可用 `trace.instrumentation.database` / `metrics.instrumentation.database` 单独覆盖。

### GrpcProvider — `grpc.*`

| 键 | 类型 | 必填(enabled 时) | 说明 |
|---|---|---|---|
| `enabled` | bool | — | |
| `addr` | string | ✓ | `host:port`。配 `:0` 走内核选随机端口,RegistryProvider 会从 listener 读实际端口 |
| `max_recv_msg_size` | int | — | `>0` 时覆盖 grpc 默认 |
| `max_send_msg_size` | int | — | 同上 |
| `shutdown_timeout` | duration | — | 超过此值后从 GracefulStop 升级到 Stop |

otelgrpc server stats handler 默认在 `trace.enabled` 或 `metrics.enabled` 任一开启时挂载,也可用 `trace.instrumentation.grpc` / `metrics.instrumentation.grpc` 单独覆盖。

### HttpProvider — `http.*`

| 键 | 类型 | 必填(enabled 时) | 说明 |
|---|---|---|---|
| `enabled` | bool | — | |
| `addr` | string | ✓ | |
| `read_timeout` | duration | — | |
| `read_header_timeout` | duration | — | |
| `write_timeout` | duration | — | |
| `idle_timeout` | duration | — | |
| `shutdown_timeout` | duration | — | |

### GatewayProvider — `gateway.*`

| 键 | 类型 | 必填(enabled 时) | 说明 |
|---|---|---|---|
| `enabled` | bool | — | |
| `addr` | string | ✓ | gateway 监听地址 |
| `grpc_endpoint` | string | ✓ | gateway → gRPC 后端地址,通常是同进程的 `grpc.addr` |
| `read_timeout` / `read_header_timeout` / `write_timeout` / `idle_timeout` / `shutdown_timeout` | duration | — | 同 HttpProvider |

JSON 编码固定使用 proto 字段名(`UseProtoNames=true`),枚举输出字符串,默认值输出到响应体。

otelgrpc client stats handler 默认在 `trace.enabled` 或 `metrics.enabled` 任一开启时挂载,也可用 `trace.instrumentation.gateway` / `metrics.instrumentation.gateway` 单独覆盖。HTTP `traceparent` 提取跟随 `trace.instrumentation.gateway`。

### TcpProvider — `tcp.*`

| 键 | 类型 | 必填(enabled 时) | 说明 |
|---|---|---|---|
| `enabled` | bool | — | |
| `addr` | string | ✓ | `host:port` |

业务方在自己 Setup 里调 `provider.MustSetTcpHandler(ctx, myHandler)` 注入 `TcpHandler`(`HandleConn(ctx, net.Conn)`)。`Serve` 阶段做 accept loop 派发。

### PprofProvider — `pprof.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 实际生效需同时满足 `http.enabled=true` |
| `path_prefix` | string | `/debug/pprof/` | 挂载前缀 |
| `mutex_fraction` | int | `100`(1/100 抽样) | `runtime.SetMutexProfileFraction`,显式设 `0` 关闭 |
| `block_rate` | int | `10000`(≥10µs 阻塞才采样) | `runtime.SetBlockProfileRate`,显式设 `0` 关闭 |

⚠️ pprof 与业务 HTTP 共端口,生产环境通过反向代理 / ACL 屏蔽路径,或 `pprof.enabled=false`。

ℹ️ `threadcreate` profile 路由由 Go `runtime/pprof` 默认注册,但因 [golang/go#6104](https://github.com/golang/go/issues/6104) 数据始终为空,与本框架无关。

### TokenProvider — `token.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `redis` | string | `default` | redis 实例名 |
| `prefix` | string | `token`(`token` 包默认) | redis key 前缀 |
| `access_ttl` | duration | `token` 包默认 | 逻辑过期 |
| `access_save_ttl` | duration | `token` 包默认 | 物理过期(>access_ttl) |
| `refresh_ttl` | duration | `token` 包默认 | |
| `refresh_save_ttl` | duration | `token` 包默认 | |
| `refresh_rotation` | bool | `true` | **必须用 `IsSet` 守卫**,否则 GetBool 在键缺失时返回 false 会误关默认开启的旋转策略 |
| `skips` | []string | — | 跳过鉴权的 HTTP path 或 gRPC FullMethod 列表(/v1/auth/login 或 /api.Auth/Login,任一命中即放行) |

### RateLimitProvider — `ratelimit.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `redis` | string | `default` | |
| `prefix` | string | `ratelimit` 包默认 | |
| `reload_period` | duration | `30s`(包默认) | 后台周期重载规则 |
| `failure_policy` | string | `ratelimit` 包默认 | `fail_open` / `fail_closed`,Redis 不可用时放行还是拒绝 |

规则在 redis 中维护,运行期可通过 `MustGetRateLimiter(ctx).SetRule(...)` 动态调整。

### RegistryProvider — `registry.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `redis` | string | `default` | |
| `prefix` | string | `registry` 包默认 | redis key 前缀 |
| `ttl` | duration | `registry` 包默认 | 实例 TTL |
| `heartbeat` | duration | `registry` 包默认(`<ttl/3`) | 续租周期 |
| `resolve_interval` | duration | `registry` 包默认 | resolver 周期轮询 |
| `advertise_host` | string | 推导 | 优先取此值,其次 `grpc.addr` 的 host 段(非 `0.0.0.0`/`::`),再次取本机 RFC1918 私有 IPv4 |
| `advertise_port` | int | 推导 | 优先取此值,其次 grpc listener 实际端口(支持 `grpc.addr=":0"` 场景) |

`enabled=true` 且 `grpc.enabled=true` 时自动用 `BuildInfo.InstanceId()` 注册当前实例;Shutdown 主动 Deregister(2s 超时),进程被 `kill -9` 时靠 TTL 兜底。

### LockProvider — `lock.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `redis` | string | `default` | |
| `prefix` | string | `lock` 包默认 | |
| `ttl` | duration | `lock` 包默认 | 锁 TTL,后台按 `ttl/3` 续租 |
| `retry` | int | `lock` 包默认 | **必须用 `IsSet` 守卫**,否则 GetInt 缺失时返回 0 会误关默认重试 |
| `retry_interval` | duration | `lock` 包默认 | 指数退避基础间隔 |

### CronProvider — `cron.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `timezone` | string | `Local` | 解析失败立即返回错误,避免 prod 时区错配 |
| `shutdown_timeout` | duration | `30s` | 等待运行中任务结束 |

`Setup` 不读任何业务任务 — 业务方在自己的 `Setup` 里 `MustGetCron(ctx).AddJob(...)`。

### JobQueueProvider — `jobqueue.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `redis` | string | `default` | |
| `key_prefix` | string | — | redis key 前缀 |
| `shutdown_timeout` | duration | `30s` | 等待 worker 收尾 |

业务方在自己的 `Setup` 里 `MustGetJobQueue(ctx).Subscribe(topic, fn)`。
JobQueue OTel metrics 默认跟随 `metrics.enabled`,也可用 `metrics.instrumentation.jobqueue` 单独覆盖;关闭时使用 `jobqueue.NoopMetrics`。

### DbcacheProvider — `dbcache.<name>.*`

每个 `<name>` 是一个独立的 `dbcache.Store`(memory / redis / tiered),按 name 注入容器,业务方通过 `MustGetDbcacheStore(ctx, "<name>")` 获取后传给 `dbcache.NewBun[K,V](db, dbcache.WithStore(store))`。Provider 不直接构造泛型 `*Cache[K, V]`,Cache 的生命周期由业务方掌握。

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `store` | string | `memory` | `memory` / `redis` / `tiered` |
| `size` | int | `100000` | LRU 容量上限,`memory` / `tiered` 用 |
| `redis` | string | `default` | redis 实例名,`redis` / `tiered` 用 |
| `key_prefix` | string | — | Redis key 前缀,`redis` / `tiered` 用,多 cache 共享同一 Redis 时务必区分 |

`memory` 后端无外部依赖,`Shutdown` 时 `Purge` 清空 LRU;`redis` / `tiered` 的 Redis 客户端由 `RedisProvider` 统一管理,Dbcache 的 `Shutdown` 不再 Close redis client。

> **失效**:dbcache 不实装跨进程广播,多实例下 L1 靠 TTL 自然收敛。业务方写后失效仍由自家 model 的 bun `AfterUpdate/AfterDelete` hook 调 `cache.Delete`。

**可观测性(默认 noop,显式接入 OTel)**:业务在 `dbcache.New` 时显式 `dbcache.WithMetrics(dbcache.NewOTelMetrics())` 和/或 `dbcache.WithTracer(dbcache.NewOTelTracer())` 才会上报。两个工厂内部走全局 `otel.MeterProvider` / `otel.TracerProvider`,与 `metrics.enabled` / `trace.enabled` 联动 —— 未启用时是 noop。Provider 的 `trace.instrumentation.*` / `metrics.instrumentation.*` 只控制自动挂载的 Provider hook,不控制这种业务显式接入。

- 指标:`dbcache.hits` / `dbcache.misses`(Counter,标签 `dbcache.name`)、`dbcache.load.duration`(Histogram,单位 `ms`,标签 `dbcache.name` + `dbcache.status`=`ok`|`not_found`|`error`)。
- 追踪:`dbcache.Get` / `dbcache.MGet` / `dbcache.Set` / `dbcache.Delete` / `dbcache.Warm` 外层 span;loader 回源用子 span `dbcache.Loader`;Redis store 走 redisotel 自动作为更深一层子 span 出现。
- attribute 只记 cache 名与 key 数量,不记 key 值(防泄露)。
- 不传或显式传 `dbcache.NoopMetrics{}` / `dbcache.NoopTracer()` 都是无可观测性。

### AccessLogProvider — `accesslog.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `payload` | bool | `false` | 是否记录请求 / 响应摘要 |
| `payload_max_bytes` | int | `accesslog` 包默认 | payload 截断阈值 |
| `slow_threshold` | duration | `accesslog` 包默认 | 超过此值标记 slow 字段 |
| `skips` | []string | — | path / FullMethod 白名单 |
| `mask_fields` | []string | — | protojson 摘要中精确匹配 key 名替换为 `"***"`,典型如 `password` / `old_password` / `*_token` |

### ValidateProvider / RecoveryProvider / ErrorProvider

无 yaml 配置,`App.Use` 后注册即生效。`RecoveryProvider.Setup` 依赖 `MustGetNotifier`,所以必须排在 `NotifyProvider` 之后。

### MessageProvider — `message.email.*` / `message.sms.*`

`message.email.enabled` 或 `message.sms.enabled` 任一为 true 即视为启用,`Setup` 阶段才真正构建。

```yaml
message:
  email:
    enabled: true
    templates_dir: ./templates/email
    providers:
      - type: smtp                       # smtp | sendgrid
        name: primary
        from: noreply@example.com
        host: smtp.example.com
        port: 587
        username: user
        password: pass
        tls: starttls                    # tls | starttls | none
        include_domains: ["example.com"] # 可选,域名后缀过滤
      - type: sendgrid
        name: bulk
        from: bulk@example.com
        api_key: SG.xxx
    templates:
      - id: welcome
        subject: Welcome
        html_file: welcome.html          # html / text 任一可走 inline 或 *_file,二者互斥
        text: "Welcome {{.Name}}"
  sms:
    enabled: true
    providers:
      - type: twilio                     # twilio | byteplus | aliyun | aliyun-cn
        name: intl
        account_sid: ACxxx
        auth_token: xxx
        from: "+1234567890"
        include_regions: ["1", "44"]
      - type: aliyun-cn
        name: cn
        access_key: xxx
        secret_key: xxx
        endpoint: dysmsapi.aliyuncs.com
        sign_name: "示例签名"
```

每个 provider 可配 `include_domains` / `exclude_domains`(邮件)或 `include_regions` / `exclude_regions`(短信),按顺序作 fallback 优先级。完整字段表见 `provider/message.go` 顶部的 `emailProviderYaml` / `smsProviderYaml` 结构。

### MigrationProvider(代码层)

只在迁移子命令路径下注册,正常 serve 不挂。构造方式:

```go
//go:embed migrations
var migrationsFS embed.FS

app.Use(provider.NewMigrationProvider(migrationsFS))
```

`fsys` 顶层每个目录名必须对应已配置的 `database.<name>`,否则 Register 阶段直接报错。反向缺失(database 配了但还没写迁移)允许。

### BuildProvider

无配置,`Register` 阶段读 `runtime/debug.ReadBuildInfo()` 生成 `*BuildInfo` 注入容器。go build 时自动捕获 VCS 标签 / commit / dirty 状态。

---

## 5. 怎么使用

### 5.1 完整 main.go(serve 模式)

```go
package main

import (
    "context"
    "embed"

    "github.com/toolbelts/forge/ioc"
    "github.com/toolbelts/forge/provider"
    pb "your/proto/package"
)

//go:embed configs
var configsFS embed.FS // 可选;若用磁盘 ./configs 直接省略

func main() {
    app := ioc.New(ioc.WithShutdownTimeout(30 * time.Second))

    // A. 基础(Build 必须在 Trace/Metrics/Notify 之前;Metrics/Trace 必须在 Redis/DB 之前)
    must(app.Use(
        &provider.BuildProvider{},
        provider.NewConfigProvider("myapp"),
        &provider.LoggerProvider{},
        &provider.MetricsProvider{},
        &provider.TraceProvider{},
        &provider.NotifyProvider{},
    ))

    // B. 数据层(JobQueue / Dbcache 必须在 Redis 之后)
    must(app.Use(
        &provider.RedisProvider{},
        &provider.DatabaseProvider{},
        &provider.LockProvider{},
        &provider.CronProvider{},
        &provider.JobQueueProvider{},
        &provider.DbcacheProvider{},
        &provider.MessageProvider{},
    ))

    // C. 拦截器(顺序 = 链外层→内层,**必须放在 GrpcProvider 之前**)
    must(app.Use(
        &provider.RecoveryProvider{},
        &provider.AccessLogProvider{},
        &provider.ErrorProvider{},
        &provider.RateLimitProvider{},
        &provider.ValidateProvider{},
        &provider.TokenProvider{},
    ))

    // D. 服务端
    must(app.Use(
        &provider.GrpcProvider{},
        &provider.HttpProvider{},
        &provider.PprofProvider{},
        &provider.GatewayProvider{},
    ))

    // E. 注册中心,Setup 时读 grpc listener 的实际端口
    must(app.Use(&provider.RegistryProvider{}))

    // 业务 Provider 自己实现 Provider 接口,在 Setup 阶段注册 gRPC service
    must(app.Use(&MyBusinessProvider{}))

    if err := app.Run(context.Background(), nil); err != nil {
        log.Fatal().Err(err).Msg("app exited with error")
    }
}

func must(err error) {
    if err != nil {
        panic(err)
    }
}

// MyBusinessProvider 示意:在 Setup 拿到 grpc.Server 注册 service 实现
type MyBusinessProvider struct{}

func (p *MyBusinessProvider) Register(ctx context.Context) error { return nil }
func (p *MyBusinessProvider) Setup(ctx context.Context) error {
    srv := provider.MustGetGrpcServer(ctx)
    pb.RegisterMyServiceServer(srv, &myServiceImpl{
        tm:  provider.MustGetTokenManager(ctx),
        rdb: provider.MustGetRedis(ctx, "default"),
        db:  provider.MustGetDb(ctx, "default"),
    })
    return nil
}
```

### 5.2 CLI 子命令(fn 模式)

`fn != nil` 时跳过 Server,`fn` 跑完即退出。常用于 migrate / seed / 一次性脚本:

```go
func main() {
    app := ioc.New()
    must(app.Use(
        &provider.BuildProvider{},
        provider.NewConfigProvider("myapp"),
        &provider.LoggerProvider{},
        &provider.DatabaseProvider{},
        provider.NewMigrationProvider(migrationsFS),
    ))

    if err := app.Run(context.Background(), func(ctx context.Context) error {
        set := provider.MustGetMigrationSet(ctx)
        db := provider.MustGetDb(ctx, "default")
        return set.Migrate(ctx, "default", db) // 假定 migration 包提供此方法
    }); err != nil {
        log.Fatal().Err(err).Send()
    }
}
```

### 5.3 最小 `configs/common.yaml`

打开 grpc + http + redis + token + ratelimit 的最小可跑示例:

```yaml
app:
  name: myapp                          # registry 自注册时使用

log:
  level: info

redis:
  default:
    addr: 127.0.0.1:6379
    db: 0

grpc:
  enabled: true
  addr: ":50051"
  shutdown_timeout: 10s

http:
  enabled: true
  addr: ":8080"
  read_header_timeout: 5s
  shutdown_timeout: 10s

token:
  enabled: true
  redis: default
  access_ttl: 2h
  refresh_ttl: 720h
  refresh_rotation: true
  skips:
    - /v1/auth/login
    - /v1/auth/refresh
    - /grpc.health.v1.Health/Check

ratelimit:
  enabled: true
  redis: default
  reload_period: 30s
  failure_policy: fail_open

accesslog:
  enabled: true
  payload: false
  slow_threshold: 1s

notify:
  telegram:
    token: ""                          # 留空则关闭该通道
    chat_id: ""
  lark:
    webhook: ""
```

环境变量覆盖示例:`REDIS_DEFAULT_ADDR=redis:6379 ./myapp`(`.` → `_` 自动替换)。

---

## 6. 业务 handler 取依赖速查

| 依赖类型 | 取用方式 | 备注 |
|---|---|---|
| `*viper.Viper` | `provider.MustGetViper(ctx)` | |
| `provider.AppName`(string) | `provider.MustGetAppName(ctx)` | |
| `*provider.BuildInfo` | `provider.MustGetBuildInfo(ctx)` | |
| `*grpc.Server` | `provider.MustGetGrpcServer(ctx)` | grpc.enabled=true 才存在 |
| `net.Listener`(grpc) | `provider.MustGetGrpcListener(ctx)` | 同上 |
| `*http.ServeMux` | `provider.MustGetHttpMux(ctx)` | http.enabled=true 才存在 |
| `*runtime.ServeMux`(gateway) | `provider.MustGetGatewayMux(ctx)` | gateway.enabled=true 才存在 |
| `*grpc.ClientConn`(gateway) | `provider.MustGetGatewayConn(ctx)` | 同上 |
| `TcpHandler` | `provider.GetTcpHandler(ctx)` / `provider.MustSetTcpHandler(ctx, h)` | tcp.enabled=true 时业务方注入 |
| `*redis.Client` | `provider.MustGetRedis(ctx, "default")` | 实例名按 yaml |
| `*bun.DB` | `provider.MustGetDb(ctx, "default")` | 同上 |
| `*token.Manager` | `provider.MustGetTokenManager(ctx)` | token.enabled=true |
| `*ratelimit.RedisRateLimiter` | `provider.MustGetRateLimiter(ctx)` | ratelimit.enabled=true |
| `*registry.Manager` | `provider.MustGetRegistryManager(ctx)` | registry.enabled=true |
| `*lock.Manager` | `provider.MustGetLockManager(ctx)` | lock.enabled=true |
| `*cron.Cron` | `provider.MustGetCron(ctx)` | cron.enabled=true |
| `*jobqueue.Queue` | `provider.MustGetJobQueue(ctx)` | jobqueue.enabled=true |
| `dbcache.Store` | `provider.MustGetDbcacheStore(ctx, "default")` | 实例名按 yaml `dbcache.<name>` |
| `*message.Manager` | `provider.MustGetMessageManager(ctx)` | message.email/sms.enabled=true |
| `notify.Notifier` | `provider.MustGetNotifier(ctx)` | 始终存在,空配置走 noop |
| `*metricSdk.MeterProvider` | `provider.MustGetMeterProvider(ctx)` | metrics.enabled=true |
| `*traceSdk.TracerProvider` | `provider.MustGetTracerProvider(ctx)` | trace.enabled=true |
| `*migration.Set` | `provider.MustGetMigrationSet(ctx)` | 仅当 `NewMigrationProvider(...)` 已 Use |

每个 `MustGet*` 都有对应 `Get*` 变体,返回 `(T, error)` 而非 panic,适合启动后动态判断的场景。

---

## 7. 常见坑

- **拦截器 Provider 必须在 GrpcProvider 之前 Use**:`InterceptorChain.Use` 是 Setup 阶段的 side effect,Setup 按 Use 顺序跑;`GrpcProvider.Setup` 读 chain 构造 server。如果拦截器 Provider 排在 GrpcProvider 之后,Grpc 拿到的 chain 是空的,所有拦截器丢失。**6 个拦截器内部顺序也必须严格按链外→内**:`Recovery → AccessLog → Error → RateLimit → Validate → Token`
- **`enabled=false` 与 `MustGet`**:`CronProvider`/`JobQueueProvider`/`LockProvider` 等关闭时不向容器注入实例,业务方调 `MustGet` 直接 panic。这是有意为之 —— 关闭某能力时不应允许业务方依赖它
- **`viper.GetBool` / `GetInt` 在键缺失时返回零值**:对默认值非零的开关(`token.refresh_rotation` 默认 true、`lock.retry` 默认非零)必须用 `v.IsSet(...)` 守卫,否则会被误关
- **Metrics / Trace 时序**:启用自动 instrumentation 时,必须 Use 在 Redis/Database/Gateway 之前。后三者的 Register 阶段可能挂 `redisotel.InstrumentTracing` / `bunotel.NewQueryHook` / `otelgrpc.NewClientHandler`,需要全局 MeterProvider/TracerProvider 已就绪。`GrpcProvider` 的 otelgrpc handler 在 Setup 阶段才装,顺序无强约束(Setup 永远晚于所有 Register)
- **JobQueue / Dbcache(redis|tiered)必须在 Redis 之后**:它们在 Register 阶段就调 `(Must)GetRedis`,所以必须排在 RedisProvider 之后 Use。Dbcache 的 `memory` 后端无此约束
- **InterceptorChain 总要 Use**:即使不开 gRPC,`GrpcProvider` 也要 Use(可以 `grpc.enabled=false`),它的 Register 阶段创建 chain。否则拦截器 Provider 在 Setup 时拿不到 chain
- **`grpc.addr=":0"` + Registry**:支持。RegistryProvider 从 listener 读实际端口,而不是从 `grpc.addr` 字符串解析
- **`Shutdown` LIFO**:`NotifyProvider` 排第一所以最后 Shutdown,正好给业务 server 关停后再发 "stopped" 通知 — 不要把 `NotifyProvider` 改放后面

---

## 8. 验证

```sh
go vet ./provider/...
go build ./...
go doc github.com/toolbelts/forge/provider | head
```

源码导航:

- 拦截器链顺序权威定义 — `provider/interceptor.go:13`
- 生命周期阶段定义 — `ioc/app.go`,`ioc/doc.go`
- 各 Provider 的 yaml 键覆盖率 — 逐文件搜 `v.GetXxx(`
