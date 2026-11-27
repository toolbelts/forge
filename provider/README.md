# forge/provider

把 forge 各子库 (redis、gRPC、限速、token 等) 封装成 `Provider`,由 `ioc.Container` 统一编排:配置驱动、生命周期一致、按需启用。

接入只需三步:写 yaml → `c.Instance(...)` 按推荐顺序串起来 → `c.Run(ctx)` 起服务。

---

## 1. 概述

每个 Provider 就是一个绑定到容器的普通实例,按需实现 `ioc` 的三个可选接口:`Setup(ctx) error`(初始化)、`Serve(ctx) error`(阻塞式服务)、`Shutdown(ctx) error`(释放资源)。`c.Run(ctx)` 内部按以下顺序跑:

1. `Boot` — 按注册顺序逐个 `Setup`。每个 Setup 可以往容器里注入自己的产物,也可以取别的 Provider 的产物;取的时候如果对方还没 Setup,容器会先把它 Setup(见第 2 节)
2. `Serve` — 所有实现了 `Server` 的 Provider 并发跑。任一 Serve 返回 error,或全部 Serve 都返回,即触发收尾
3. `Shutdown` — 已成功 Setup 的 Provider 按逆序执行 Shutdown

一次性任务(CLI 子命令、批处理)用 `c.Exec(ctx, fn)`:`Boot → fn(ctx) → Shutdown`,不启动任何 Server。

任一 Setup 返回 error 立即收尾:对已 Setup 的 Provider 逆序 Shutdown,避免半启动状态。`SIGINT` / `SIGTERM` 通过 ctx 传给 fn / Serve,视为正常退出;收尾期间再次收到信号进程立即终止(详见 `ioc/ioc.go`)。

Setup / Serve / Shutdown 收到的 ctx 带着容器,`provider.MustGetXxx(ctx)` 这类访问函数都从它取实例。**请求级 ctx(gRPC handler 的 ctx)不带容器**,依赖要在 Setup 时取出来存到自己的结构里。

**统一约定**

- 配置键全部从单一 viper 实例读取(`ConfigProvider` 注入)
- 多数 Provider 用 `<name>.enabled` 控制总开关。`enabled=false` 时:
  - **不向容器注入实例** — 业务方调 `MustGet*` 会 panic(`TokenProvider`、`RateLimitProvider`、`LockProvider`、`CronProvider`、`JobQueueProvider`、`ReliableQueueProvider`、`MetricsProvider`、`TraceProvider`、`GrpcProvider`、`HttpProvider`、`GatewayProvider`、`PprofProvider`、`MessageProvider`)。这是有意为之 —— 关闭某能力时不应允许业务方依赖它
  - **拦截器 Provider** 不挂拦截器(`AccessLogProvider`、`RateLimitProvider`、`TokenProvider`)
  - **始终启用、无 `enabled` 字段**:`BuildProvider`、`ConfigProvider`、`LoggerProvider`、`RecoveryProvider`、`ErrorProvider`、`ValidateProvider`、`MigrationProvider`;`RedisProvider` / `DatabaseProvider` 按 yaml `redis.<name>` / `database.<name>` map 是否非空自动判断
  - `TcpProvider` 有 `enabled` 但不向容器注入任何实例,业务方反过来用 `MustSetTcpHandler` 注入 `TcpHandler`
  - **`NotifyProvider`**:无 `enabled`,所有通道留空时使用 noop 实现,`MustGetNotifier` 仍可获取
- Redis 派生 Provider 默认从名为 `default` 的 redis 实例取客户端,可通过 `<name>.redis` 切换其它实例

---

## 2. 推荐编排顺序

Provider 之间的**依赖不靠注册顺序**:`provider.MustGetXxx(ctx)` 取产物时,如果产出它的 Provider 还没 Setup,容器会先把它 Setup(收尾时相应地后关)。所以 `LockProvider` 可以写在 `RedisProvider` 前面,业务 Provider 也不用关心基础设施的先后;互相依赖会在启动时报 `ioc: dependency cycle`。

仍然由注册顺序决定的只有两件事:

1. **读写链的 Provider 不会被提前拉起** — `GrpcProvider` / `HttpProvider` / `GatewayProvider`(消费链)和 `TokenProvider` / `RateLimitProvider`(往链上加拦截器)的 Setup 结果取决于执行时机,所以取它们的产物(`MustGetGrpcServer`、`MustGetHttpMux`、`MustGetTokenManager` 等)时必须排在它们之后,否则启动报 `ioc: binding not found`
2. **链的内容** — `InterceptorChain.Use` 是各拦截器 Provider 在 Setup 里的 side effect,而 `GrpcProvider.Setup` 读 chain 来构造 `*grpc.Server`。所以**所有拦截器 Provider 必须注册在 `GrpcProvider` 之前**,否则构造 server 时 chain 还是空的。`MiddlewareChain.Use` 同理:`HttpProvider.Setup` / `GatewayProvider.Setup` 读各自命名链(`http` / `gateway`)包装 server handler,**HTTP 中间件 Provider 必须注册在两者之前**

Shutdown 自动按逆序执行,无需手动管理。

### A. 基础(必装,一开始就 Use)

| # | Provider | 说明 |
|---|---|---|
| 1 | `BuildProvider` | 注入 `*BuildInfo`,Trace/Metrics/Notify 的 Setup 会读它 |
| 2 | `ConfigProvider` | viper + .env 加载,所有后续 Provider 的前置 |
| 3 | `LoggerProvider` | zerolog 全局 logger,读 `log.level` 并热重载 |
| 4 | `MetricsProvider` | OTel MeterProvider。Redis/Database/Grpc/Gateway/JobQueue/ReliableQueue 挂 hook 前会先把它拉起 |
| 5 | `TraceProvider` | OTel TracerProvider + Propagator。Redis/Database/Grpc/Gateway 挂 hook 前会先把它拉起 |
| 6 | `NotifyProvider` | 通知器,被 `RecoveryProvider.Setup` 引用。本身实现 `Server`:所有 Provider Setup 完成后发 "started",Shutdown 阶段(逆序,越靠前越晚)发 "stopped" — 所以位置要靠前 |

### B. 数据层(按需)

| # | Provider | 说明 |
|---|---|---|
| 7 | `RedisProvider` | 多实例 redis 客户端,Setup 时注入容器;按 instrumentation 配置决定是否挂 redisotel hook |
| 8 | `DatabaseProvider` | 多实例 `*bun.DB`;按 instrumentation 配置决定是否挂 bunotel query hook |
| 9 | `MigrationProvider` | 仅迁移子命令路径下注册,正常 serve 不挂(参见 `migration.go` 顶部注释) |
| 10 | `LockProvider` | 分布式锁工厂。Setup 时取 Redis 客户端 |
| 11 | `CronProvider` | 定时任务调度器 |
| 12 | `JobQueueProvider` | redis 任务队列。Setup 时取 Redis 客户端 |
| 13 | `ReliableQueueProvider` | Redis Streams 至少一次队列。Setup 时取 Redis 客户端 |
| 14 | `DbcacheProvider` | 数据库缓存 Store 工厂。redis / tiered 后端 Setup 时取 Redis 客户端 |
| 15 | `MessageProvider` | 邮件 / 短信路由,无外部依赖 |

### C. gRPC 拦截器(按链外→内顺序注册,**全部放在 GrpcProvider 之前**)

`InterceptorChain.Use` 是追加,**Use 顺序 = 外层执行顺序**(见 `provider/interceptor.go`)。把下列 Provider 严格按此序注册:

| # | Provider | 拦截器位置 | 说明 |
|---|---|---|---|
| 16 | `RecoveryProvider` | 最外层 | 必须最先 recover panic,异步推送告警。Setup 读 `MustGetNotifier` |
| 17 | `AccessLogProvider` | 内一层 | 看到的 err 已被 Recovery 兜底成 `errkit.Error` |
| 18 | `ErrorProvider` | 内二层 | 把裸 error / `context.*` / `status.Status` 归一化为 `errkit.Error` |
| 19 | `RateLimitProvider` | 内三层 | Setup 同时构造 `*RedisRateLimiter` 实例 + 挂拦截器。在 protovalidate 之前砍 CPU |
| 20 | `ValidateProvider` | 内四层 | protovalidate,纯内存 CEL |
| 21 | `TokenProvider` | 最内层 | Setup 同时构造 `token.Manager` 实例 + 挂拦截器。鉴权要查 Redis,放最后让畸形请求先被前面砍掉 |

> `InterceptorChain` 没有固定的创建者:第一个调 `provider.MustGetInterceptorChain(ctx)` 的 Provider 创建它。所以不启用 gRPC 时可以不注册 `GrpcProvider`,拦截器 Provider 照常工作(只是没人消费这条链)。

### D. 服务端(按需)

| # | Provider | 说明 |
|---|---|---|
| 22 | `GrpcProvider` | Setup 抢端口,读 `*InterceptorChain` 构造 `*grpc.Server`,把 listener 和 server 注入容器。**所有拦截器 Provider 必须注册在它之前**,注册 gRPC service 的业务 Provider 在它之后 |
| 23 | `HttpProvider` | Setup 把 `*http.ServeMux` 注入容器,并用命名 `http` 的 `*MiddlewareChain` 包装 mux。**HTTP 中间件 Provider 必须注册在它之前**,注册路由的业务 Provider 在它之后 |
| 24 | `PprofProvider` | 复用 HttpProvider 的 mux,仅当 `pprof.enabled && http.enabled` 才挂。必须在 `HttpProvider` 之后 |
| 25 | `GatewayProvider` | grpc-gateway,反向 dial gRPC 后端;Setup 用命名 `gateway` 的 `*MiddlewareChain` 包装 gateway mux(trace 提取在链外最外层)。**HTTP 中间件 Provider 必须注册在它之前** |
| 26 | `TcpProvider` | 通用 TCP accept loop,业务方通过 `MustSetTcpHandler(ctx, h)` 注入 `TcpHandler` |

> `MiddlewareChain` 与 `InterceptorChain` 同理:链在第一次 `MustGetHttpMiddlewareChain` / `MustGetGatewayMiddlewareChain` 时创建,与 `HttpProvider` / `GatewayProvider` 是否注册、是否 enabled 无关。注意中间件包装的是整个 server handler,pprof 等挂在 http mux 上的路由同样会被包裹。

### E. 注册中心(按需)

| # | Provider | 说明 |
|---|---|---|
| 27 | `RegistryProvider` | `Setup` 读 `MustGetGrpcListener` 的实际端口注册当前实例,必须在 `GrpcProvider` 之后。同时全局注册 `redis://` resolver,供 client 端 `grpc.NewClient("redis:///<service>")` 解析 |

---

## 3. 拦截器链顺序速查

```
Recovery → AccessLog → Error → RateLimit → Validate → Token → 业务
(最外层)                                                  (最内层)
```

`Use` 顺序 = 链表追加顺序 = 拦截器外→内执行顺序。原文见 `provider/interceptor.go`。

### HTTP 中间件链

HTTP 侧的对应机制是 `MiddlewareChain`(权威定义 `provider/middleware.go`),同样 **Use 顺序 = 外层执行顺序**。中间件 Provider 在自己的 Setup 中取出对应命名链追加,必须注册在 `HttpProvider` / `GatewayProvider` 之前:

```go
// 某中间件 Provider 的 Setup —— 必须注册在 GatewayProvider 之前
chain := provider.MustGetGatewayMiddlewareChain(ctx)
chain.Use(func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body) // 在 grpc-gateway 消费 body 之前读取
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body)) // 读完必须回填,下游才能再次消费
		// r.Method / r.URL.RawQuery / body 可用于验签等场景
		next.ServeHTTP(w, r)
	})
})
```

gateway 的 trace 提取(`withTracePropagation`)始终在所有中间件之外的最外层,中间件内可直接使用已接续上游 trace 的 ctx。

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
| `instrumentation.reliablequeue` | bool | 上一行结果 | 覆盖 ReliableQueueProvider metrics |

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

注入容器的通知器统一将标题格式化为 `[hostname] appName 原标题`，hostname 与 AppName 在启动时读取并缓存。
已有当前主机名前缀或应用名前缀时不重复添加，启动、停止与 panic 通知沿用现有标题。
无法读取主机名时使用 `unknown`；正文、通道选择与发送错误保持原有行为。

### RedisProvider — `redis.<name>.*`

每个 `<name>` 是一份独立的 `redis.UniversalClient`,通过 `MustGetRedis(ctx, "<name>")` 获取。约定使用 `default` 作为主实例名。Provider 当前仍按单地址配置创建 `*redis.Client`,容器只暴露接口,消费方无需依赖具体客户端类型。

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
| `debug` | bool | — | 开启后每条 SQL 以 zerolog debug 记录(需全局 `log.level=debug` 才可见);与 `slow` 独立,可同时开启 |

启动时 `PingContext`,失败立即返回错误。bunotel query hook 默认在 `trace.enabled` 或 `metrics.enabled` 任一开启时挂载,也可用 `trace.instrumentation.database` / `metrics.instrumentation.database` 单独覆盖。`debug` 与 `slow` 互不替代:`debug` 输出全部查询(debug 级),`slow` 仅突出超阈值查询(warn 级)。

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

HTTP 中间件无配置键,通过 `provider.MustGetHttpMiddlewareChain(ctx).Use(...)` 在 Setup 阶段挂载,见第 3 节。

### GatewayProvider — `gateway.*`

| 键 | 类型 | 必填(enabled 时) | 说明 |
|---|---|---|---|
| `enabled` | bool | — | |
| `addr` | string | ✓ | gateway 监听地址 |
| `grpc_endpoint` | string | ✓ | gateway → gRPC 后端地址,通常是同进程的 `grpc.addr` |
| `read_timeout` / `read_header_timeout` / `write_timeout` / `idle_timeout` / `shutdown_timeout` | duration | — | 同 HttpProvider |

JSON 编码固定使用 proto 字段名(`UseProtoNames=true`),枚举输出字符串,默认值输出到响应体。

otelgrpc client stats handler 默认在 `trace.enabled` 或 `metrics.enabled` 任一开启时挂载,也可用 `trace.instrumentation.gateway` / `metrics.instrumentation.gateway` 单独覆盖。HTTP `traceparent` 提取跟随 `trace.instrumentation.gateway`。

HTTP 中间件无配置键,通过 `provider.MustGetGatewayMiddlewareChain(ctx).Use(...)` 在 Setup 阶段挂载,见第 3 节。

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
| `shutdown_timeout` | duration | `30s` | 等待 worker 与 coalesce drain 收尾 |
| `max_len` | int | `0` | 全局 LIST 长度上限,`0` = 不限;超限时 Publish 自动 LTRIM 到 `max/2` |
| `coalesce.<topic>.window` | duration | — | per-topic 合并触发时间窗 |
| `coalesce.<topic>.max_batch` | int | — | per-topic 合并触发条数,需 `>=2` 才生效 |

业务方在自己的 `Setup` 里 `MustGetJobQueue(ctx).Subscribe(topic, fn)`。
JobQueue OTel metrics 默认跟随 `metrics.enabled`,也可用 `metrics.instrumentation.jobqueue` 单独覆盖;关闭时使用 `jobqueue.NoopMetrics`。

**合并模式**:`coalesce.<topic>` 同时配 `window` 与 `max_batch` 才生效,`<topic>` 支持 `user.presence` 这种带点的 literal key;任一缺失或非法跳过并 warn 日志。开启后该 topic 的 `Publish` 转 fire-and-forget:成功只代表 payload 已进入内存缓冲,flush 成功上报 `jobqueue.coalesce.flush.total` 与 `jobqueue.coalesce.batch.size`,flush 失败只产生日志和 `jobqueue.publish.dropped{jobqueue.drop_reason="flush_failed"}`;进程崩溃丢未 flush 的缓冲。适用「单条不重要、靠最终态收敛」事件:在线状态、心跳、点击埋点。重要事件勿用。建议 `max_batch <= 1000` 且单事件 payload < 4KB,避免 Redis 单命令过大。

```yaml
jobqueue:
  enabled: true
  redis: default
  max_len: 50000
  coalesce:
    user.presence:
      window: 100ms
      max_batch: 200
    device.heartbeat:
      window: 500ms
      max_batch: 500
```

JobQueue 是 at-most-once 队列,取出后进程崩溃会丢消息,不适合扣款、记账等关键链路。

### ReliableQueueProvider — `reliablequeue.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `redis` | string | `default` | 建议使用独立高可用 Redis 实例 |
| `key_prefix` | string | — | Stream key 前缀 |
| `block_timeout` | duration | `2s` | 拉取新消息的阻塞时长 |
| `claim_idle` | duration | `30s` | Pending 空闲超过此值后允许其他 consumer 认领 |
| `recovery_interval` | duration | `5s` | 扫描 Pending 的间隔 |
| `batch_size` | int | `32` | 单次读取或认领上限 |
| `wait_replicas` | int | `0` | 发布后要求确认的副本数;大于 0 时客户端必须支持固定连接 |
| `wait_timeout` | duration | `2s` | `WAIT` 副本确认超时 |
| `dlq_topic` | string | `dlq` | 永久错误的默认死信 topic |
| `shutdown_timeout` | duration | `30s` | 停止拉取后等待执行中 handler 的时长 |

业务方在自己的 `Setup` 中调用 `MustGetReliableQueue(ctx).Subscribe(topic, group, handler, ...)`;Provider 在 `Serve` 启动 worker,在 `Shutdown` 等待 handler 退出。handler 返回普通错误时消息留在 PEL 等待 `XAUTOCLAIM`;返回 `reliablequeue.Permanent(err)` 时先写 DLQ 再 ACK;返回 `Result.Publications` 时先发布结果并完成副本确认,再 ACK 输入消息。

```yaml
reliablequeue:
  enabled: true
  redis: reliable
  key_prefix: "sp:reliable:"
  block_timeout: 2s
  claim_idle: 30s
  recovery_interval: 5s
  batch_size: 32
  wait_replicas: 1
  wait_timeout: 2s
  shutdown_timeout: 30s
```

ReliableQueue 是至少一次投递:崩溃和副本确认超时可能导致重复,handler 和下游发布必须以稳定 `message.Id` 幂等。`WAIT` 只能降低 Redis 主从故障时丢失概率,不能替代 PostgreSQL Outbox 或提供强一致。启用 `wait_replicas` 时内部在同一连接执行 `XADD → WAIT`;`*redis.Client` 支持该能力,ClusterClient 会在构造时明确失败。生产资金 topic 应使用独立高可用 Redis、至少一个副本、持久化和不会淘汰 Stream 的 eviction 策略,并配合 Outbox、业务幂等及 `WithDeleteAfterAck(true)`。

### DbcacheProvider — `dbcache.<name>.*`

每个 `<name>` 是一个独立的 `dbcache.Store`(memory / redis / tiered),按 name 注入容器,业务方通过 `MustGetDbcacheStore(ctx, "<name>")` 获取后传给 `dbcache.NewBun[K,V](db, dbcache.WithStore(store))`。Provider 不直接构造泛型 `*Cache[K, V]`,Cache 的生命周期由业务方掌握。

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `store` | string | `memory` | `memory` / `redis` / `tiered` |
| `size` | int | `100000` | LRU 容量上限,`memory` / `tiered` 用 |
| `redis` | string | `default` | redis 实例名,`redis` / `tiered` 用 |
| `key_prefix` | string | — | Redis key 前缀,`redis` / `tiered` 用,多 cache 共享同一 Redis 时务必区分 |
| `invalidation.enabled` | bool | `false` | 开启 Redis Pub/Sub 删除广播,`redis` / `tiered` 用 |
| `invalidation.channel` | string | `dbcache:<name>:invalidate` | 同一逻辑缓存的所有进程必须一致 |

`memory` 后端无外部依赖,`Shutdown` 时 `Purge` 清空 LRU;`redis` / `tiered` 的 Redis 客户端由 `RedisProvider` 统一管理,Dbcache 的 `Shutdown` 不再 Close redis client。`tiered` 开启失效通知时,Shutdown 会先停止订阅协程再清理 L1/L2。

> **失效**:业务方写后失效仍由自家 model 的 bun `AfterUpdate/AfterDelete` hook 调 `cache.Delete`。开启通知后,Redis Store 在 L2 删除成功后发布逻辑 key;其它 `tiered` 实例收到消息只清自己的 L1。纯 `redis` 后端没有 L1,但可作为发布者兼容混合部署。Redis Pub/Sub 是低延迟、best-effort 通知,断线期间不补发,现有 L1 TTL 继续作为最终收敛兜底;它不提供严格一致性。同一缓存的所有进程必须使用相同 Redis、`key_prefix` 与通知频道。

```yaml
dbcache:
  users:
    store: tiered
    redis: default
    key_prefix: "app:users:"
    invalidation:
      enabled: true
      channel: "app:dbcache:users:invalidate"
```

**可观测性(默认 noop,显式接入 OTel)**:业务在 `dbcache.New` 时显式 `dbcache.WithMetrics(dbcache.NewOTelMetrics())` 和/或 `dbcache.WithTracer(dbcache.NewOTelTracer())` 才会上报。两个工厂内部走全局 `otel.MeterProvider` / `otel.TracerProvider`,与 `metrics.enabled` / `trace.enabled` 联动 —— 未启用时是 noop。Provider 的 `trace.instrumentation.*` / `metrics.instrumentation.*` 只控制自动挂载的 Provider hook,不控制这种业务显式接入。

- 指标:`dbcache.hits` / `dbcache.misses`(Counter,标签 `dbcache.name`)、`dbcache.load.duration`(Histogram,单位 `ms`,标签 `dbcache.name` + `dbcache.status`=`ok`|`not_found`|`error`)。
- 追踪:`dbcache.Get` / `dbcache.MGet` / `dbcache.Set` / `dbcache.Delete` / `dbcache.Warm` 外层 span;loader 回源用子 span `dbcache.Loader`;Redis store 走 redisotel 自动作为更深一层子 span 出现。
- attribute 只记 cache 名与 key 数量,不记 key 值(防泄露)。
- 不传或显式传 `dbcache.NoopMetrics{}` / `dbcache.NoopTracer()` 都是无可观测性。

### AccessLogProvider — `accesslog.*`

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | |
| `payload` | bool | `false` | 是否记录请求 / 响应摘要。proto 消息以 JSON 对象直接嵌入 `req` / `resp` 字段(零二次转义);截断前缀 / 占位符 / 非 proto 值落 `req_text` / `resp_text` 字符串字段,同一条日志两组字段互斥出现 |
| `payload_max_bytes` | int | `accesslog` 包默认(2048) | payload 单字段截断阈值,`< 0` 不截断 |
| `payload_size_limit` | int | `0`(自动) | proto wire size 预检阈值:序列化前 `proto.Size` 超过此值直接跳过 protojson,`*_text` 记 `<payload too large: <消息全名>, N bytes>` 占位符,大消息不再白付全量序列化成本。`0` 自动派生为 `4 × payload_max_bytes`(`payload_max_bytes < 0` 时不预检),`< 0` 强制禁用预检 |
| `slow_threshold` | duration | `accesslog` 包默认 | 超过此值标记 slow 字段 |
| `skips` | []string | — | path / FullMethod 白名单 |
| `mask_fields` | []string | — | protojson 摘要中精确匹配 key 名替换为 `"***"`,典型如 `password` / `old_password` / `*_token` |

> **下游索引提示**:`req` / `resp` 是结构随各 RPC 变化的嵌套对象,Elasticsearch 类动态映射会随 proto 字段数膨胀,建议索引模板对这两个字段用 `flattened` 或 `enabled: false`;`req_text` / `resp_text` 恒为字符串,可正常索引。

### ValidateProvider / RecoveryProvider / ErrorProvider

无 yaml 配置,注册即生效。`RecoveryProvider.Setup` 依赖 `MustGetNotifier`(会先把 `NotifyProvider` 拉起)。

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

c.Instance(provider.NewMigrationProvider(migrationsFS))
```

`fsys` 顶层每个目录名必须对应已配置的 `database.<name>`,否则 Setup 直接报错。反向缺失(database 配了但还没写迁移)允许。

### BuildProvider

无配置,Setup 读 `runtime/debug.ReadBuildInfo()` 生成 `*BuildInfo` 注入容器。go build 时自动捕获 VCS 标签 / commit / dirty 状态。

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
    c := ioc.New(ioc.WithShutdownTimeout(30 * time.Second))

    // A. 基础(Logger 靠前才能尽早生效;Notify 靠前才会在收尾时最后发 "stopped")
    c.Instance(&provider.BuildProvider{}).
        Instance(provider.NewConfigProvider("myapp")).
        Instance(&provider.LoggerProvider{}).
        Instance(&provider.MetricsProvider{}).
        Instance(&provider.TraceProvider{}).
        Instance(&provider.NotifyProvider{})

    // B. 数据层(彼此之间的依赖由容器按需拉起,顺序不限)
    c.Instance(&provider.RedisProvider{}).
        Instance(&provider.DatabaseProvider{}).
        Instance(&provider.LockProvider{}).
        Instance(&provider.CronProvider{}).
        Instance(&provider.JobQueueProvider{}).
        Instance(&provider.ReliableQueueProvider{}).
        Instance(&provider.DbcacheProvider{}).
        Instance(&provider.MessageProvider{})

    // C. 拦截器(顺序 = 链外层→内层,**必须放在 GrpcProvider 之前**)
    c.Instance(&provider.RecoveryProvider{}).
        Instance(&provider.AccessLogProvider{}).
        Instance(&provider.ErrorProvider{}).
        Instance(&provider.RateLimitProvider{}).
        Instance(&provider.ValidateProvider{}).
        Instance(&provider.TokenProvider{})

    // D. 服务端(Pprof 必须在 Http 之后:读写链的 Provider 不会被提前拉起)
    c.Instance(&provider.GrpcProvider{}).
        Instance(&provider.HttpProvider{}).
        Instance(&provider.PprofProvider{}).
        Instance(&provider.GatewayProvider{})

    // E. 注册中心,Setup 时读 grpc listener 的实际端口
    c.Instance(&provider.RegistryProvider{})

    // 业务 Provider 放最后:要用 grpc server / http mux,必须排在 D 之后
    c.Instance(&MyBusinessProvider{})

    if err := c.Run(context.Background()); err != nil {
        log.Fatal().Err(err).Msg("app exited with error")
    }
}

// MyBusinessProvider 示意:在 Setup 拿到 grpc.Server 注册 service 实现。
// 只需要实现用得到的生命周期方法,这里只有 Setup。
type MyBusinessProvider struct{}

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

同一个 Provider 类型只能注册一次(重复注册会 panic)。业务代码也可以不写 Provider,直接用构造函数注入:

```go
// 参数按类型从容器解析;只有一个 *bun.DB 绑定时不用指定名字
func NewUserService(db *bun.DB, tm token.Manager) *UserService { ... }

c.Provide(NewUserService) // 构造函数参数直接按类型取,不会拉起 Provider,所以要放在 DatabaseProvider / TokenProvider 之后
```

### 5.2 CLI 子命令(Exec 模式)

`c.Exec(ctx, fn)` 只做 `Boot → fn → Shutdown`,不启动 Server,`fn` 跑完即退出。常用于 migrate / seed / 一次性脚本:

```go
func main() {
    c := ioc.New()
    c.Instance(&provider.BuildProvider{}).
        Instance(provider.NewConfigProvider("myapp")).
        Instance(&provider.LoggerProvider{}).
        Instance(&provider.DatabaseProvider{}).
        Instance(provider.NewMigrationProvider(migrationsFS))

    if err := c.Exec(context.Background(), func(ctx context.Context) error {
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
  # payload_max_bytes: 2048              # 单字段截断阈值,<0 不截断
  # payload_size_limit: 0                # 大消息预检阈值,0 = 自动(4×payload_max_bytes)
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
| `*provider.InterceptorChain` | `provider.MustGetInterceptorChain(ctx)` | 首次获取时创建,与 GrpcProvider 是否注册 / enabled 无关 |
| `*http.ServeMux` | `provider.MustGetHttpMux(ctx)` | http.enabled=true 才存在 |
| `*provider.MiddlewareChain`(http) | `provider.MustGetHttpMiddlewareChain(ctx)` | 首次获取时创建,与 HttpProvider 是否注册 / enabled 无关 |
| `*runtime.ServeMux`(gateway) | `provider.MustGetGatewayMux(ctx)` | gateway.enabled=true 才存在 |
| `*grpc.ClientConn`(gateway) | `provider.MustGetGatewayConn(ctx)` | 同上 |
| `*provider.MiddlewareChain`(gateway) | `provider.MustGetGatewayMiddlewareChain(ctx)` | 首次获取时创建,与 GatewayProvider 是否注册 / enabled 无关 |
| `TcpHandler` | `provider.GetTcpHandler(ctx)` / `provider.MustSetTcpHandler(ctx, h)` | tcp.enabled=true 时业务方注入 |
| `redis.UniversalClient` | `provider.MustGetRedis(ctx, "default")` | 实例名按 yaml |
| `*bun.DB` | `provider.MustGetDb(ctx, "default")` | 同上 |
| `token.Manager`(接口) | `provider.MustGetTokenManager(ctx)` | token.enabled=true |
| `*ratelimit.RedisRateLimiter` | `provider.MustGetRateLimiter(ctx)` | ratelimit.enabled=true |
| `*registry.Manager` | `provider.MustGetRegistryManager(ctx)` | registry.enabled=true |
| `lock.Manager`(接口) | `provider.MustGetLockManager(ctx)` | lock.enabled=true |
| `*cron.Cron` | `provider.MustGetCron(ctx)` | cron.enabled=true |
| `*jobqueue.Queue` | `provider.MustGetJobQueue(ctx)` | jobqueue.enabled=true |
| `*reliablequeue.Queue` | `provider.MustGetReliableQueue(ctx)` | reliablequeue.enabled=true |
| `dbcache.Store` | `provider.MustGetDbcacheStore(ctx, "default")` | 实例名按 yaml `dbcache.<name>` |
| `*message.Manager` | `provider.MustGetMessageManager(ctx)` | message.email/sms.enabled=true |
| `notify.Notifier` | `provider.MustGetNotifier(ctx)` | 始终存在,空配置走 noop |
| `*metricSdk.MeterProvider` | `provider.MustGetMeterProvider(ctx)` | metrics.enabled=true |
| `*traceSdk.TracerProvider` | `provider.MustGetTracerProvider(ctx)` | trace.enabled=true |
| `*migration.Set` | `provider.MustGetMigrationSet(ctx)` | 仅当 `NewMigrationProvider(...)` 已注册 |

每个 `MustGet*` 都有对应 `Get*` 变体,返回 `(T, error)` 而非 panic,适合启动后动态判断的场景。

业务自己的实例直接用容器的泛型方法存取:

```go
c := ioc.FromContext(ctx)              // Setup / Serve / Shutdown / Exec 的 ctx
c.Instance(svc)                        // 按声明类型绑定
c.Instance[UserStore](pgStore, "main") // 显式绑定为接口类型,并命名
svc := c.MustGet[*UserService]()
store, err := c.Get[UserStore]("main")
```

业务 Provider 如果也在 Setup 里绑定产物,给它写访问函数时用 `GetFrom[T, Owner]`,取的时候就不依赖注册顺序:

```go
// 先确保 *UserProvider 已 Setup(没注册则跳过),再取 *UserService
func MustGetUserService(ctx context.Context) *UserService {
    return ioc.FromContext(ctx).MustGetFrom[*UserService, *UserProvider]()
}
```

前提是 `UserProvider.Setup` 的结果与执行时机无关;如果它要往拦截器链 / 中间件链上加东西,就别用 `GetFrom`,让调用方排在它后面。

---

## 7. 常见坑

- **拦截器 Provider 必须注册在 GrpcProvider 之前**:`InterceptorChain.Use` 是 Setup 里的 side effect,Setup 按注册顺序跑;`GrpcProvider.Setup` 读 chain 构造 server。如果拦截器 Provider 排在 GrpcProvider 之后,Grpc 拿到的 chain 是空的,所有拦截器丢失。**6 个拦截器内部顺序也必须严格按链外→内**:`Recovery → AccessLog → Error → RateLimit → Validate → Token`
- **HTTP 中间件 Provider 必须注册在 HttpProvider/GatewayProvider 之前**:`MiddlewareChain.Use` 是 Setup 里的 side effect;两个 Provider 的 Setup 读链包装 handler,之后追加的中间件被**静默丢弃**
- **`enabled=false` 与 `MustGet`**:`CronProvider`/`JobQueueProvider`/`ReliableQueueProvider`/`LockProvider` 等关闭时不向容器注入实例,业务方调 `MustGet` 直接 panic。这是有意为之 —— 关闭某能力时不应允许业务方依赖它
- **`viper.GetBool` / `GetInt` 在键缺失时返回零值**:对默认值非零的开关(`token.refresh_rotation` 默认 true、`lock.retry` 默认非零)必须用 `v.IsSet(...)` 守卫,否则会被误关
- **Metrics / Trace 时序**:forge 内挂 OTel hook 的 Provider(Redis/Database/Grpc/Gateway/队列)会在 Setup 里先把 `MetricsProvider` / `TraceProvider` 拉起,不用管顺序。业务代码若在自己的 Setup 里直接用全局 tracer / meter,仍应排在它们之后
- **哪些依赖与顺序无关,哪些不是**:`provider.MustGetXxx(ctx)` 会先把产出该实例的 Provider 拉起,所以 Config / Build / Redis / Database / Notify / Lock / 队列 / Dbcache 等之间不用排顺序。例外是读写链的 5 个 Provider(Grpc / Http / Gateway / Token / RateLimit):`MustGetGrpcServer` / `MustGetGrpcListener` / `MustGetHttpMux` / `MustGetGatewayMux` / `MustGetGatewayConn` / `MustGetTokenManager` / `MustGetRateLimiter` 不会拉起它们(提前 Setup 会静默丢拦截器或打乱链顺序),调用方必须排在它们之后,否则启动报 `ioc: binding not found: <类型>`。用 `c.Provide(构造函数)` 注入的参数同样只按类型取、不拉起
- **自定义 message 后端的注册时机**:`RegisterSmsSender` / `RegisterEmailSender` 必须早于 `MessageProvider.Setup` — 放在包级 `init()` 或 `main` 里 `c.Run` 之前。不要放进业务 Provider 的 Setup:`MessageProvider` 可能被别处的 `MustGetMessageManager` 提前拉起
- **没有 Server 时 `Run` 立即返回**:`c.Run` 在全部 Serve 返回后就收尾。`NotifyProvider.Serve` 会阻塞到收到信号,所以注册了它的应用始终常驻;没注册任何常驻 Server 的一次性程序请用 `c.Exec`
- **`grpc.addr=":0"` + Registry**:支持。RegistryProvider 从 listener 读实际端口,而不是从 `grpc.addr` 字符串解析
- **`Shutdown` 逆序**:`NotifyProvider` 排在最前面一批所以最后 Shutdown,正好给业务 server 关停后再发 "stopped" 通知 — 不要把 `NotifyProvider` 改放后面

---

## 8. 验证

```sh
go vet ./provider/...
go build ./...
go doc github.com/toolbelts/forge/provider | head
```

源码导航:

- 拦截器链顺序权威定义 — `provider/interceptor.go`
- HTTP 中间件链编排约定 — `provider/middleware.go`
- 容器与生命周期定义 — `ioc/ioc.go`
- 各 Provider 的 yaml 键覆盖率 — 逐文件搜 `v.GetXxx(`
