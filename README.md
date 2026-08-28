# forge

一组 Go 公共库,围绕 `ioc` + `provider` 提供"配置驱动的服务编排":每个目录是独立子库,可单独引入,也可由 `provider` 用 viper 配置串成完整服务。

- 模块路径: `github.com/toolbelts/forge`
- Go 版本: `1.26+`
- 文档约定: 每个子库根目录有 `doc.go` 给出 godoc 风格说明;关于 Provider 编排见 [`provider/README.md`](./provider/README.md)

---

## 库一览

### 基础设施

| 库 | 一句话 |
|---|---|
| [`ioc`](./ioc/) | 轻量 DI 容器 + Provider 生命周期编排(Register / Setup / Serve / Shutdown 四阶段,Shutdown 走 LIFO) |
| [`errkit`](./errkit/) | forge 与应用之间的错误契约,无 proto / 业务码依赖。中间件统一抽 `code` / `message` / `metadata`,序列化由应用注入 Encoder |
| [`meta`](./meta/) | 请求级 ctx 元数据,桥接 gRPC incoming metadata 与 HTTP 请求字段(path / IP / UA / country),支持跨服务透传 |
| [`migration`](./migration/) | 多 db 分组的 bun migrate 集合。从 `embed.FS` 顶层目录扫迁移,目录名匹配 `database.<name>` 配置 |

### 业务能力

| 库 | 一句话 |
|---|---|
| [`accesslog`](./accesslog/) | gRPC 一元 / 流式访问日志拦截器,字段命名下划线风格,err 已被上层归一化为 `errkit.Error` |
| [`cron`](./cron/) | `robfig/cron/v3` 薄封装。默认 `SkipIfStillRunning + Recover`,6 字段秒级表达式 |
| [`dbcache`](./dbcache/) | 数据库缓存(cache-aside),后端可选 memory/redis/tiered;singleflight 防击穿、负缓存防穿透;Redis Pub/Sub 可选跨进程失效;`NewBun` 自动反射主键 |
| [`jobqueue`](./jobqueue/) | Redis `LIST + BRPOP/BLMPOP` 极简任务队列,at-most-once;反射推断 handler 入参类型;支持 per-topic 合并发送 |
| [`reliablequeue`](./reliablequeue/) | Redis Streams 至少一次可靠队列;消费组 PEL + `XAUTOCLAIM` 恢复;支持结果先发布再 ACK、DLQ 与副本确认 |
| [`lock`](./lock/) | Redis 分布式互斥锁,fence token + TTL 自动续租,加 / 续 / 解锁均走原子 Lua |
| [`message`](./message/) | 邮件(SMTP / SendGrid)+ 短信(Twilio / BytePlus / Aliyun)多后端路由,per-recipient 优先级 fallback,html 模板自动转义 |
| [`notify`](./notify/) | Telegram + Lark 运维通知,fire-and-forget,10s HTTP 超时,适合"容忍丢失"的告警场景 |
| [`ratelimit`](./ratelimit/) | Redis 滑动窗口限流,规则运行时热重载,`fail_open` / `fail_closed` 策略可选 |
| [`registry`](./registry/) | Redis 心跳驱动的服务注册发现,自带 grpc resolver — `grpc.NewClient("redis:///<service>")` 即可 |
| [`token`](./token/) | Redis access / refresh 令牌生命周期,关键路径走原子 Lua,默认开启 refresh 旋转 + 用户索引 + replay 防护 |

### 编排

| 库 | 一句话 |
|---|---|
| [`provider`](./provider/) | 把上述每一项封装成 `Provider`,viper 驱动配置,一行 `App.Use(...)` 即可启用 — 详见 [`provider/README.md`](./provider/README.md) |

---

## 快速上手

绝大多数应用场景:直接用 `ioc + provider` 起服务,业务代码只关心实现 gRPC handler。

```go
import (
    "github.com/toolbelts/forge/ioc"
    "github.com/toolbelts/forge/provider"
)

app := ioc.New()
_ = app.Use(
    &provider.BuildProvider{},
    provider.NewConfigProvider("myapp"),
    &provider.LoggerProvider{},
    &provider.RedisProvider{},
    // 拦截器必须在 GrpcProvider 之前 Use
    &provider.RecoveryProvider{},
    &provider.ErrorProvider{},
    // ...
    &provider.GrpcProvider{},
    // ... 见 provider/README.md
)
_ = app.Run(ctx, nil)
```

完整可跑示例、推荐编排顺序、yaml 配置键参考 → [`provider/README.md`](./provider/README.md)。

需要绕过 `provider` 直接使用某个子库(例如脚本里只想用 `lock`、`jobqueue` 或 `reliablequeue`),按各子库 `doc.go` 直接 `import` 即可,所有子库都不强依赖 `provider` 或 `ioc`。

---

## 文档约定

- README 给"挑选 + 编排"读者看(怎么选、怎么串、怎么配)
- `doc.go` 给"已经在写代码"的读者看(godoc 风格,讲清抽象 + 取舍)
- 中文优先(代码与注释均以中文为主)
- 不重复 — README 里不复述 doc.go 已说清的细节
