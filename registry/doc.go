// Package registry 提供基于 Redis 的服务注册与发现,以心跳驱动 TTL:
//
//   - Manager.Register(ctx, instance) 写入实例(Id / Service / Addr / Metadata),启动后台心跳
//     续租(默认 5s,小于 ttl/3),返回 *Registration 用于 Deregister
//   - Resolve(ctx, service) 用 SCAN + MGET 拿全量实例,结果按 Id 排序保证稳定
//   - Watch(ctx, service) 周期轮询(WithResolveInterval 控制),内容未变时不向 channel 推送,
//     避免下游误更新
//   - Resolver Builder 注册到 grpc-go 解析器表,gRPC client 直接 Dial("redis:///<service>") 即可
//   - 注册与续约均为原子 Lua,冲突直接返回错误,避免并发覆盖
//
// 实例失联随 TTL 自动过期,无需显式注销;Deregister 只用于优雅停机减少抖动。
package registry
