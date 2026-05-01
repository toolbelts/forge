// Package meta 在 context 里维护请求级元数据,桥接 gRPC incoming metadata 与 HTTP 请求信息:
//
//   - Attach 把入站 gRPC metadata 物化成可读写的 store;Set / Delete / Has / Raw 做增删查改
//   - String / Int64 / Float64 / Bool 做类型安全只读转换,未命中或类型不符返回零值不报错
//   - Request(ctx) 返回常用字段的快照视图;UserId / Token 是更短的 shortcut
//   - OutgoingContext 把 store 内容回写为 gRPC outgoing metadata,跨服务透传
//   - Annotator 是 grpc-gateway 的 ServeMuxOption,从 *http.Request 抽 path / IP / UA / country 等
//   - BuildSkips / MatchSkips 用一组 method 名或 HTTP path 做白名单跳过(健康检查、内省等)
//
// 仅支持 string-keyed 元数据,不做反射或泛型;必填语义由业务侧自行判断。
package meta
