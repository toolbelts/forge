// Package token 提供基于 Redis 的 access/refresh 令牌生命周期,关键路径走原子 Lua:
//
//   - Create(ctx, userId, meta) 生成一对 (access, refresh) 并把 access 加到 user → tokens 索引
//   - Validate(ctx, access) 校验存在性与逻辑过期;Renew(ctx, access) 在不更换字符串的前提下延长 TTL
//   - Refresh(ctx, refresh) 换发 access(WithRefreshRotation 默认开启时同时轮换 refresh);轮换时
//     刷新 refresh entry 内的 access 字段,避免下次刷新拿到旧 token
//   - Delete / DeleteByUser / ListByUser 维护用户索引,过期条目惰性清理
//   - Token.GetMeta / SetMeta 在不影响 TTL 的前提下读写元数据;SetMeta 设置的字段在 Refresh 后保留
//
// 物理 SaveTtl 大于逻辑 Ttl,留出过期排查窗口;并发刷新通过 Lua CAS 保证只有一个调用方拿到
// 新 token,其它返回 ErrTokenNotFound(replay 防护)。
package token
