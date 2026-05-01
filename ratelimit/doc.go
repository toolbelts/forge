// Package ratelimit 提供基于 Redis 的滑动窗口限流,支持运行时规则热加载:
//
//   - Rule = name + path(精确或正则) + methods + Quota{Capacity, Window} + 维度键(IP / user_id 等)
//   - NewLimiter(rdb, opts) 持有规则快照,Allow(ctx, req) 命中规则后返回
//     {Allowed, Remaining, ResetAfter, RuleName}
//   - LoadRules 整体替换规则快照,运行中替换不打断在飞请求;非法规则跳过单条不影响其它规则
//   - RedisRuleStore 持久化规则(SetRule 写入前做完整校验);RedisRateLimiter 把 Limiter 与 Store
//     拼好,默认 30s 自动 Reload
//   - WithFailurePolicy(fail_open / fail_closed) 决定 Redis 不可用时放行还是拒绝
//
// 多规则同时命中时取最严格的;subject(维度键值)缺失时跳过该规则,避免误伤匿名流量。
package ratelimit
