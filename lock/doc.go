// Package lock 提供基于 Redis 的分布式互斥锁,带 fence token 与 TTL 自动续租:
//
//   - Manager.NewLocker(key) 拿到 *Locker;Lock(指数退避重试)、TryLock(单次)、Unlock、Renew
//   - Manager.Run(ctx, key, fn) 持锁执行 fn,后台每 ttl/3 续租一次;续租失败取消 fn 的 ctx
//   - Locker.Fence() 返回本次加锁单调递增的 fencing token,供下游存储校验"持锁者写入"
//   - 加锁与续租走原子 Lua 脚本,Unlock 校验 token 后再 DEL,防止 TTL 过期误删别人持有的锁
//
// 已被占用返回 ErrLocked、未持锁返回 ErrNotHeld、空 key 返回 ErrEmptyKey,业务可据此区分
// "被别人持有""锁已过期"与"参数错误"。Locker 非并发安全,多 goroutine 各自 NewLocker。
package lock
