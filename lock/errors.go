package lock

import "errors"

var (
	// ErrNilRedisClient 表示未提供可用的 Redis 客户端。
	ErrNilRedisClient = errors.New("lock: nil redis client")
	// ErrInvalidOption 表示 Manager 配置项不合法。
	ErrInvalidOption = errors.New("lock: invalid option")
	// ErrEmptyKey 表示 Lock 时发现 key 为空,Manager.NewLocker 不做强制校验。
	ErrEmptyKey = errors.New("lock: empty key")
	// ErrLocked 表示 key 已被其他持有者占用。
	ErrLocked = errors.New("lock: already locked")
	// ErrNotHeld 表示 Unlock 时锁已过期或本实例未持锁,业务通常忽略。
	ErrNotHeld = errors.New("lock: not held")
)
