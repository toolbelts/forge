package registry

import "errors"

var (
	// ErrNilRedisClient 表示未提供可用的 Redis 客户端。
	ErrNilRedisClient = errors.New("registry: nil redis client")
	// ErrInvalidOption 表示 Manager 配置项不合法。
	ErrInvalidOption = errors.New("registry: invalid option")
	// ErrEmptyService 表示 service 名为空。
	ErrEmptyService = errors.New("registry: empty service")
	// ErrEmptyInstanceId 表示实例 id 为空。
	ErrEmptyInstanceId = errors.New("registry: empty instance id")
	// ErrEmptyAddr 表示实例 addr 为空。
	ErrEmptyAddr = errors.New("registry: empty addr")
	// ErrInstanceConflict 表示同 service+id 下已存在 addr 不同的实例,拒绝重复注册。
	ErrInstanceConflict = errors.New("registry: instance id conflict")
)
