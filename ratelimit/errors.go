package ratelimit

import "errors"

var (
	// ErrInvalidRule 表示规则定义不合法。
	ErrInvalidRule = errors.New("ratelimit: invalid rule")
	// ErrNilRedisClient 表示未提供可用的 Redis 客户端。
	ErrNilRedisClient = errors.New("ratelimit: nil redis client")
	// ErrInvalidCounterResponse 表示 Lua 计数脚本返回了非法结果。
	ErrInvalidCounterResponse = errors.New("ratelimit: invalid counter response")
)
