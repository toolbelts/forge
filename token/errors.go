package token

import "errors"

var (
	// ErrNilRedisClient 表示未提供可用的 Redis 客户端。
	ErrNilRedisClient = errors.New("token: nil redis client")
	// ErrInvalidOption 表示 Manager 的选项不合法。
	ErrInvalidOption = errors.New("token: invalid option")
	// ErrTokenNotFound 表示 token 在 Redis 中不存在。
	ErrTokenNotFound = errors.New("token: not found")
	// ErrTokenExpired 表示 token 已逻辑过期,但缓冲期内仍可在 Redis 中查到原始数据。
	ErrTokenExpired = errors.New("token: expired")
	// ErrTokenCorrupted 表示 token 在 Redis 中的载荷无法解析。
	ErrTokenCorrupted = errors.New("token: corrupted payload")
	// ErrGeneratorFailed 表示 token 字符串生成器执行失败。
	ErrGeneratorFailed = errors.New("token: generator failed")
	// ErrInvalidUserId 表示传入的用户 id 非法。
	ErrInvalidUserId = errors.New("token: invalid user id")
	// ErrEmptyToken 表示传入的 token 字符串为空。
	ErrEmptyToken = errors.New("token: empty token")
)
