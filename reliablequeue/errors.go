package reliablequeue

import "errors"

var (
	// ErrNilRedisClient 表示 New 未收到 Redis 客户端。
	ErrNilRedisClient = errors.New("reliablequeue: nil redis client")
	// ErrUnsupportedClient 表示当前 Redis 客户端不支持已启用的固定连接语义。
	ErrUnsupportedClient = errors.New("reliablequeue: unsupported redis client")
	// ErrInvalidTopic 表示 topic 为空或包含非法字符。
	ErrInvalidTopic = errors.New("reliablequeue: invalid topic")
	// ErrInvalidGroup 表示消费组为空或包含非法字符。
	ErrInvalidGroup = errors.New("reliablequeue: invalid group")
	// ErrInvalidMessage 表示消息缺少稳定标识或字段无法编码。
	ErrInvalidMessage = errors.New("reliablequeue: invalid message")
	// ErrInvalidHandler 表示订阅未传入 Handler。
	ErrInvalidHandler = errors.New("reliablequeue: invalid handler")
	// ErrSubscriptionExists 表示同一 topic/group 已注册。
	ErrSubscriptionExists = errors.New("reliablequeue: subscription already exists")
	// ErrAlreadyStarted 表示 Queue 已开始运行，不能继续注册订阅或重复启动。
	ErrAlreadyStarted = errors.New("reliablequeue: queue already started")
	// ErrStopped 表示 Queue 已停止。
	ErrStopped = errors.New("reliablequeue: queue stopped")
	// ErrReplicationUnconfirmed 表示 WAIT 未达到要求的副本数。
	ErrReplicationUnconfirmed = errors.New("reliablequeue: replication unconfirmed")
)

// permanentError 标记不应自动重试的 Handler 错误。
type permanentError struct {
	err error
}

// Error 返回底层错误文本。
func (err permanentError) Error() string { return err.err.Error() }

// Unwrap 返回底层错误，供 errors.Is/errors.As 判别。
func (err permanentError) Unwrap() error { return err.err }

// Permanent 把协议损坏、版本不支持等不可重试错误标记为死信错误。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent 返回错误是否要求进入死信而非继续保留在 PEL 重试。
func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}
