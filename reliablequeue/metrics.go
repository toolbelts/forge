package reliablequeue

import "time"

// Metrics 接收可靠队列的发布、消费、恢复和死信指标。
// 实现必须并发安全且不得 panic。
type Metrics interface {
	// PublishTotal 记录一条已被 Redis XADD 接受的消息。
	PublishTotal(topic string)
	// PublishError 记录一条未完成发布流程的消息。
	PublishError(topic string)
	// ReplicaWaitTimeout 记录 WAIT 未达到要求副本数。
	ReplicaWaitTimeout(topic string)
	// ConsumeTotal 记录一次 Handler 调用，redelivered 表示来自 Pending 恢复。
	ConsumeTotal(topic string, redelivered bool)
	// HandlerError 记录 Handler 返回错误，permanent 表示消息将进入死信。
	HandlerError(topic string, permanent bool)
	// Pending 记录消费组当前 Pending 数量采样。
	Pending(topic, group string, count int64)
	// ProcessingDuration 记录一次 Handler 调用耗时。
	ProcessingDuration(topic string, duration time.Duration)
	// DlqTotal 记录一条成功发布到死信 topic 的消息。
	DlqTotal(topic string)
}

// NoopMetrics 丢弃全部指标，是 Queue 默认实现。
type NoopMetrics struct{}

func (NoopMetrics) PublishTotal(string)                      {}
func (NoopMetrics) PublishError(string)                      {}
func (NoopMetrics) ReplicaWaitTimeout(string)                {}
func (NoopMetrics) ConsumeTotal(string, bool)                {}
func (NoopMetrics) HandlerError(string, bool)                {}
func (NoopMetrics) Pending(string, string, int64)            {}
func (NoopMetrics) ProcessingDuration(string, time.Duration) {}
func (NoopMetrics) DlqTotal(string)                          {}
