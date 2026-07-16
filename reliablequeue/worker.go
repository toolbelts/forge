package reliablequeue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const workerErrorBackoff = 200 * time.Millisecond

const ackDeleteScriptSource = `
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged > 0 then
    redis.call('XDEL', KEYS[1], ARGV[2])
end
return acknowledged
`

var ackDeleteScript = redis.NewScript(ackDeleteScriptSource)

// delivery 保存一条 Redis Stream entry 及其消费上下文。
type delivery struct {
	topic       string
	stream      string
	group       string
	streamId    string
	message     Message
	redelivered bool
}

// readLoop 阻塞读取未投递过的新消息，并同步交给受并发信号量限制的 Handler。
func (queue *Queue) readLoop(ctx context.Context, subscription *subscription, consumer string) {
	stream := queue.streamKey(subscription.topic)
	for {
		if ctx.Err() != nil {
			return
		}
		streams, err := queue.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: subscription.group, Consumer: consumer,
			Streams: []string{stream, ">"}, Count: queue.batchSize, Block: queue.blockTimeout,
		}).Result()
		if err != nil {
			if shouldStopWorker(ctx, err) {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			log.Ctx(ctx).Error().Err(err).
				Str("topic", subscription.topic).
				Str("group", subscription.group).
				Msg("reliablequeue: read group failed")
			sleepContext(ctx, workerErrorBackoff)
			continue
		}
		for _, result := range streams {
			for _, entry := range result.Messages {
				queue.dispatchEntry(ctx, subscription, entry, false)
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

// recoveryLoop 周期扫描 PEL，并用 XAUTOCLAIM 接管超过 claimIdle 的消息。
func (queue *Queue) recoveryLoop(ctx context.Context, subscription *subscription, consumer string) {
	cursor := "0-0"
	ticker := time.NewTicker(queue.recoveryInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		entries, next, err := queue.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: queue.streamKey(subscription.topic), Group: subscription.group,
			Consumer: consumer, MinIdle: queue.claimIdle, Start: cursor, Count: queue.batchSize,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			if shouldStopWorker(ctx, err) {
				return
			}
			log.Ctx(ctx).Error().Err(err).
				Str("topic", subscription.topic).
				Str("group", subscription.group).
				Msg("reliablequeue: auto claim failed")
		} else {
			if next == "" || next == "0-0" {
				cursor = "0-0"
			} else {
				cursor = next
			}
			for _, entry := range entries {
				queue.dispatchEntry(ctx, subscription, entry, true)
				if ctx.Err() != nil {
					return
				}
			}
			queue.recordPending(ctx, subscription)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// dispatchEntry 获取订阅并发名额，解码消息并执行完整确认流程。
func (queue *Queue) dispatchEntry(
	ctx context.Context,
	subscription *subscription,
	entry redis.XMessage,
	redelivered bool,
) {
	select {
	case subscription.semaphore <- struct{}{}:
		defer func() { <-subscription.semaphore }()
	case <-ctx.Done():
		return
	}
	message, err := decodeEntry(entry)
	if err != nil {
		queue.metrics.HandlerError(subscription.topic, true)
		fallback := Message{Id: entry.ID, Metadata: map[string]string{}}
		if payload, ok := streamValue(entry.Values, payloadField); ok {
			fallback.Payload = []byte(payload)
		}
		queue.handlePermanent(ctx, subscription, delivery{
			topic: subscription.topic, stream: queue.streamKey(subscription.topic),
			group: subscription.group, streamId: entry.ID, message: fallback, redelivered: redelivered,
		}, err)
		return
	}
	queue.handleDelivery(ctx, subscription, delivery{
		topic: subscription.topic, stream: queue.streamKey(subscription.topic),
		group: subscription.group, streamId: entry.ID, message: message, redelivered: redelivered,
	})
}

// handleDelivery 调用 Handler，先提交输出 Publications，再确认输入消息。
func (queue *Queue) handleDelivery(ctx context.Context, subscription *subscription, delivery delivery) {
	queue.metrics.ConsumeTotal(delivery.topic, delivery.redelivered)
	startedAt := time.Now()
	result, err := callHandler(ctx, subscription.handler, delivery.message.Clone())
	queue.metrics.ProcessingDuration(delivery.topic, time.Since(startedAt))
	if err != nil {
		permanent := IsPermanent(err)
		queue.metrics.HandlerError(delivery.topic, permanent)
		if permanent {
			queue.handlePermanent(ctx, subscription, delivery, err)
			return
		}
		log.Ctx(ctx).Error().Err(err).
			Str("topic", delivery.topic).
			Str("group", delivery.group).
			Str("message_id", delivery.message.Id).
			Bool("redelivered", delivery.redelivered).
			Msg("reliablequeue: handler failed, keep pending")
		return
	}
	if err := queue.publishMany(ctx, result.Publications); err != nil {
		if errors.Is(err, ErrInvalidTopic) || errors.Is(err, ErrInvalidMessage) {
			queue.metrics.HandlerError(delivery.topic, true)
			queue.handlePermanent(ctx, subscription, delivery, err)
			return
		}
		log.Ctx(ctx).Error().Err(err).
			Str("topic", delivery.topic).
			Str("group", delivery.group).
			Str("message_id", delivery.message.Id).
			Msg("reliablequeue: publish handler result failed, keep pending")
		return
	}
	if err := queue.acknowledge(ctx, subscription, delivery); err != nil {
		log.Ctx(ctx).Error().Err(err).
			Str("topic", delivery.topic).
			Str("group", delivery.group).
			Str("message_id", delivery.message.Id).
			Msg("reliablequeue: acknowledge failed, keep pending")
		return
	}
	log.Ctx(ctx).Debug().
		Str("topic", delivery.topic).
		Str("group", delivery.group).
		Str("message_id", delivery.message.Id).
		Bool("redelivered", delivery.redelivered).
		Msg("reliablequeue: handler done")
}

// handlePermanent 发布死信并仅在成功后确认原消息。
func (queue *Queue) handlePermanent(
	ctx context.Context,
	subscription *subscription,
	delivery delivery,
	cause error,
) {
	message := delivery.message.Clone()
	if message.Id == "" {
		message.Id = delivery.streamId
	}
	if message.Metadata == nil {
		message.Metadata = make(map[string]string)
	}
	message.Metadata["reliablequeue_original_topic"] = delivery.topic
	message.Metadata["reliablequeue_original_group"] = delivery.group
	message.Metadata["reliablequeue_original_stream_id"] = delivery.streamId
	message.Metadata["reliablequeue_error"] = truncateError(cause, 1024)
	if err := queue.publishMany(ctx, []Publication{{Topic: subscription.dlqTopic, Message: message}}); err != nil {
		log.Ctx(ctx).Error().Err(err).
			Str("topic", delivery.topic).
			Str("group", delivery.group).
			Str("message_id", message.Id).
			Msg("reliablequeue: publish dead letter failed, keep pending")
		return
	}
	queue.metrics.DlqTotal(subscription.dlqTopic)
	if err := queue.acknowledge(ctx, subscription, delivery); err != nil {
		log.Ctx(ctx).Error().Err(err).
			Str("topic", delivery.topic).
			Str("group", delivery.group).
			Str("message_id", message.Id).
			Msg("reliablequeue: acknowledge dead letter failed")
	}
}

// acknowledge 从 PEL 确认消息，并按订阅配置选择是否删除 Stream entry。
func (queue *Queue) acknowledge(
	ctx context.Context,
	subscription *subscription,
	delivery delivery,
) error {
	if !subscription.deleteAfterAck {
		if err := queue.client.XAck(ctx, delivery.stream, delivery.group, delivery.streamId).Err(); err != nil {
			return fmt.Errorf("xack: %w", err)
		}
		return nil
	}
	if _, err := ackDeleteScript.Run(
		ctx, queue.client, []string{delivery.stream}, delivery.group, delivery.streamId,
	).Int64(); err != nil {
		return fmt.Errorf("xack and xdel: %w", err)
	}
	return nil
}

// recordPending 采样当前消费组 PEL 数量，失败仅记调试日志。
func (queue *Queue) recordPending(ctx context.Context, subscription *subscription) {
	pending, err := queue.client.XPending(
		ctx, queue.streamKey(subscription.topic), subscription.group,
	).Result()
	if err != nil {
		log.Ctx(ctx).Debug().Err(err).
			Str("topic", subscription.topic).
			Str("group", subscription.group).
			Msg("reliablequeue: pending sample failed")
		return
	}
	queue.metrics.Pending(subscription.topic, subscription.group, pending.Count)
}

// decodeEntry 把 Redis Stream fields 解码为稳定 Message。
func decodeEntry(entry redis.XMessage) (Message, error) {
	messageId, ok := streamValue(entry.Values, messageIdField)
	if !ok || messageId == "" || len(messageId) > 128 {
		return Message{}, fmt.Errorf("%w: missing or invalid message id", ErrInvalidMessage)
	}
	payload, ok := streamValue(entry.Values, payloadField)
	if !ok {
		return Message{}, fmt.Errorf("%w: missing payload", ErrInvalidMessage)
	}
	metadataRaw, ok := streamValue(entry.Values, metadataField)
	if !ok || metadataRaw == "" {
		metadataRaw = "{}"
	}
	metadata := make(map[string]string)
	if err := json.Unmarshal([]byte(metadataRaw), &metadata); err != nil {
		return Message{}, fmt.Errorf("%w: decode metadata: %v", ErrInvalidMessage, err)
	}
	return Message{Id: messageId, Payload: []byte(payload), Metadata: metadata}, nil
}

// streamValue 把 go-redis/miniredis 返回的字符串或字节字段统一为 string。
func streamValue(values map[string]any, key string) (string, bool) {
	value, ok := values[key]
	if !ok {
		return "", false
	}
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return fmt.Sprint(typed), true
	}
}

// callHandler 捕获 Handler panic，并作为可重试错误保留消息。
func callHandler(ctx context.Context, handler Handler, message Message) (result Result, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, message)
}

// shouldStopWorker 判断错误是否由退出上下文触发。
func shouldStopWorker(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// sleepContext 在上下文取消时提前结束退避。
func sleepContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// truncateError 限制死信 metadata 中的错误文本长度。
func truncateError(err error, limit int) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}
