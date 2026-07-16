package reliablequeue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	messageIdField = "message_id"
	payloadField   = "payload"
	metadataField  = "metadata"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// stickyClient 是启用 WAIT 时必须具备的固定连接能力。
type stickyClient interface {
	redis.UniversalClient
	Conn() *redis.Conn
}

// Queue 基于 Redis Streams 提供至少一次发布和消费。
type Queue struct {
	client redis.UniversalClient
	sticky stickyClient

	keyPrefix        string
	dlqTopic         string
	blockTimeout     time.Duration
	claimIdle        time.Duration
	recoveryInterval time.Duration
	batchSize        int64
	waitReplicas     int
	waitTimeout      time.Duration
	metrics          Metrics
	consumerPrefix   string

	mu            sync.Mutex
	subscriptions map[string]*subscription
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	started       atomic.Bool
	stopped       atomic.Bool
	stopOnce      sync.Once
	stopDone      chan struct{}
}

// subscription 是一个 topic/group 的消费配置。
type subscription struct {
	topic          string
	group          string
	handler        Handler
	dlqTopic       string
	concurrency    int
	deleteAfterAck bool
	semaphore      chan struct{}
}

// New 创建 ReliableQueue；启用副本 WAIT 时 client 必须支持 Conn 固定连接。
func New(client redis.UniversalClient, keyPrefix string, opts ...QueueOption) (*Queue, error) {
	if client == nil {
		return nil, ErrNilRedisClient
	}
	if keyPrefix == "" {
		keyPrefix = defaultKeyPrefix
	}
	if !strings.HasSuffix(keyPrefix, ":") {
		keyPrefix += ":"
	}
	hostname, _ := os.Hostname()
	queue := &Queue{
		client: client, keyPrefix: keyPrefix,
		dlqTopic: defaultDlqTopic, blockTimeout: defaultBlockTimeout,
		claimIdle: defaultClaimIdle, recoveryInterval: defaultRecoveryPeriod,
		batchSize: defaultBatchSize, waitTimeout: defaultWaitTimeout,
		metrics: NoopMetrics{}, subscriptions: make(map[string]*subscription),
		consumerPrefix: fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.NewString()),
		cancel:         func() {}, stopDone: make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(queue)
		}
	}
	if err := validateName(queue.dlqTopic, ErrInvalidTopic); err != nil {
		return nil, fmt.Errorf("default dlq topic: %w", err)
	}
	if queue.waitReplicas > 0 {
		sticky, ok := client.(stickyClient)
		if !ok {
			return nil, fmt.Errorf("%w: wait_replicas requires Conn support", ErrUnsupportedClient)
		}
		queue.sticky = sticky
	}
	return queue, nil
}

// Subscribe 注册一个显式消费组；必须在 Start 前调用。
func (queue *Queue) Subscribe(
	topic string,
	group string,
	handler Handler,
	opts ...SubscribeOption,
) error {
	if err := validateName(topic, ErrInvalidTopic); err != nil {
		return err
	}
	if err := validateName(group, ErrInvalidGroup); err != nil {
		return err
	}
	if handler == nil {
		return ErrInvalidHandler
	}
	if queue.stopped.Load() {
		return ErrStopped
	}
	if queue.started.Load() {
		return ErrAlreadyStarted
	}
	subscription := &subscription{
		topic: topic, group: group, handler: handler,
		dlqTopic: queue.dlqTopic, concurrency: defaultHandlerParallel,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(subscription)
		}
	}
	if err := validateName(subscription.dlqTopic, ErrInvalidTopic); err != nil {
		return fmt.Errorf("subscription dlq topic: %w", err)
	}
	subscription.semaphore = make(chan struct{}, subscription.concurrency)
	key := subscriptionKey(topic, group)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.stopped.Load() {
		return ErrStopped
	}
	if queue.started.Load() {
		return ErrAlreadyStarted
	}
	if _, exists := queue.subscriptions[key]; exists {
		return fmt.Errorf("%w: %s/%s", ErrSubscriptionExists, topic, group)
	}
	queue.subscriptions[key] = subscription
	return nil
}

// Publish 把消息追加到 topic Stream，并按配置等待副本确认。
func (queue *Queue) Publish(ctx context.Context, topic string, message Message) error {
	if queue.stopped.Load() {
		return ErrStopped
	}
	return queue.publishMany(ctx, []Publication{{Topic: topic, Message: message}})
}

// Start 创建消费组并启动新消息和 Pending 恢复循环。
func (queue *Queue) Start(parent context.Context) error {
	if queue.stopped.Load() {
		return ErrStopped
	}
	if !queue.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	queue.mu.Lock()
	subscriptions := make([]*subscription, 0, len(queue.subscriptions))
	for _, subscription := range queue.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	workerCtx, cancel := context.WithCancel(parent)
	queue.cancel = cancel
	queue.mu.Unlock()

	for _, subscription := range subscriptions {
		if err := queue.ensureGroup(workerCtx, subscription); err != nil {
			cancel()
			return err
		}
	}
	if len(subscriptions) == 0 {
		log.Ctx(parent).Warn().Msg("reliablequeue: no subscriptions, publish only")
		return nil
	}
	for _, subscription := range subscriptions {
		for index := range subscription.concurrency {
			consumer := fmt.Sprintf("%s-%s-%d", queue.consumerPrefix, subscription.group, index)
			queue.wg.Go(func() { queue.readLoop(workerCtx, subscription, consumer) })
		}
		consumer := fmt.Sprintf("%s-%s-recovery", queue.consumerPrefix, subscription.group)
		queue.wg.Go(func() { queue.recoveryLoop(workerCtx, subscription, consumer) })
	}
	return nil
}

// Stop 取消读取并返回在所有 worker 退出后关闭的 channel；重复调用返回同一 channel。
func (queue *Queue) Stop() <-chan struct{} {
	queue.stopOnce.Do(func() {
		queue.stopped.Store(true)
		queue.mu.Lock()
		cancel := queue.cancel
		queue.mu.Unlock()
		cancel()
		go func() {
			queue.wg.Wait()
			close(queue.stopDone)
		}()
	})
	return queue.stopDone
}

// publishMany 先完整校验 Publications，再逐条 XADD 并按配置 WAIT。
func (queue *Queue) publishMany(ctx context.Context, publications []Publication) error {
	if len(publications) == 0 {
		return nil
	}
	encoded := make([]encodedPublication, len(publications))
	for index, publication := range publications {
		item, err := queue.encodePublication(publication)
		if err != nil {
			return err
		}
		encoded[index] = item
	}
	if queue.waitReplicas == 0 {
		return queue.appendPublications(ctx, queue.client, encoded)
	}
	conn := queue.sticky.Conn()
	defer conn.Close()
	if err := queue.appendPublications(ctx, conn, encoded); err != nil {
		return err
	}
	replicas, err := conn.Wait(ctx, queue.waitReplicas, queue.waitTimeout).Result()
	if err != nil {
		queue.recordReplicationFailure(encoded)
		return fmt.Errorf("%w: wait replicas: %v", ErrReplicationUnconfirmed, err)
	}
	if replicas < int64(queue.waitReplicas) {
		queue.recordReplicationFailure(encoded)
		return fmt.Errorf(
			"%w: required %d replicas, confirmed %d",
			ErrReplicationUnconfirmed, queue.waitReplicas, replicas,
		)
	}
	return nil
}

// encodedPublication 保存已校验且完成 metadata 编码的发布参数。
type encodedPublication struct {
	topic    string
	stream   string
	message  Message
	metadata string
}

// encodePublication 校验并编码一条输出，确保批量发布不会因后项参数错误产生部分写。
func (queue *Queue) encodePublication(publication Publication) (encodedPublication, error) {
	if err := validateName(publication.Topic, ErrInvalidTopic); err != nil {
		return encodedPublication{}, err
	}
	if publication.Message.Id == "" || len(publication.Message.Id) > 128 {
		return encodedPublication{}, fmt.Errorf("%w: id must contain 1..128 bytes", ErrInvalidMessage)
	}
	metadata, err := json.Marshal(publication.Message.Metadata)
	if err != nil {
		return encodedPublication{}, fmt.Errorf("%w: encode metadata: %v", ErrInvalidMessage, err)
	}
	return encodedPublication{
		topic: publication.Topic, stream: queue.streamKey(publication.Topic),
		message: publication.Message.Clone(), metadata: string(metadata),
	}, nil
}

// appendPublications 执行 XADD；调用方负责需要的副本复制等待。
func (queue *Queue) appendPublications(
	ctx context.Context,
	client redis.Cmdable,
	publications []encodedPublication,
) error {
	for _, publication := range publications {
		if err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: publication.stream,
			Values: []any{
				messageIdField, publication.message.Id,
				payloadField, string(publication.message.Payload),
				metadataField, publication.metadata,
			},
		}).Err(); err != nil {
			queue.metrics.PublishError(publication.topic)
			return fmt.Errorf("reliablequeue: publish %q: %w", publication.topic, err)
		}
		queue.metrics.PublishTotal(publication.topic)
	}
	return nil
}

// recordReplicationFailure 记录一次批量发布未完成副本确认的逐 topic 指标。
func (queue *Queue) recordReplicationFailure(publications []encodedPublication) {
	for _, publication := range publications {
		queue.metrics.PublishError(publication.topic)
		queue.metrics.ReplicaWaitTimeout(publication.topic)
	}
}

// ensureGroup 幂等创建消费组，从 0 开始以覆盖组创建前已经存在的消息。
func (queue *Queue) ensureGroup(ctx context.Context, subscription *subscription) error {
	err := queue.client.XGroupCreateMkStream(
		ctx, queue.streamKey(subscription.topic), subscription.group, "0",
	).Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf(
		"reliablequeue: create group %q for %q: %w",
		subscription.group, subscription.topic, err,
	)
}

// streamKey 返回 topic 对应的 Redis Stream key。
func (queue *Queue) streamKey(topic string) string {
	return queue.keyPrefix + "stream:" + topic
}

// subscriptionKey 返回进程内订阅唯一键。
func subscriptionKey(topic, group string) string { return topic + "\x00" + group }

// validateName 校验 topic/group 只使用适合 Redis key 和日志标签的稳定字符。
func validateName(name string, sentinel error) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("%w: %q", sentinel, name)
	}
	return nil
}
