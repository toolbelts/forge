package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const defaultKeyPrefix = "jobqueue:"

// 错误集合,均供调用方 errors.Is 判别。
var (
	// ErrNilRedisClient New 时未传 redis 客户端。
	ErrNilRedisClient = errors.New("jobqueue: nil redis client")
	// ErrInvalidFunc Subscribe 时 fn 形态不合法 (非函数 / 缺 ctx / 出参非 1 个 error)。
	ErrInvalidFunc = errors.New("jobqueue: invalid handler func")
	// ErrEmptyTopic Subscribe/Publish 时 topic 为空。
	ErrEmptyTopic = errors.New("jobqueue: empty topic")
	// ErrTopicExists 同一 topic 已注册过 handler。
	ErrTopicExists = errors.New("jobqueue: topic already subscribed")
	// ErrAlreadyStarted Start 之后再 Subscribe / 重复 Start。
	ErrAlreadyStarted = errors.New("jobqueue: queue already started")
)

// Queue 单 Redis LIST 后端的极简任务队列。
// 生命周期:New → Subscribe* → Start → (运行中) → Stop。
// Publish 任何时候都可调用,与 worker 状态无关。
type Queue struct {
	client    *redis.Client
	keyPrefix string

	mu       sync.Mutex          // 保护 handlers + cancel
	handlers map[string]*handler // topic -> handler
	started  atomic.Bool         // 快路径判定 Start 是否已发生
	cancel   context.CancelFunc  // New 时初始化为 no-op,Start 后被替换;Stop 调用即可
	wg       sync.WaitGroup      // worker goroutine 计数
}

// New 构造 Queue。client 必传;keyPrefix 为空时使用 "jobqueue:"。
func New(client *redis.Client, keyPrefix string) (*Queue, error) {
	if client == nil {
		return nil, ErrNilRedisClient
	}
	if keyPrefix == "" {
		keyPrefix = defaultKeyPrefix
	}
	return &Queue{
		client:    client,
		keyPrefix: keyPrefix,
		handlers:  make(map[string]*handler),
		cancel:    func() {},
	}, nil
}

// Subscribe 注册 topic 的处理函数。fn 形态:
//
//	func(ctx context.Context, args...) error
//
// args 数量与类型由 fn 决定,Publish 必须按相同顺序传值。
// 必须在 Start 前调用;Start 后调用返回 ErrAlreadyStarted。
func (q *Queue) Subscribe(topic string, fn any, opts ...SubscribeOption) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	h, err := newHandler(topic, fn)
	if err != nil {
		return err
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started.Load() {
		return ErrAlreadyStarted
	}
	if _, ok := q.handlers[topic]; ok {
		return fmt.Errorf("%w: %s", ErrTopicExists, topic)
	}
	q.handlers[topic] = h
	return nil
}

// Publish 把 args JSON 化后 LPUSH 到 topic 对应 key。
// 不依赖 Start,允许"只生产不消费"的服务调用。
//
// args 中的 []byte 会被 json.Marshal 编为 base64 字符串,业务方自行权衡。
func (q *Queue) Publish(ctx context.Context, topic string, args ...any) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if args == nil {
		args = []any{}
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("jobqueue: marshal args for %q: %w", topic, err)
	}
	if err := q.client.LPush(ctx, q.keyOf(topic), payload).Err(); err != nil {
		return fmt.Errorf("jobqueue: lpush %q: %w", topic, err)
	}
	return nil
}

// Start 起 worker。每个 handler 按 concurrency 起对应数量的 goroutine,
// goroutine 内部根据 batch 选择 BRPOP / BLMPOP 模式。
// parent 取消会传播到 worker ctx。重复 Start 返回 ErrAlreadyStarted。
func (q *Queue) Start(parent context.Context) error {
	if !q.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	q.mu.Lock()
	snapshot := maps.Clone(q.handlers)
	workerCtx, cancel := context.WithCancel(parent)
	q.cancel = cancel
	q.mu.Unlock()

	if len(snapshot) == 0 {
		log.Ctx(parent).Warn().Msg("jobqueue: no subscribers, only publish path is active")
		return nil
	}

	for _, h := range snapshot {
		for i := 0; i < h.concurrency; i++ {
			q.wg.Add(1)
			go q.runWorker(workerCtx, h)
		}
	}
	return nil
}

// Stop 取消 worker ctx 并返回 done chan,所有 worker 退出后被关闭。
// Stop 在 Start 之前调用安全 (cancel 默认是 no-op,wg 为空,done 立即 close)。
func (q *Queue) Stop() <-chan struct{} {
	q.mu.Lock()
	cancel := q.cancel
	q.mu.Unlock()
	cancel()

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()
	return done
}

// keyOf 返回 topic 对应的 Redis key。
func (q *Queue) keyOf(topic string) string {
	return q.keyPrefix + topic
}
