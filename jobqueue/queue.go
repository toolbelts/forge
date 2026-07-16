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
	// ErrStopped Stop 之后再 Start / Subscribe / Publish。
	ErrStopped = errors.New("jobqueue: queue stopped")
)

// Queue 单 Redis LIST 后端的极简任务队列。
// 生命周期:New → Subscribe* → Start → (运行中) → Stop。
// Publish 在 Stop 前均可调用,与 worker 是否 Start 无关。
type Queue struct {
	client    redis.UniversalClient
	keyPrefix string

	defaultMaxLen int            // 0 = 不限
	topicMaxLen   map[string]int // 每 topic 覆盖,优先级高于 default;0 表示该 topic 不限
	metrics       Metrics

	// 合并发送配置与运行态。coalesceCfg 由 WithTopicCoalesce 填,
	// coalescers 在 New 末尾按 cfg 构造好,运行期只读,Publish 路径无锁查表。
	coalesceCfg map[string]coalesceConfig
	coalescers  map[string]*coalescer

	mu       sync.Mutex          // 保护 handlers + cancel
	handlers map[string]*handler // topic -> handler
	started  atomic.Bool         // 快路径判定 Start 是否已发生
	stopped  atomic.Bool         // Stop 已触发标志;Publish/Start/Subscribe 据此 fast-fail
	cancel   context.CancelFunc  // New 时初始化为 no-op,Start 后被替换;Stop 调用即可
	wg       sync.WaitGroup      // worker + coalesce flush goroutine 计数
	stopOnce sync.Once
	stopDone chan struct{}
}

// publishScript LPUSH 后按 max 裁剪,返回 {length_after_trim, dropped}。
// max=0 时短路掉 LTRIM,行为等同纯 LPUSH。返回数组省一次 LLEN 往返。
//
// 触发裁剪时一次性裁到 max/2 (high/low watermark),而不是裁到刚好 max:
// 若每次只裁 1 条,稳态下每个 Publish 都触发 LTRIM 与 warn 日志,造成日志洪水;
// 裁到 max/2 后能撑住后续 ~max/2 次 Publish 才再次触发,告警频率降两个量级。
// 副作用是单次丢弃量更大,但反正已经是消费跟不上的状态,丢得快比丢得慢更能保护 Redis 内存。
const publishScript = `
local max = tonumber(ARGV[2])
local n = redis.call('LPUSH', KEYS[1], ARGV[1])
if max > 0 and n > max then
  local keep = math.floor(max / 2)
  if keep < 1 then keep = 1 end
  redis.call('LTRIM', KEYS[1], 0, keep - 1)
  return {keep, n - keep}
end
return {n, 0}
`

// scriptPublish 是 publishScript 的 *redis.Script 句柄,New 时构造一次,Run 走 EVALSHA 缓存。
var scriptPublish = redis.NewScript(publishScript)

// New 构造 Queue。client 必传;keyPrefix 为空时使用 "jobqueue:"。
func New(client redis.UniversalClient, keyPrefix string, opts ...QueueOption) (*Queue, error) {
	if client == nil {
		return nil, ErrNilRedisClient
	}
	if keyPrefix == "" {
		keyPrefix = defaultKeyPrefix
	}
	q := &Queue{
		client:    client,
		keyPrefix: keyPrefix,
		handlers:  make(map[string]*handler),
		cancel:    func() {},
		stopDone:  make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(q)
		}
	}
	if q.metrics == nil {
		q.metrics = NoopMetrics{}
	}
	// 依据 coalesceCfg 实例化 coalescer,运行期只读 — 让 Publish 分流不需加锁。
	if len(q.coalesceCfg) > 0 {
		q.coalescers = make(map[string]*coalescer, len(q.coalesceCfg))
		for topic, cfg := range q.coalesceCfg {
			q.coalescers[topic] = newCoalescer(q, topic, cfg)
		}
	}
	return q, nil
}

// maxLenOf 返回 topic 的 max_len:topicMaxLen 显式配置优先,否则回退 defaultMaxLen。
// 0 表示不限。
func (q *Queue) maxLenOf(topic string) int {
	if n, ok := q.topicMaxLen[topic]; ok {
		return n
	}
	return q.defaultMaxLen
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
	if q.stopped.Load() {
		return ErrStopped
	}
	if q.started.Load() {
		return ErrAlreadyStarted
	}
	if _, ok := q.handlers[topic]; ok {
		return fmt.Errorf("%w: %s", ErrTopicExists, topic)
	}
	q.handlers[topic] = h
	return nil
}

// Publish 把 args JSON 化后推送到 topic 对应 key。
// 不依赖 Start,允许"只生产不消费"的服务调用;Stop 后返回 ErrStopped。
//
// 若 topic 配了 max_len 且 LPUSH 后超过上限,会通过 LTRIM 原子裁掉最老消息
// (FIFO 角度:即将被消费的那一端),且一次裁到 max/2 而非 max,以避免稳态日志洪水;
// 被丢弃的数量通过 Metrics.PublishDropped 上报,Publish 本身仍返回 nil —— 这是
// 预期行为,业务通过指标告警感知。
//
// 若 topic 通过 WithTopicCoalesce 开启了合并模式,Publish 转为 fire-and-forget:
// payload 入内存缓冲后立即返回,达到 maxBatch 或满 window 时批量 LPUSH;
// Redis/Lua 失败仅产生日志 + dropped 指标,进程崩溃会丢缓冲中未 flush 的消息。
//
// args 中的 []byte 会被 json.Marshal 编为 base64 字符串,业务方自行权衡。
func (q *Queue) Publish(ctx context.Context, topic string, args ...any) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if q.stopped.Load() {
		return ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jobqueue: publish %q: %w", topic, err)
	}
	if args == nil {
		args = []any{}
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("jobqueue: marshal args for %q: %w", topic, err)
	}

	if c, ok := q.coalescers[topic]; ok {
		if err := c.enqueue(payload); err != nil {
			return err
		}
		q.metrics.PublishTotal(topic) // 合并模式计数表示 payload 已进入内存缓冲
		return nil
	}
	if q.stopped.Load() {
		return ErrStopped
	}
	return q.publishOne(ctx, topic, payload)
}

// publishOne 同步路径:Lua 一次 LPUSH + 条件 LTRIM,失败向上抛错。
// 合并路径不复用本函数 (它有自己的 batch Lua 与失败语义),指标上报字段相同。
func (q *Queue) publishOne(ctx context.Context, topic string, payload []byte) error {
	key := q.keyOf(topic)
	maxLen := q.maxLenOf(topic)
	raw, err := scriptPublish.Run(ctx, q.client, []string{key}, payload, maxLen).Int64Slice()
	if err != nil {
		return fmt.Errorf("jobqueue: publish %q: %w", topic, err)
	}
	if len(raw) != 2 {
		return fmt.Errorf("jobqueue: publish %q: unexpected script result %v", topic, raw)
	}
	length, dropped := raw[0], raw[1]

	q.metrics.PublishTotal(topic)
	q.metrics.QueueLength(topic, length)
	if dropped > 0 {
		q.metrics.PublishDropped(topic, dropped, DropReasonOverflow)
		log.Ctx(ctx).Warn().
			Str("topic", topic).
			Int("max_len", maxLen).
			Int64("dropped", dropped).
			Int64("kept", length).
			Msg("jobqueue: queue overflow, oldest messages trimmed to half-watermark")
	}
	return nil
}

// Start 起 worker。每个 handler 按 concurrency 起对应数量的 goroutine,
// goroutine 内部根据 batch 选择 BRPOP / BLMPOP 模式。
// parent 取消会传播到 worker ctx。重复 Start 返回 ErrAlreadyStarted。
func (q *Queue) Start(parent context.Context) error {
	if q.stopped.Load() {
		return ErrStopped
	}
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
		for range h.concurrency {
			q.wg.Go(func() { q.runWorker(workerCtx, h) })
		}
	}
	return nil
}

// Stop 取消 worker ctx、终止后续 Publish,并返回 done chan。
// done 在所有 worker 与 coalesce flush 退出后关闭。
// Stop 在 Start 之前调用安全 (cancel 默认是 no-op,wg 为空,done 立即 close)。
//
// 合并模式下的残留缓冲会异步 drain,由 done 表示完成;Stop 本身不会等待 Redis。
func (q *Queue) Stop() <-chan struct{} {
	q.stopOnce.Do(func() {
		q.stopped.Store(true)
		for _, c := range q.coalescers {
			c.stop()
		}
		q.mu.Lock()
		cancel := q.cancel
		q.mu.Unlock()
		cancel()

		go func() {
			q.wg.Wait()
			close(q.stopDone)
		}()
	})

	return q.stopDone
}

// keyOf 返回 topic 对应的 Redis key。
func (q *Queue) keyOf(topic string) string {
	return q.keyPrefix + topic
}
