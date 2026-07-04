package jobqueue

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// coalesceFlushTimeout 是合并 flush 的内置超时。Publish 上下文在缓冲期间可能已被取消,
// flush 用 background ctx + 此超时保证 Redis 故障不会卡死后台 goroutine。
const coalesceFlushTimeout = 5 * time.Second

// publishBatchScript 在一个 Lua 脚本内按顺序执行 N 次 LPUSH + 条件 LTRIM,
// 返回 {length_after_trim, dropped}。ARGV[1] 是 max,后续为 N 个 payload。
// 逐条套用 high/low watermark,保证最终 LIST 与 dropped 数严格等价于 N 次 Publish。
const publishBatchScript = `
local max = tonumber(ARGV[1])
local dropped = 0
local n = redis.call('LLEN', KEYS[1])
for i = 2, #ARGV do
  n = redis.call('LPUSH', KEYS[1], ARGV[i])
  if max > 0 and n > max then
    local keep = math.floor(max / 2)
    if keep < 1 then keep = 1 end
    redis.call('LTRIM', KEYS[1], 0, keep - 1)
    dropped = dropped + n - keep
    n = keep
  end
end
return {n, dropped}
`

var scriptPublishBatch = redis.NewScript(publishBatchScript)

// coalesceConfig 单 topic 的合并配置,WithTopicCoalesce 校验过后填入,运行期只读。
type coalesceConfig struct {
	window   time.Duration
	maxBatch int
}

// coalescer 单 topic 的合并缓冲。buf / timer / flushing 受 mu 保护。
// 同一 topic 最多一个 flush goroutine;Redis 慢/挂时后续批次在该 goroutine 内串行接着刷。
type coalescer struct {
	mu             sync.Mutex
	buf            [][]byte
	timer          *time.Timer
	flushing       bool
	flushRequested bool
	stopping       bool
	cfg            coalesceConfig
	topic          string
	queue          *Queue
}

// newCoalescer 按 cfg 建空缓冲;timer 留待首条入队再启动。
func newCoalescer(q *Queue, topic string, cfg coalesceConfig) *coalescer {
	return &coalescer{cfg: cfg, topic: topic, queue: q}
}

// enqueue 把 payload 放入缓冲。命中 maxBatch 立即 flush;否则首条入队启动 window 计时。
func (c *coalescer) enqueue(payload []byte) error {
	if c.queue.stopped.Load() {
		return ErrStopped
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queue.stopped.Load() {
		return ErrStopped
	}
	if c.stopping {
		return ErrStopped
	}

	c.buf = append(c.buf, payload)
	fireNow := len(c.buf) >= c.cfg.maxBatch
	if len(c.buf) == 1 && c.timer == nil {
		c.timer = time.AfterFunc(c.cfg.window, c.flushByTimer)
	}
	if fireNow && !c.flushing {
		c.startFlushLocked()
	}
	return nil
}

// flushByTimer time.AfterFunc 回调:加锁摘 batch。若已有 flush 在跑,只标记到期,
// 当前 flush 结束后由同一个 goroutine 串行刷下一批。
func (c *coalescer) flushByTimer() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timer = nil
	if c.stopping || len(c.buf) == 0 {
		return
	}
	if c.flushing {
		c.flushRequested = true
		return
	}
	c.startFlushLocked()
	c.flushRequested = true
}

// stop 进入 drain 阶段:阻止新 enqueue,停掉 timer,并确保残留 buffer 被异步 flush。
func (c *coalescer) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping = true
	c.flushRequested = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if c.flushing {
		return
	}
	c.startFlushLocked()
}

// takeLocked 调用方必须持 mu。最多摘 maxBatch 条,空 buf 返回 nil。
func (c *coalescer) takeLocked() [][]byte {
	if len(c.buf) == 0 {
		return nil
	}
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	n := min(len(c.buf), c.cfg.maxBatch)
	batch := append([][]byte(nil), c.buf[:n]...)
	c.buf = c.buf[n:]
	return batch
}

// startFlushLocked 调用方必须持 mu。它只在 flushing=false 时启动一个串行 flush goroutine。
func (c *coalescer) startFlushLocked() {
	batch := c.takeLocked()
	if batch == nil {
		return
	}
	c.flushing = true
	c.flushRequested = false
	c.queue.wg.Go(func() { c.flushLoop(batch) })
}

func (c *coalescer) flushLoop(batch [][]byte) {
	for {
		c.flush(batch)

		c.mu.Lock()
		if len(c.buf) == 0 {
			c.flushing = false
			c.flushRequested = false
			c.mu.Unlock()
			return
		}
		if c.stopping || c.flushRequested || len(c.buf) >= c.cfg.maxBatch {
			batch = c.takeLocked()
			c.flushRequested = false
			c.mu.Unlock()
			continue
		}
		c.flushing = false
		if c.timer == nil {
			c.timer = time.AfterFunc(c.cfg.window, c.flushByTimer)
		}
		c.mu.Unlock()
		return
	}
}

// flush 调用 publishBatchScript 把 batch 一次写入 Redis。
// 失败一律记 log + dropped 指标,合并模式语义上不向调用方报错。
func (c *coalescer) flush(batch [][]byte) {
	ctx, cancel := context.WithTimeout(context.Background(), coalesceFlushTimeout)
	defer cancel()

	key := c.queue.keyOf(c.topic)
	maxLen := c.queue.maxLenOf(c.topic)

	argv := make([]any, 0, len(batch)+1)
	argv = append(argv, maxLen)
	for _, p := range batch {
		argv = append(argv, p)
	}
	raw, err := scriptPublishBatch.Run(ctx, c.queue.client, []string{key}, argv...).Int64Slice()
	if err != nil || len(raw) != 2 {
		log.Ctx(ctx).Error().
			Err(err).
			Str("topic", c.topic).
			Int("batch", len(batch)).
			Msg("jobqueue: coalesced flush failed, messages dropped")
		c.queue.metrics.PublishDropped(c.topic, int64(len(batch)), DropReasonFlushFailed)
		return
	}
	length, dropped := raw[0], raw[1]

	c.queue.metrics.CoalesceFlushTotal(c.topic)
	c.queue.metrics.CoalesceBatchSize(c.topic, int64(len(batch)))
	c.queue.metrics.QueueLength(c.topic, length)
	if dropped > 0 {
		c.queue.metrics.PublishDropped(c.topic, dropped, DropReasonOverflow)
		log.Ctx(ctx).Warn().
			Str("topic", c.topic).
			Int("max_len", maxLen).
			Int64("dropped", dropped).
			Int64("kept", length).
			Msg("jobqueue: queue overflow on coalesced flush, oldest messages trimmed to half-watermark")
	}
}
