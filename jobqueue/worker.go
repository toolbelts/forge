package jobqueue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// popTimeout 是 BRPOP/BLMPOP 的阻塞超时。设短一点让 worker 在 ctx 取消之外
// 也有定期"喘气"的机会;真正的取消依赖 go-redis 在 ctx Done 时关连接。
//
// 用 var 而非 const,便于单元测试改小避免等满整个超时窗口。
var popTimeout = 5 * time.Second

// errBackoff 是 pop 出现非预期错误时的简单退避,避免连续打 Redis。
var errBackoff = 200 * time.Millisecond

// runWorker 单个 worker 的主循环。根据 handler.batch 选 BRPOP / BLMPOP。
// 由 q.wg.Go 派生,退出即计数归还。
func (q *Queue) runWorker(ctx context.Context, h *handler) {
	if h.batch > 1 {
		q.blmpopLoop(ctx, h)
		return
	}
	q.brpopLoop(ctx, h)
}

// brpopLoop 单条阻塞拉取并 dispatch。
func (q *Queue) brpopLoop(ctx context.Context, h *handler) {
	key := q.keyOf(h.topic)
	for {
		if ctx.Err() != nil {
			return
		}
		// BRPOP 返回 [key, value]。
		res, err := q.client.BRPop(ctx, popTimeout, key).Result()
		if err != nil {
			if shouldExit(err) {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue // 阻塞超时,正常
			}
			log.Ctx(ctx).Error().Err(err).
				Str("topic", h.topic).
				Msg("jobqueue: brpop failed")
			sleep(ctx, errBackoff)
			continue
		}
		if len(res) != 2 {
			log.Ctx(ctx).Error().
				Str("topic", h.topic).
				Int("len", len(res)).
				Msg("jobqueue: unexpected brpop reply shape, skipped")
			continue
		}
		h.dispatch(ctx, []byte(res[1]))
	}
}

// blmpopLoop 一次最多拉 batch 条,顺序 dispatch。
// 单 worker 内部串行处理,跨 worker (concurrency>1) 并行。
func (q *Queue) blmpopLoop(ctx context.Context, h *handler) {
	key := q.keyOf(h.topic)
	count := int64(h.batch)
	for {
		if ctx.Err() != nil {
			return
		}
		_, vals, err := q.client.BLMPop(ctx, popTimeout, "RIGHT", count, key).Result()
		if err != nil {
			if shouldExit(err) {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			log.Ctx(ctx).Error().Err(err).
				Str("topic", h.topic).
				Msg("jobqueue: blmpop failed")
			sleep(ctx, errBackoff)
			continue
		}
		for _, v := range vals {
			h.dispatch(ctx, []byte(v))
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// shouldExit 判断 pop 错误是否来自 ctx 取消,需要立即退出 worker。
func shouldExit(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// sleep 在 ctx 未取消时睡 d,被取消则立即返回。
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
