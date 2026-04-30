// Package cron 是 github.com/robfig/cron/v3 的薄封装,统一日志格式与执行语义,
// 把 zerolog 接到调度器内部事件,并默认应用 SkipIfStillRunning + Recover 中间件:
//
//   - 上一次任务还没跑完时,本次触发被丢弃(打 Warn 日志而非排队/并发)
//   - 任务 panic 不影响调度器,Recover 中间件捕获并打 Error
//   - 表达式使用 6 字段(秒 分 时 日 月 周),同时兼容 @every / @hourly 等 descriptor
//
// 业务方拿到 *Cron 后调用 AddJob(name, spec, fn) 注册任务即可,不必关心 robfig/cron 的细节。
package cron

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	rcron "github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Func 是任务函数签名。Cron.Stop 会取消 ctx,任务应及时收手。
type Func func(ctx context.Context) error

// Cron 包装 robfig cron 调度器,封装日志与重叠跳过语义。
type Cron struct {
	raw *rcron.Cron

	mu      sync.Mutex
	rootCtx context.Context
	cancel  context.CancelFunc
}

// New 用给定时区构造调度器。loc 为 nil 时使用 time.Local。
func New(loc *time.Location) *Cron {
	if loc == nil {
		loc = time.Local
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	adapter := zerologAdapter{}
	raw := rcron.New(
		rcron.WithLocation(loc),
		rcron.WithSeconds(),
		rcron.WithLogger(adapter),
		rcron.WithChain(
			rcron.SkipIfStillRunning(adapter),
			rcron.Recover(adapter),
		),
	)
	return &Cron{
		raw:     raw,
		rootCtx: rootCtx,
		cancel:  cancel,
	}
}

// AddJob 注册任务。name 用于日志识别;spec 为 6 字段表达式或 descriptor。
// 返回的 EntryID 可用于后续 Remove。包装层负责打执行起止日志,
// SkipIfStillRunning / Recover 由全局 chain 兜底,这里不重复处理。
func (c *Cron) AddJob(name, spec string, fn Func) (rcron.EntryID, error) {
	if fn == nil {
		return 0, errors.New("cron: AddJob fn is nil")
	}
	if name == "" {
		name = "anonymous"
	}

	job := rcron.FuncJob(func() {
		c.runOnce(name, spec, fn)
	})

	id, err := c.raw.AddJob(spec, job)
	if err != nil {
		return 0, fmt.Errorf("cron: add job %q (spec %q): %w", name, spec, err)
	}

	log.Ctx(c.currentCtx()).Info().
		Str("job", name).
		Int("entry_id", int(id)).
		Str("spec", spec).
		Time("next", c.raw.Entry(id).Next).
		Msg("cron job registered")
	return id, nil
}

// Start 启动调度(非阻塞)。parent 作为任务函数的父 ctx,Stop 时会被取消。
func (c *Cron) Start(parent context.Context) {
	c.mu.Lock()
	// 用新的 parent 重建 rootCtx,旧的 cancel 触发以防漏挂。
	c.cancel()
	rootCtx, cancel := context.WithCancel(parent)
	c.rootCtx = rootCtx
	c.cancel = cancel
	c.mu.Unlock()
	c.raw.Start()
}

// Stop 停止接收新触发并等待运行中任务结束。
// 返回的 ctx 在所有任务收尾后 Done;同时取消任务函数感知到的 ctx 以促其尽快退出。
func (c *Cron) Stop() context.Context {
	c.mu.Lock()
	c.cancel()
	c.mu.Unlock()
	return c.raw.Stop()
}

// Entries 返回当前所有 entry 的快照,主要用于 Shutdown 超时时打日志诊断。
func (c *Cron) Entries() []rcron.Entry {
	return c.raw.Entries()
}

// Raw 暴露原生 *cron.Cron。通过 Raw 旁路注册的 job 不会获得本封装的日志包装。
func (c *Cron) Raw() *rcron.Cron {
	return c.raw
}

// runOnce 执行单次任务并附带计时与结果日志。panic 由 Recover 中间件兜底。
func (c *Cron) runOnce(name, spec string, fn Func) {
	parent := c.currentCtx()
	logger := log.Ctx(parent).With().Str("job", name).Str("spec", spec).Logger()
	ctx := logger.WithContext(parent)

	logger.Info().Msg("cron job start")
	start := time.Now()
	err := fn(ctx)
	dur := time.Since(start)

	evt := logger.Info()
	if err != nil {
		evt = logger.Error().Err(err)
	}
	evt.Dur("duration", dur).Msg("cron job done")
}

func (c *Cron) currentCtx() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rootCtx
}

// zerologAdapter 把 robfig/cron 的 Logger 转发到 zerolog,
// SkipIfStillRunning 跳过、调度器自身的 schedule/wake 事件都会经过这里。
type zerologAdapter struct{}

// Info 输出调度器例行事件。SkipIfStillRunning 跳过(msg="skip")也走这里,
// 我们提到 Warn 级别以便监控更容易抓到"任务积压"信号。
func (zerologAdapter) Info(msg string, keysAndValues ...any) {
	evt := log.Info().Str("source", "cron")
	if msg == "skip" {
		evt = log.Warn().Str("source", "cron")
	}
	appendKV(evt, keysAndValues)
	evt.Msg(msg)
}

// Error 输出调度器异常,Recover 中间件捕获 panic 时走这里。
func (zerologAdapter) Error(err error, msg string, keysAndValues ...any) {
	evt := log.Error().Err(err).Str("source", "cron")
	appendKV(evt, keysAndValues)
	evt.Msg(msg)
}

// appendKV 把 robfig 风格的 (k, v, k, v, ...) 平展到 zerolog 事件。
// k 不是 string 时跳过该对,避免格式不规范导致日志丢失。
func appendKV(evt *zerolog.Event, kvs []any) {
	for i := 0; i+1 < len(kvs); i += 2 {
		key, ok := kvs[i].(string)
		if !ok {
			continue
		}
		evt.Interface(key, kvs[i+1])
	}
}
