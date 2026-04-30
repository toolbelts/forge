package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	gpcron "github.com/toolbelts/forge/cron"
	"github.com/toolbelts/forge/ioc"
)

// CronProvider 定时任务提供者:Register 阶段构造 *cron.Cron 并注入容器,
// 业务方在自己的 Setup 中通过 MustGetCron(ctx) 拿到调度器再 AddJob。
//
// 行为约定:
//   - cron.enabled=false 时静默跳过(不向容器注入实例),业务方调用 MustGetCron 会 panic,
//     这是有意为之 —— 关闭定时任务时不应允许业务方依赖它
//   - 6 字段表达式(秒级)+ 全局 SkipIfStillRunning + Recover,详见 pkg/cron
//   - cron.timezone 解析失败立即返回错误,避免 prod 时区错配跑错时间
//   - Serve 仅在 enabled 时启动调度器并阻塞到 ctx 取消
//   - Shutdown 通过 cron.Stop() 等待运行中任务收尾,超时阈值 cron.shutdown_timeout
type CronProvider struct {
	enabled         bool
	cron            *gpcron.Cron
	timezone        string
	shutdownTimeout time.Duration
}

// Register 读 cron.* 配置,构造调度器并注入容器。enabled=false 静默跳过。
func (p *CronProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("cron.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "cron").Msg("cron disabled, skip register")
		return nil
	}

	p.timezone = v.GetString("cron.timezone")
	loc := time.Local
	if p.timezone != "" {
		l, err := time.LoadLocation(p.timezone)
		if err != nil {
			return fmt.Errorf("cron: load timezone %s: %w", p.timezone, err)
		}
		loc = l
	}

	p.shutdownTimeout = v.GetDuration("cron.shutdown_timeout")

	p.cron = gpcron.New(loc)
	ioc.MustInstance(ctx, p.cron)

	log.Ctx(ctx).Info().
		Str("provider", "cron").
		Str("timezone", loc.String()).
		Dur("shutdown_timeout", p.shutdownTimeout).
		Msg("cron registered")
	return nil
}

// Setup 无操作。业务方在自己的 Setup 里 MustGetCron(ctx).AddJob(...)。
func (p *CronProvider) Setup(ctx context.Context) error {
	return nil
}

// Serve 启动调度器并阻塞到 ctx 取消。enabled=false 时仅 wait,不参与调度。
func (p *CronProvider) Serve(ctx context.Context) error {
	if !p.enabled {
		<-ctx.Done()
		return nil
	}
	p.cron.Start(ctx)
	<-ctx.Done()
	return nil
}

// Shutdown 等待运行中任务结束,超过 shutdownTimeout 仍未结束则记录还在跑的 entry 后返回。
// Go 没有强杀 goroutine 的能力,超时只代表本 Provider 不再阻塞 ioc 退出流程,
// 任务函数本身会通过 ctx.Done() 收到取消信号。
func (p *CronProvider) Shutdown(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	stopCtx := p.cron.Stop()
	timeout := p.shutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-stopCtx.Done():
		log.Ctx(ctx).Info().Str("provider", "cron").Msg("cron stopped cleanly")
		return nil
	case <-timer.C:
		entries := p.cron.Entries()
		ids := make([]int, 0, len(entries))
		for _, e := range entries {
			ids = append(ids, int(e.ID))
		}
		log.Ctx(ctx).Warn().
			Str("provider", "cron").
			Dur("timeout", timeout).
			Ints("entries", ids).
			Msg("cron shutdown timed out, jobs still running in background")
		return nil
	}
}

// GetCron 从容器获取 *cron.Cron。
func GetCron(ctx context.Context) (*gpcron.Cron, error) {
	return ioc.Get[*gpcron.Cron](ctx)
}

// MustGetCron 从容器获取 *cron.Cron,缺失时 panic(通常是 cron.enabled=false 时被业务方误用)。
func MustGetCron(ctx context.Context) *gpcron.Cron {
	return ioc.MustGet[*gpcron.Cron](ctx)
}
