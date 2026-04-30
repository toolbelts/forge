package provider

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/notify"
)

const (
	notifyEventStarted = "started"
	notifyEventStopped = "stopped"
	notifyEventPanic   = "panic"

	notifySendTimeout      = 10 * time.Second
	notifyStackMaxBytes    = 1500
	notifyStackTruncateMsg = "\n... (stack truncated)"
	notifyTimeFormat       = "2006-01-02 15:04:05"
	notifyShortRevisionLen = 7
)

// NotifyProvider 通知提供者：构造 notify.Notifier 注入容器，并在
// 服务启动 / 关闭 / panic 时通过 notifier 发送格式化提醒。
//
// 同时实现 Server 与 Shutdowner：
//   - Serve  : 等所有 Provider Setup 完成后被调用，发 "started" 后阻塞到 ctx 取消
//   - Shutdown: LIFO 阶段最后被调用，发 "stopped"
type NotifyProvider struct {
	notifier  notify.Notifier
	buildInfo *BuildInfo
	appName   string
	hostname  string
}

// Register 读取 viper 配置创建 notifier 并注入容器。无任何渠道配置时使用 noop 实现。
func (p *NotifyProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)

	p.notifier = notify.New(
		notify.WithTelegram(v.GetString("notify.telegram.token"), v.GetString("notify.telegram.chat_id")),
		notify.WithLark(v.GetString("notify.lark.webhook")),
	)
	ioc.MustInstance(ctx, p.notifier)

	log.Ctx(ctx).Info().Msg("notifier initialized")
	return nil
}

// Setup 缓存生命周期通知所需依赖。
func (p *NotifyProvider) Setup(ctx context.Context) error {
	p.buildInfo = MustGetBuildInfo(ctx)
	p.appName = string(MustGetAppName(ctx))
	p.hostname, _ = os.Hostname()
	return nil
}

// Serve 在所有 Provider Setup 完成后由 ioc 调度调用：发 started 通知，阻塞到 ctx 取消。
// 不能提前返回，否则会触发 ioc serveOrWait 的 "任一 server 退出即收敛" 逻辑。
func (p *NotifyProvider) Serve(ctx context.Context) error {
	p.sendLifecycle(ctx, notifyEventStarted)
	<-ctx.Done()
	return nil
}

// Shutdown 在 LIFO 阶段最后被调用：发 stopped 通知。
// 此时业务 server (grpc/http/tcp/gateway) 已优雅关闭。
func (p *NotifyProvider) Shutdown(ctx context.Context) error {
	p.sendLifecycle(ctx, notifyEventStopped)
	return nil
}

// sendLifecycle 同步发送一个生命周期通知，附带 BuildInfo 字段。
func (p *NotifyProvider) sendLifecycle(ctx context.Context, event string) {
	title, content := buildLifecycleMessage(p.hostname, p.appName, event, p.buildInfo)

	sendCtx, cancel := context.WithTimeout(ctx, notifySendTimeout)
	defer cancel()

	if err := p.notifier.Send(sendCtx, title, content); err != nil {
		log.Ctx(ctx).Error().Err(err).Str("event", event).Msg("lifecycle notify failed")
	}
}

// MustGetNotifier 从容器获取 Notifier，缺失时 panic。
func MustGetNotifier(ctx context.Context) notify.Notifier {
	return ioc.MustGet[notify.Notifier](ctx)
}

// notifyField 表示通知正文中的一个 KV 字段。
type notifyField struct {
	Key   string
	Value string
}

// buildLifecycleMessage 拼装服务启动 / 关闭通知的 title 与 content。
func buildLifecycleMessage(hostname, appName, event string, bi *BuildInfo) (string, string) {
	title := fmt.Sprintf("[%s] %s %s", hostname, appName, event)

	fields := []notifyField{
		{Key: "Version", Value: valueOrUnknown(bi.Version)},
		{Key: "Go", Value: valueOrUnknown(bi.GoVersion)},
		{Key: "Module", Value: valueOrUnknown(bi.Module)},
		{Key: "Revision", Value: valueOrUnknown(shortRevision(bi.Revision))},
	}
	if bi.Time != nil {
		fields = append(fields, notifyField{Key: "Build Time", Value: bi.Time.Format(notifyTimeFormat)})
	}
	fields = append(fields, notifyField{Key: "Dirty", Value: fmt.Sprintf("%t", bi.Dirty)})

	return title, formatFields(fields)
}

// buildPanicMessage 拼装 panic 告警的 title 与 content。
func buildPanicMessage(hostname, appName, method string, panicValue any, stack []byte) (string, string) {
	title := fmt.Sprintf("[%s] %s %s", hostname, appName, notifyEventPanic)

	fields := []notifyField{
		{Key: "Method", Value: valueOrUnknown(method)},
		{Key: "Time", Value: time.Now().Format(notifyTimeFormat)},
		{Key: "Panic", Value: fmt.Sprint(panicValue)},
		{Key: "Stack", Value: "\n" + truncateStack(stack)},
	}

	return title, formatFields(fields)
}

// formatFields 把 KV 列表拼成 "Key: Value\n" 多行文本，末尾去尾换行。
func formatFields(fields []notifyField) string {
	var sb strings.Builder
	for _, f := range fields {
		sb.WriteString(f.Key)
		sb.WriteString(": ")
		sb.WriteString(f.Value)
		sb.WriteByte('\n')
	}
	return strings.TrimRight(sb.String(), "\n")
}

// shortRevision 取短 commit hash，长度不足时原样返回。
func shortRevision(rev string) string {
	if len(rev) > notifyShortRevisionLen {
		return rev[:notifyShortRevisionLen]
	}
	return rev
}

// valueOrUnknown 空字符串归一化为 "(unknown)"，避免 "Key: " 这种残缺行。
func valueOrUnknown(s string) string {
	if s == "" {
		return "(unknown)"
	}
	return s
}

// truncateStack 截断 stack 文本到 notifyStackMaxBytes，避免超过通知后端长度上限。
func truncateStack(stack []byte) string {
	if len(stack) <= notifyStackMaxBytes {
		return string(stack)
	}
	return string(stack[:notifyStackMaxBytes]) + notifyStackTruncateMsg
}
