// Package notify 聚合多通道运维通知发送(Telegram Bot、Lark Webhook):
//
//   - Notifier 接口接收 (ctx, title, content);Telegram 用 HTML 加粗 title,Lark 剥离 HTML 标签
//   - WithTelegram(token, chatID) / WithLark(webhook) 任选其一或同时配置
//   - New(opts...) 返回 noop / 单通知器 / multi 三种之一;multi 向所有通道并发发送,记录失败
//     但只返回最后一个错误
//   - 内部使用 resty,10s HTTP 超时,fire-and-forget,不做队列与重试
//
// 适合"运维侧告警提醒"等容忍丢失的场景;对可靠性敏感的业务通知应走 jobqueue + 重试。
package notify
