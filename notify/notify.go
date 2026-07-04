package notify

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
)

// Notifier 通知发送接口，支持标题与正文分离。
// 各后端不原生支持标题/正文分离时，自行在 Send 内部拼装。
type Notifier interface {
	Send(ctx context.Context, title, content string) error
}

type config struct {
	notifiers []Notifier
}

// Option 配置项，用于 New() 添加通知后端
type Option func(*config)

// WithTelegram 添加 Telegram Bot 通知后端
func WithTelegram(token, chatID string) Option {
	return func(c *config) {
		if token == "" || chatID == "" {
			return
		}
		c.notifiers = append(c.notifiers, newTelegramNotifier(token, chatID))
	}
}

// WithLark 添加飞书 Webhook 通知后端
func WithLark(webhook string) Option {
	return func(c *config) {
		if webhook == "" {
			return
		}
		c.notifiers = append(c.notifiers, newLarkNotifier(webhook))
	}
}

// New 根据 Option 构造 Notifier。
// 无有效 Option → noopNotifier；1 个 → 单一实现；多个 → multiNotifier。
func New(opts ...Option) Notifier {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	switch len(cfg.notifiers) {
	case 0:
		return &noopNotifier{}
	case 1:
		return cfg.notifiers[0]
	default:
		return &multiNotifier{notifiers: cfg.notifiers}
	}
}

// noopNotifier 空实现，无任何通知发送
type noopNotifier struct{}

func (n *noopNotifier) Send(_ context.Context, _, _ string) error { return nil }

// multiNotifier 聚合多个 Notifier，逐一发送，单个失败记录日志后继续
type multiNotifier struct {
	notifiers []Notifier
}

func (m *multiNotifier) Send(ctx context.Context, title, content string) error {
	var errs []error
	for _, n := range m.notifiers {
		if err := n.Send(ctx, title, content); err != nil {
			log.Error().Err(err).Msg("notifier send failed")
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
