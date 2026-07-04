package notify

import (
	"context"
	"fmt"
	"time"

	"resty.dev/v3"
)

// telegramNotifier 通过 Telegram Bot API 发送通知
type telegramNotifier struct {
	client *resty.Client
	token  string
	chatID string
}

func newTelegramNotifier(token, chatID string) *telegramNotifier {
	client := resty.New().
		SetBaseURL("https://api.telegram.org").
		SetTimeout(10 * time.Second)
	return &telegramNotifier{
		client: client,
		token:  token,
		chatID: chatID,
	}
}

// Send 将 title 和 content 拼装为 HTML 格式发送到 Telegram。
// 格式：<b>{title}</b>\n{content}
func (t *telegramNotifier) Send(ctx context.Context, title, content string) error {
	text := fmt.Sprintf("<b>%s</b>\n%s", title, content)

	resp, err := t.client.R().
		SetContext(ctx).
		SetBody(map[string]any{
			"chat_id":    t.chatID,
			"text":       text,
			"parse_mode": "HTML",
		}).
		Post(fmt.Sprintf("/bot%s/sendMessage", t.token))
	if err != nil {
		return fmt.Errorf("telegram send failed: %w", err)
	}
	if !resp.IsStatusSuccess() {
		return fmt.Errorf("telegram send failed: status=%d body=%s", resp.StatusCode(), resp.String())
	}
	return nil
}
