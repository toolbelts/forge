package notify

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/go-resty/resty/v2"
)

// htmlTagRe 匹配 HTML 标签，用于向飞书发送前剥离格式标记
var htmlTagRe = regexp.MustCompile(`<[^>]+>`)

// larkNotifier 通过飞书 Webhook 发送通知
type larkNotifier struct {
	client  *resty.Client
	webhook string
}

func newLarkNotifier(webhook string) *larkNotifier {
	client := resty.New().SetTimeout(10 * time.Second)
	return &larkNotifier{
		client:  client,
		webhook: webhook,
	}
}

// Send 将 title 和 content 拼装为纯文本后发送到飞书 Webhook。
// 飞书不支持 HTML，先剥离 HTML 标签再发送。
func (l *larkNotifier) Send(ctx context.Context, title, content string) error {
	text := fmt.Sprintf("%s\n%s", title, content)
	text = htmlTagRe.ReplaceAllString(text, "")

	resp, err := l.client.R().
		SetContext(ctx).
		SetBody(map[string]any{
			"msg_type": "text",
			"content":  map[string]string{"text": text},
		}).
		Post(l.webhook)
	if err != nil {
		return fmt.Errorf("lark send failed: %w", err)
	}
	if !resp.IsSuccess() {
		return fmt.Errorf("lark send failed: status=%d body=%s", resp.StatusCode(), resp.String())
	}
	return nil
}
