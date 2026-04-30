package message

import (
	"context"
	"fmt"

	"github.com/sendgrid/sendgrid-go"
	sgmail "github.com/sendgrid/sendgrid-go/helpers/mail"
)

// sendGridSender 是 emailSender 的 SendGrid HTTP API 实现,基于 sendgrid/sendgrid-go。
//
// 走 v3 mail/send 端点;同一收件人列表合并到一个 Personalization 内,
// Cc / Bcc 同 personalization,与 Twilio SendGrid 控制台行为一致。
type sendGridSender struct {
	domainFilter
	cfg  SendGridConfig
	host string // 测试覆盖用,空 → 走 sendgrid-go 默认 host (api.sendgrid.com)
}

// build 让 SendGridConfig 实现 emailSpec 接口。
func (c SendGridConfig) build() (emailSender, error) {
	return newSendGridSender(c)
}

func newSendGridSender(cfg SendGridConfig) (*sendGridSender, error) {
	if cfg.ApiKey == "" || cfg.From == "" {
		return nil, fmt.Errorf("message: sendgrid %q missing api_key/from", cfg.Name)
	}
	return &sendGridSender{
		domainFilter: domainFilter{Include: cfg.IncludeDomains, Exclude: cfg.ExcludeDomains},
		cfg:          cfg,
	}, nil
}

func (s *sendGridSender) Name() string {
	if s.cfg.Name == "" {
		return "sendgrid"
	}
	return "sendgrid:" + s.cfg.Name
}

func (s *sendGridSender) Send(ctx context.Context, msg EmailMessage) error {
	sendCtx, cancel := context.WithTimeout(ctx, pickTimeout(s.cfg.Timeout))
	defer cancel()

	mail := sgmail.NewV3Mail()
	mail.SetFrom(sgmail.NewEmail(s.cfg.FromName, s.cfg.From))
	mail.Subject = msg.Subject

	person := sgmail.NewPersonalization()
	person.AddTos(sgmail.NewEmail("", msg.To))
	for _, addr := range msg.Cc {
		person.AddCCs(sgmail.NewEmail("", addr))
	}
	for _, addr := range msg.Bcc {
		person.AddBCCs(sgmail.NewEmail("", addr))
	}
	for k, v := range msg.Headers {
		person.SetHeader(k, v)
	}
	mail.AddPersonalizations(person)

	// SendGrid 文档建议同时存在时 plain 在前,html 在后,客户端按能力优先取 html。
	if msg.Text != "" {
		mail.AddContent(sgmail.NewContent("text/plain", msg.Text))
	}
	if msg.Html != "" {
		mail.AddContent(sgmail.NewContent("text/html", msg.Html))
	}

	req := sendgrid.GetRequest(s.cfg.ApiKey, "/v3/mail/send", s.host)
	req.Method = "POST"
	req.Body = sgmail.GetRequestBody(mail)

	resp, err := sendgrid.MakeRequestWithContext(sendCtx, req)
	if err != nil {
		return fmt.Errorf("message: sendgrid send: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("message: sendgrid send status=%d body=%s", resp.StatusCode, resp.Body)
	}
	return nil
}
