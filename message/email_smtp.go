package message

import (
	"context"
	"fmt"

	"github.com/wneessen/go-mail"
)

// smtpSender 是 emailSender 的 SMTP 实现,基于 wneessen/go-mail。
//
// 一个 smtpSender 绑定一组 SMTP 凭证 + 默认 From 地址,
// 每次 Send 都新建一个 *mail.Client,内部 DialAndSendWithContext 完整 connect→send→quit。
// 此模式简单、并发安全、易于按 ctx 取消;牺牲连接复用,但 SMTP 中继本身吞吐有限,可接受。
type smtpSender struct {
	domainFilter
	cfg SmtpConfig
}

// build 让 SmtpConfig 实现 emailSpec 接口。
func (c SmtpConfig) build() (emailSender, error) {
	return newSmtpSender(c)
}

func newSmtpSender(cfg SmtpConfig) (*smtpSender, error) {
	if cfg.Host == "" || cfg.From == "" || cfg.Port == 0 {
		return nil, fmt.Errorf("message: smtp %q missing host/port/from", cfg.Name)
	}
	return &smtpSender{
		domainFilter: domainFilter{Include: cfg.IncludeDomains, Exclude: cfg.ExcludeDomains},
		cfg:          cfg,
	}, nil
}

func (s *smtpSender) Name() string {
	if s.cfg.Name == "" {
		return "smtp"
	}
	return "smtp:" + s.cfg.Name
}

func (s *smtpSender) Send(ctx context.Context, msg EmailMessage) error {
	m := mail.NewMsg()

	// From - 优先 FromName + From 组合,无 FromName 退化为单地址
	if s.cfg.FromName != "" {
		if err := m.FromFormat(s.cfg.FromName, s.cfg.From); err != nil {
			return fmt.Errorf("message: smtp set from: %w", err)
		}
	} else {
		if err := m.From(s.cfg.From); err != nil {
			return fmt.Errorf("message: smtp set from: %w", err)
		}
	}

	if err := m.To(msg.To); err != nil {
		return fmt.Errorf("message: smtp set to: %w", err)
	}
	if len(msg.Cc) > 0 {
		if err := m.Cc(msg.Cc...); err != nil {
			return fmt.Errorf("message: smtp set cc: %w", err)
		}
	}
	if len(msg.Bcc) > 0 {
		if err := m.Bcc(msg.Bcc...); err != nil {
			return fmt.Errorf("message: smtp set bcc: %w", err)
		}
	}

	m.Subject(msg.Subject)
	for k, v := range msg.Headers {
		m.SetGenHeader(mail.Header(k), v)
	}

	// Body 装载顺序:text 作为 main(向下兼容),html 作为 alternative。
	// 仅 html 时 main 设为 html;仅 text 时 main 设为 text;两者皆有则 multipart/alternative。
	switch {
	case msg.Text != "" && msg.Html != "":
		m.SetBodyString(mail.TypeTextPlain, msg.Text)
		m.AddAlternativeString(mail.TypeTextHTML, msg.Html)
	case msg.Html != "":
		m.SetBodyString(mail.TypeTextHTML, msg.Html)
	default:
		m.SetBodyString(mail.TypeTextPlain, msg.Text)
	}

	clientOpts, err := s.clientOptions()
	if err != nil {
		return err
	}

	client, err := mail.NewClient(s.cfg.Host, clientOpts...)
	if err != nil {
		return fmt.Errorf("message: smtp client: %w", err)
	}
	if err := client.DialAndSendWithContext(ctx, m); err != nil {
		return fmt.Errorf("message: smtp send: %w", err)
	}
	return nil
}

// clientOptions 把 SmtpConfig 翻译为 go-mail 选项。
func (s *smtpSender) clientOptions() ([]mail.Option, error) {
	opts := []mail.Option{
		mail.WithPort(s.cfg.Port),
		mail.WithTimeout(pickTimeout(s.cfg.Timeout)),
	}

	if s.cfg.Username != "" {
		opts = append(opts,
			mail.WithSMTPAuth(mail.SMTPAuthPlain),
			mail.WithUsername(s.cfg.Username),
			mail.WithPassword(s.cfg.Password),
		)
	}

	tlsMode := s.cfg.Tls
	if tlsMode == "" {
		tlsMode = defaultSmtpTls
	}
	switch tlsMode {
	case "starttls":
		opts = append(opts, mail.WithTLSPolicy(mail.TLSMandatory))
	case "tls":
		opts = append(opts, mail.WithSSL())
	case "none":
		opts = append(opts, mail.WithTLSPolicy(mail.NoTLS))
	default:
		return nil, fmt.Errorf("message: smtp %q unknown tls mode %q (want starttls|tls|none)", s.cfg.Name, tlsMode)
	}

	return opts, nil
}
