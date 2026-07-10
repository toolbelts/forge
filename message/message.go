package message

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

// Manager 是邮件 + 短信发送门面,通过 New() 构造,内部 senders 都是有序列表。
type Manager struct {
	email     []emailSender             // 顺序即 fallback 优先级
	sms       []smsSender               // 顺序即 fallback 优先级,另叠加 mode 过滤
	templates map[string]*emailTemplate // 邮件模板,id → 编译好的 template
}

// New 根据 opts 构造 Manager。
//
// 行为:apply opts → 编译模板(任一失败立即返回)→ 构建所有 sender(任一失败立即返回)
// → 校验至少存在一个邮件或短信后端,否则 ErrNoBackendConfigured。
func New(opts ...Option) (*Manager, error) {
	c := &config{}
	for _, opt := range opts {
		opt(c)
	}
	if len(c.errs) > 0 {
		return nil, errors.Join(c.errs...)
	}

	m := &Manager{
		templates: make(map[string]*emailTemplate, len(c.templates)),
	}

	for _, t := range c.templates {
		tpl, err := compileEmailTemplate(t.id, t.subject, t.html, t.text)
		if err != nil {
			return nil, err
		}
		m.templates[t.id] = tpl
	}

	for _, spec := range c.emailSpecs {
		s, err := spec.build()
		if err != nil {
			return nil, err
		}
		m.email = append(m.email, s)
	}
	for _, spec := range c.smsSpecs {
		s, err := spec.build()
		if err != nil {
			return nil, err
		}
		m.sms = append(m.sms, s)
	}

	if len(m.email) == 0 && len(m.sms) == 0 {
		return nil, ErrNoBackendConfigured
	}
	return m, nil
}

// SendEmail 走「单收件人 + provider fallback」:对 msg.To 按 m.email 优先级
// 遍历 sender,同时满足 sender.Accepts(domain Include/Exclude) 才会被选中,首个发送成功胜出。
//
// 一封 EmailMessage 只允许一个 To,因此路由决策只发生一次,Cc/Bcc 一次性跟随选中的
// sender 下发,不会出现「同一封邮件被多个 provider 各发一份」。多收件人请由调用方循环。
//
// 错误形态:
//   - 没有任何 sender 接受该 To(域名 Include/Exclude 全部不命中)→ ErrNoCompatibleEmailSender。
//   - 至少一个 sender 接受该 To 但全部 Send 失败 → 包装最后一次失败的错误,错误文本带上 To。
//
// 若 msg.TemplateId 非空,则先在内部渲染已注册模板,以渲染结果覆盖 Subject/Html/Text,
// 再走正常发送流程;模板不存在返回 ErrTemplateNotFound。
func (m *Manager) SendEmail(ctx context.Context, msg EmailMessage) error {
	if len(m.email) == 0 {
		return ErrNoEmailSender
	}

	if msg.TemplateId != "" {
		tpl, ok := m.templates[msg.TemplateId]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTemplateNotFound, msg.TemplateId)
		}
		subject, html, text, err := tpl.render(msg.Params)
		if err != nil {
			return err
		}
		msg.Subject, msg.Html, msg.Text = subject, html, text
	}

	if err := msg.validate(); err != nil {
		return err
	}

	var (
		lastErr  error
		eligible bool
	)
	for _, s := range m.email {
		if !s.Accepts(msg.To) {
			continue
		}
		eligible = true
		if err := s.Send(ctx, msg); err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("email", s.Name()).Str("to", msg.To).Msg("email send failed, fallback")
			lastErr = err
			continue
		}
		return nil
	}
	if !eligible {
		return ErrNoCompatibleEmailSender
	}
	return fmt.Errorf("message: email send failed for %s: %w", msg.To, lastErr)
}

// SendSms 按消息字段判定所需 mode,然后走「单号码 + provider fallback」。
//
// 路由规则:
//   - 按 m.sms 优先级遍历 sender:同时满足 mode 兼容(smsMode.supports)与区号筛选
//     (sender.Accepts)才会被选中,首个发送成功胜出。
//   - 若所有兼容 sender 都失败 → 包装最后一次失败的错误,错误文本带上 To。
//
// 模式判定:TemplateId 优先(template mode),否则 Content(raw mode);两者皆空 →
// ErrInvalidSmsMessage。多号码需求由调用方循环 SendSms。
func (m *Manager) SendSms(ctx context.Context, msg SmsMessage) error {
	if len(m.sms) == 0 {
		return ErrNoSmsSender
	}
	if msg.To == "" {
		return ErrInvalidSmsMessage
	}
	needMode, ok := msg.inferMode()
	if !ok {
		return ErrInvalidSmsMessage
	}

	var (
		lastErr  error
		eligible bool
	)
	for _, s := range m.sms {
		if !s.Mode().supports(needMode) {
			continue
		}
		if !s.Accepts(msg.To) {
			continue
		}
		eligible = true
		if err := s.Send(ctx, msg); err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("sms", s.Name()).Str("to", msg.To).Msg("sms send failed, fallback")
			lastErr = err
			continue
		}
		return nil
	}
	if !eligible {
		return ErrNoCompatibleSmsSender
	}
	return fmt.Errorf("message: sms send failed for %s: %w", msg.To, lastErr)
}

// IsTransientErr 是辅助函数,业务方可借此判断 SendEmail / SendSms 返回的
// 包装错误是否来自「全部 fallback 都失败」(从而决定是否换信道 / 重试 / 落库)。
//
// 当前实现:errors.Is 命中任一已知 sentinel 时返回 false(代表配置/参数问题,
// 重试无意义);其它情况(如 SMTP 网络错、HTTP 5xx)视为 transient → true。
func IsTransientErr(err error) bool {
	if err == nil {
		return false
	}
	for _, sentinel := range []error{
		ErrNoBackendConfigured, ErrNoEmailSender, ErrNoSmsSender,
		ErrNoCompatibleSmsSender, ErrNoCompatibleEmailSender, ErrTemplateNotFound,
		ErrInvalidEmailMessage, ErrInvalidSmsMessage,
	} {
		if errors.Is(err, sentinel) {
			return false
		}
	}
	return true
}
