package message

import (
	"context"
	"strings"
)

// EmailMessage 表示一封待发送的邮件。
//
// 一封 EmailMessage 只允许一个主收件人 (To);多人需求由调用方循环。
// 这样 provider 路由(域名后缀 Include/Exclude)只需对 To 一次决策,Cc/Bcc 跟随
// 选定的 sender 一次性下发,无需在多个 provider 之间复制邮件。
//
// 支持两种 mode:
//   - raw mode    : TemplateId 为空,Subject/Html/Text 直接填充。Html 与 Text 至少应有一个非空。
//     同时设置则发 multipart/alternative,客户端按能力优先展示 Html。
//   - template mode: TemplateId 非空,SendEmail 在内部渲染模板后再下发;
//     Subject/Html/Text 由模板产生,Params 提供渲染数据。
type EmailMessage struct {
	To         string            // 必填,单收件人地址
	Cc         []string          // 可选抄送
	Bcc        []string          // 可选密送
	Subject    string            // raw mode 必填;template mode 由模板提供
	Html       string            // raw mode HTML body;template mode 由模板提供
	Text       string            // raw mode 纯文本 body;template mode 由模板提供
	Headers    map[string]string // 额外自定义 header,可空
	TemplateId string            // template mode：已注册模板 ID
	Params     map[string]any    // template mode：模板渲染参数
}

// emailSender 是单个邮件后端的发送接口。
// 实现:smtpSender(任意 SMTP 中继)、sendGridSender(SendGrid HTTP API)。
type emailSender interface {
	Send(ctx context.Context, msg EmailMessage) error
	Name() string // 用于日志,如 "smtp:aws-ses" / "sendgrid:default"
	// Accepts 判断 sender 是否愿意承接给定收件人,主要按域名后缀筛选。
	// 实现一般通过嵌入 domainFilter 自动获得;未配置 include/exclude 时一律返回 true。
	Accepts(addr string) bool
}

// domainFilter 给 email sender 提供邮件域名后缀级别的可发送筛选。
//
// 语义(与 sms.regionFilter 完全平行,只是匹配函数从 HasPrefix 换成 HasSuffix):
//   - Include 非空:地址的域名必须命中 Include 中某个后缀,才有可能被发送。
//   - Exclude 非空:域名命中任一后缀立即拒绝(优先级高于 Include 命中)。
//   - 两者皆空:任何地址都接受,等同于不筛选。
//
// 后缀对地址 `@` 之后的域名做 strings.HasSuffix(大小写不敏感),因此 yaml 里写 ".cn" 可
// 命中 "a@example.cn" / "b@my.com.cn",写 "gmail.com" 只命中 @gmail.com 子集。无 `@`
// 的异常输入会原样作为域名参与比较(走默认匹配语义)。
type domainFilter struct {
	Include []string
	Exclude []string
}

// Accepts 实现 emailSender.Accepts,通过结构体嵌入即可让具体 sender 自动获得。
func (d domainFilter) Accepts(addr string) bool {
	domain := extractEmailDomain(addr)
	if len(d.Include) > 0 && !domainSuffixMatch(domain, d.Include) {
		return false
	}
	if len(d.Exclude) > 0 && domainSuffixMatch(domain, d.Exclude) {
		return false
	}
	return true
}

// extractEmailDomain 取地址 `@` 之后的部分并 lower-case。无 `@` 则原样返回(异常输入,
// 后续 HasSuffix 比较走默认行为)。trim 一次空白避免 yaml 拷贝带尾随空格。
func extractEmailDomain(addr string) string {
	addr = strings.ToLower(strings.TrimSpace(addr))
	if i := strings.LastIndex(addr, "@"); i >= 0 {
		return addr[i+1:]
	}
	return addr
}

// domainSuffixMatch 任一后缀 hit domain 即返回 true。空后缀("")会匹配任意域名,
// 调用方应自行避免在配置中写空字符串(防御性跳过)。
func domainSuffixMatch(domain string, suffixes []string) bool {
	for _, s := range suffixes {
		if s == "" {
			continue
		}
		if strings.HasSuffix(domain, strings.ToLower(s)) {
			return true
		}
	}
	return false
}

// validate 在 dispatch 之前做最小字段校验,避免每个后端各自重复一次。
func (msg EmailMessage) validate() error {
	if msg.To == "" {
		return ErrInvalidEmailMessage
	}
	if msg.Subject == "" {
		return ErrInvalidEmailMessage
	}
	if msg.Html == "" && msg.Text == "" {
		return ErrInvalidEmailMessage
	}
	return nil
}
