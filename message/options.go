package message

import "time"

// SmtpConfig 是一个 SMTP 后端的配置。
//
// 任意 SMTP 中继(自建 / AWS SES / Aliyun DirectMail / SendGrid SMTP 等)共用此结构,
// 只是 host/port/凭证不同;TLS mode 默认 starttls,默认 timeout 10s。
//
// IncludeDomains / ExcludeDomains 是收件人邮件域名后缀级别的可发送筛选,所有 email config
// 共用同一套语义:
//
//   - IncludeDomains 非空:收件人域名必须命中其中某个后缀才会被该 sender 承接。
//   - ExcludeDomains 非空:收件人域名命中任一后缀立即被该 sender 拒绝。
//   - 两者皆空:任何收件人都接受。
//
// 后缀对地址 `@` 之后的域名做 strings.HasSuffix(大小写不敏感),与 sms 的 IncludeRegions
// 平行,差异是「后缀」而非「前缀」。yaml 里写 ".cn" 即可命中 "a@example.cn" /
// "b@my.com.cn",写 "gmail.com" 命中 "@gmail.com" 子集。
type SmtpConfig struct {
	Name           string        // 标识,用于日志,如 "aws-ses" / "aliyun-dm"
	Host           string        // SMTP 服务器地址
	Port           int           // SMTP 端口
	Username       string        // 登录用户名
	Password       string        // 登录密码
	From           string        // 发信地址
	FromName       string        // 显示名,可空
	Timeout        time.Duration // 单次发送超时,<=0 取默认 10s
	Tls            string        // "starttls" | "tls" | "none",空取默认 "starttls"
	IncludeDomains []string
	ExcludeDomains []string
}

// SendGridConfig 是 SendGrid HTTP API 后端的配置。
// IncludeDomains / ExcludeDomains 语义见 SmtpConfig 注释。
type SendGridConfig struct {
	Name           string
	ApiKey         string        // SendGrid API key
	From           string        // 发信地址
	FromName       string        // 可空
	Timeout        time.Duration // 单次发送超时,<=0 取默认 10s
	IncludeDomains []string
	ExcludeDomains []string
}

// TwilioConfig 是 Twilio Messages API 后端的配置(raw mode)。
//
// IncludeRegions / ExcludeRegions 是号码区号(国家码)前缀级别的可发送筛选,
// 所有 sms config 共用同一套语义:
//
//   - IncludeRegions 非空:号码必须命中其中某个前缀才会被该 sender 承接。
//   - ExcludeRegions 非空:号码命中任一前缀立即被该 sender 拒绝。
//   - 两者皆空:任何号码都接受。
//
// 前缀直接对号码的「数字部分」做 strings.HasPrefix(号码会先剥离头部 '+' / '00'),
// 因此 yaml 里写 "86" 即可同时匹配 "+8613800138000" / "008613800138000" / "8613800138000"。
type TwilioConfig struct {
	Name           string
	AccountSid     string // ACxxxx
	AuthToken      string // 与 AccountSid 配套
	From           string // 国际格式手机号或 alphanumeric sender id
	Timeout        time.Duration
	IncludeRegions []string
	ExcludeRegions []string
}

// BytePlusConfig 是 BytePlus(Volcengine 海外品牌)SMS OpenAPI 后端的配置(template mode)。
// IncludeRegions / ExcludeRegions 语义见 TwilioConfig 注释。
type BytePlusConfig struct {
	Name           string
	AccessKey      string // VOLC_ACCESSKEY
	SecretKey      string // VOLC_SECRETKEY
	Region         string // 如 "ap-singapore-1"
	SmsAccount     string // 控制台为子账号开通的 sms_account
	Sign           string // 已审核签名
	Timeout        time.Duration
	IncludeRegions []string
	ExcludeRegions []string
}

// AliyunSmsConfig 是 Aliyun 国际版 SMS(SendMessageToGlobe)后端的配置(raw mode)。
// IncludeRegions / ExcludeRegions 语义见 TwilioConfig 注释。
type AliyunSmsConfig struct {
	Name           string
	AccessKey      string
	SecretKey      string
	Endpoint       string // 如 "https://sms-intl.ap-southeast-1.aliyuncs.com"
	Timeout        time.Duration
	IncludeRegions []string
	ExcludeRegions []string
}

// AliyunCnSmsConfig 是 Aliyun 国内版 SMS(Dysmsapi 2017-05-25 SendSms)后端的配置(template mode)。
//
// 国内短信强制走「签名 + 模板」:SignName 即 "【公司/品牌】" 前缀,需要预先在控制台审核;
// TemplateCode 由模板审核后生成(如 "SMS_001"),业务方在 SmsMessage.TemplateId 传入。
//
// SignName 既可在 config 里设默认值(常见单品牌场景),也可在 SmsMessage.SignName 上 per-message 覆盖。
// AliyunCnSmsConfig 是 Aliyun 国内版 SMS(Dysmsapi 2017-05-25 SendSms)后端的配置(template mode)。
//
// 国内短信强制走「签名 + 模板」:SignName 即 "【公司/品牌】" 前缀,需要预先在控制台审核;
// TemplateCode 由模板审核后生成(如 "SMS_001"),业务方在 SmsMessage.TemplateId 传入。
//
// SignName 既可在 config 里设默认值(常见单品牌场景),也可在 SmsMessage.SignName 上 per-message 覆盖。
//
// IncludeRegions / ExcludeRegions 语义见 TwilioConfig 注释,
// 唯一差异:留空时 IncludeRegions 默认为 ["86"],国内通道只承接 +86 号码。
type AliyunCnSmsConfig struct {
	Name           string
	AccessKey      string // Aliyun AccessKey ID
	SecretKey      string // Aliyun AccessKey Secret
	Endpoint       string // 默认 "https://dysmsapi.aliyuncs.com",通常无需改
	SignName       string // 默认签名,SmsMessage.SignName 优先;两者皆空 → 发送报错
	Timeout        time.Duration
	IncludeRegions []string
	ExcludeRegions []string
}

// emailSpec 是一个待构建的 email 后端规格,build 在 New() 内被调用。
// 配置类型(SmtpConfig / SendGridConfig)分别实现该接口。
type emailSpec interface {
	build() (emailSender, error)
}

// smsSpec 是一个待构建的 sms 后端规格。
type smsSpec interface {
	build() (smsSender, error)
}

// templateSpec 保存待编译的邮件模板原始字符串,New() 时统一 compile。
type templateSpec struct {
	id      string
	subject string
	html    string
	text    string
}

// config 是 New(opts...) 内部累积的配置结果。
//
// emailSpecs / smsSpecs 都按 Option 注册顺序追加,顺序即 fallback 优先级。
// 同 type 可重复(两个 SMTP 中继 / 两个 SendGrid 子账号都合法)。
type config struct {
	emailSpecs []emailSpec
	smsSpecs   []smsSpec
	templates  []templateSpec
}

// Option 是 New(opts...) 的可选配置。
type Option func(*config)

// WithSmtp 注册一个 SMTP 后端;Host / From / Port 任一为空则静默跳过。
func WithSmtp(cfg SmtpConfig) Option {
	return func(c *config) {
		if cfg.Host == "" || cfg.From == "" || cfg.Port == 0 {
			return
		}
		c.emailSpecs = append(c.emailSpecs, cfg)
	}
}

// WithSendGrid 注册一个 SendGrid HTTP API 后端;ApiKey / From 任一为空则静默跳过。
func WithSendGrid(cfg SendGridConfig) Option {
	return func(c *config) {
		if cfg.ApiKey == "" || cfg.From == "" {
			return
		}
		c.emailSpecs = append(c.emailSpecs, cfg)
	}
}

// WithEmailTemplate 注册一个邮件模板,启动期 parse,运行期按 id 查找渲染。
// 三个 body 字段都可空(对应部分跳过渲染),id 为空则忽略整个模板。
func WithEmailTemplate(id, subject, htmlBody, textBody string) Option {
	return func(c *config) {
		if id == "" {
			return
		}
		c.templates = append(c.templates, templateSpec{
			id:      id,
			subject: subject,
			html:    htmlBody,
			text:    textBody,
		})
	}
}

// WithTwilioSms 注册一个 Twilio Messages 后端(raw mode)。
// AccountSid / AuthToken / From 任一为空则静默跳过。
func WithTwilioSms(cfg TwilioConfig) Option {
	return func(c *config) {
		if cfg.AccountSid == "" || cfg.AuthToken == "" || cfg.From == "" {
			return
		}
		c.smsSpecs = append(c.smsSpecs, cfg)
	}
}

// WithBytePlusSms 注册一个 BytePlus SMS 后端(template mode)。
// AccessKey / SecretKey / Region / SmsAccount / Sign 任一为空则静默跳过。
func WithBytePlusSms(cfg BytePlusConfig) Option {
	return func(c *config) {
		if cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" ||
			cfg.SmsAccount == "" || cfg.Sign == "" {
			return
		}
		c.smsSpecs = append(c.smsSpecs, cfg)
	}
}

// WithAliyunSms 注册一个 Aliyun 国际版 SMS 后端(SendMessageToGlobe,raw mode)。
// AccessKey / SecretKey / Endpoint 任一为空则静默跳过。
func WithAliyunSms(cfg AliyunSmsConfig) Option {
	return func(c *config) {
		if cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Endpoint == "" {
			return
		}
		c.smsSpecs = append(c.smsSpecs, cfg)
	}
}

// WithAliyunCnSms 注册一个 Aliyun 国内版 SMS 后端(Dysmsapi SendSms,template mode)。
// AccessKey / SecretKey 任一为空则静默跳过;Endpoint 留空走默认 dysmsapi.aliyuncs.com。
// SignName 留空表示要求 SmsMessage.SignName 必须 per-message 提供。
func WithAliyunCnSms(cfg AliyunCnSmsConfig) Option {
	return func(c *config) {
		if cfg.AccessKey == "" || cfg.SecretKey == "" {
			return
		}
		c.smsSpecs = append(c.smsSpecs, cfg)
	}
}

// 默认值常量,sender 实现可读取并应用。
const (
	defaultSendTimeout = 10 * time.Second
	defaultSmtpTls     = "starttls"
)

// pickTimeout 在 cfg 提供的 Timeout 非正时回落到默认值。
func pickTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultSendTimeout
	}
	return d
}
