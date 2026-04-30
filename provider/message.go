package provider

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/message"
)

// MessageProvider 把 message.Manager 注入容器,
// 邮件 / 短信后端均按 yaml `providers[]` 顺序构造,顺序即 fallback 优先级。
//
// 编排约定:无外部依赖(不依赖 Redis / DB / gRPC),可放在基础设施 Provider 之后、
// 业务 Provider 之前的任意位置。
type MessageProvider struct {
	enabled bool
	mgr     *message.Manager
}

// emailProviderYaml 是 message.email.providers[] 的通用 entry 结构,
// 字段按 type 选择性使用,unmarshal 时无关字段保留零值。
type emailProviderYaml struct {
	Type     string        `mapstructure:"type"`
	Name     string        `mapstructure:"name"`
	From     string        `mapstructure:"from"`
	FromName string        `mapstructure:"from_name"`
	Timeout  time.Duration `mapstructure:"timeout"`
	// smtp
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Tls      string `mapstructure:"tls"`
	// sendgrid (与未来 awsses / aliyunmail 共用部分字段)
	ApiKey    string `mapstructure:"api_key"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Region    string `mapstructure:"region"`
	// 邮件域名后缀级别筛选,所有 email type 共用
	IncludeDomains []string `mapstructure:"include_domains"`
	ExcludeDomains []string `mapstructure:"exclude_domains"`
}

// smsProviderYaml 是 message.sms.providers[] 的通用 entry 结构。
type smsProviderYaml struct {
	Type    string        `mapstructure:"type"`
	Name    string        `mapstructure:"name"`
	Timeout time.Duration `mapstructure:"timeout"`
	// twilio
	AccountSid string `mapstructure:"account_sid"`
	AuthToken  string `mapstructure:"auth_token"`
	From       string `mapstructure:"from"`
	// byteplus / aliyun 共用
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Region    string `mapstructure:"region"`
	// byteplus
	SmsAccount string `mapstructure:"sms_account"`
	Sign       string `mapstructure:"sign"`
	// aliyun (intl + cn 共用)
	Endpoint string `mapstructure:"endpoint"`
	// aliyun-cn
	SignName string `mapstructure:"sign_name"`
	// 区号(国家码)前缀级别筛选,所有 sms type 共用
	IncludeRegions []string `mapstructure:"include_regions"`
	ExcludeRegions []string `mapstructure:"exclude_regions"`
}

// emailTemplateYaml 是 message.email.templates[] 的 entry 结构,
// subject/html/text 任一可走 inline 字符串或 *_file 文件路径。
type emailTemplateYaml struct {
	Id          string `mapstructure:"id"`
	Subject     string `mapstructure:"subject"`
	Html        string `mapstructure:"html"`
	Text        string `mapstructure:"text"`
	SubjectFile string `mapstructure:"subject_file"`
	HtmlFile    string `mapstructure:"html_file"`
	TextFile    string `mapstructure:"text_file"`
}

// Register 读取 message.email.enabled / message.sms.enabled,
// 任一开启即视为 provider 启用,Setup 阶段才真正构建 Manager。
func (p *MessageProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("message.email.enabled") || v.GetBool("message.sms.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "message").Msg("message disabled, skip")
	}
	return nil
}

// Setup 从 viper 读取 providers / templates,转成 message.Option 后构造 Manager 注入容器。
func (p *MessageProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	v := MustGetViper(ctx)
	var opts []message.Option

	if v.GetBool("message.email.enabled") {
		emailOpts, err := buildEmailOpts(v)
		if err != nil {
			return err
		}
		opts = append(opts, emailOpts...)
	}

	if v.GetBool("message.sms.enabled") {
		smsOpts, err := buildSmsOpts(v)
		if err != nil {
			return err
		}
		opts = append(opts, smsOpts...)
	}

	mgr, err := message.New(opts...)
	if err != nil {
		return fmt.Errorf("provider/message: new manager: %w", err)
	}
	p.mgr = mgr
	ioc.MustInstance(ctx, p.mgr)

	log.Ctx(ctx).Info().Str("provider", "message").Msg("message manager registered")
	return nil
}

// buildEmailOpts 把 message.email.providers + templates 转成 Options。
func buildEmailOpts(v *viper.Viper) ([]message.Option, error) {
	var entries []emailProviderYaml
	if err := v.UnmarshalKey("message.email.providers", &entries); err != nil {
		return nil, fmt.Errorf("provider/message: parse email providers: %w", err)
	}

	opts := make([]message.Option, 0, len(entries))
	for _, e := range entries {
		switch e.Type {
		case "smtp":
			opts = append(opts, message.WithSmtp(message.SmtpConfig{
				Name:           e.Name,
				Host:           e.Host,
				Port:           e.Port,
				Username:       e.Username,
				Password:       e.Password,
				From:           e.From,
				FromName:       e.FromName,
				Timeout:        e.Timeout,
				Tls:            e.Tls,
				IncludeDomains: e.IncludeDomains,
				ExcludeDomains: e.ExcludeDomains,
			}))
		case "sendgrid":
			opts = append(opts, message.WithSendGrid(message.SendGridConfig{
				Name:           e.Name,
				ApiKey:         e.ApiKey,
				From:           e.From,
				FromName:       e.FromName,
				Timeout:        e.Timeout,
				IncludeDomains: e.IncludeDomains,
				ExcludeDomains: e.ExcludeDomains,
			}))
		default:
			return nil, fmt.Errorf("provider/message: unknown email provider type %q (name=%q)", e.Type, e.Name)
		}
	}

	tplOpts, err := buildEmailTemplateOpts(v)
	if err != nil {
		return nil, err
	}
	opts = append(opts, tplOpts...)
	return opts, nil
}

// buildEmailTemplateOpts 处理 templates 列表,inline 与 *_file 二选一。
func buildEmailTemplateOpts(v *viper.Viper) ([]message.Option, error) {
	var tpls []emailTemplateYaml
	if err := v.UnmarshalKey("message.email.templates", &tpls); err != nil {
		return nil, fmt.Errorf("provider/message: parse email templates: %w", err)
	}

	dir := v.GetString("message.email.templates_dir")
	opts := make([]message.Option, 0, len(tpls))
	for _, t := range tpls {
		if t.Id == "" {
			continue
		}
		subject, err := resolveTplField(dir, t.Id, "subject", t.Subject, t.SubjectFile)
		if err != nil {
			return nil, err
		}
		html, err := resolveTplField(dir, t.Id, "html", t.Html, t.HtmlFile)
		if err != nil {
			return nil, err
		}
		text, err := resolveTplField(dir, t.Id, "text", t.Text, t.TextFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, message.WithEmailTemplate(t.Id, subject, html, text))
	}
	return opts, nil
}

// buildSmsOpts 把 message.sms.providers 转成 Options。
func buildSmsOpts(v *viper.Viper) ([]message.Option, error) {
	var entries []smsProviderYaml
	if err := v.UnmarshalKey("message.sms.providers", &entries); err != nil {
		return nil, fmt.Errorf("provider/message: parse sms providers: %w", err)
	}

	opts := make([]message.Option, 0, len(entries))
	for _, e := range entries {
		switch e.Type {
		case "twilio":
			opts = append(opts, message.WithTwilioSms(message.TwilioConfig{
				Name:           e.Name,
				AccountSid:     e.AccountSid,
				AuthToken:      e.AuthToken,
				From:           e.From,
				Timeout:        e.Timeout,
				IncludeRegions: e.IncludeRegions,
				ExcludeRegions: e.ExcludeRegions,
			}))
		case "byteplus":
			opts = append(opts, message.WithBytePlusSms(message.BytePlusConfig{
				Name:           e.Name,
				AccessKey:      e.AccessKey,
				SecretKey:      e.SecretKey,
				Region:         e.Region,
				SmsAccount:     e.SmsAccount,
				Sign:           e.Sign,
				Timeout:        e.Timeout,
				IncludeRegions: e.IncludeRegions,
				ExcludeRegions: e.ExcludeRegions,
			}))
		case "aliyun":
			opts = append(opts, message.WithAliyunSms(message.AliyunSmsConfig{
				Name:           e.Name,
				AccessKey:      e.AccessKey,
				SecretKey:      e.SecretKey,
				Endpoint:       e.Endpoint,
				Timeout:        e.Timeout,
				IncludeRegions: e.IncludeRegions,
				ExcludeRegions: e.ExcludeRegions,
			}))
		case "aliyun-cn":
			opts = append(opts, message.WithAliyunCnSms(message.AliyunCnSmsConfig{
				Name:           e.Name,
				AccessKey:      e.AccessKey,
				SecretKey:      e.SecretKey,
				Endpoint:       e.Endpoint,
				SignName:       e.SignName,
				Timeout:        e.Timeout,
				IncludeRegions: e.IncludeRegions,
				ExcludeRegions: e.ExcludeRegions,
			}))
		default:
			return nil, fmt.Errorf("provider/message: unknown sms provider type %q (name=%q)", e.Type, e.Name)
		}
	}
	return opts, nil
}

// resolveTplField 处理单个模板字段 inline / file 二选一。
//
//   - 同时设两者:报错(意图不明确,boot 即暴露)
//   - 仅 inline:原样返回
//   - 仅 file:相对 dir 解析后读文件;dir 为空相对工作目录
//   - 都为空:返回 "",对应模板部分不参与渲染
func resolveTplField(dir, id, field, inline, file string) (string, error) {
	if inline != "" && file != "" {
		return "", fmt.Errorf("provider/message: template %q.%s has both inline and file", id, field)
	}
	if file == "" {
		return inline, nil
	}
	path := file
	if dir != "" {
		path = filepath.Join(dir, file)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("provider/message: read template %q.%s file %s: %w", id, field, path, err)
	}
	return string(b), nil
}

// MustGetMessageManager 从容器获取 *message.Manager,缺失时 panic。
func MustGetMessageManager(ctx context.Context) *message.Manager {
	return ioc.MustGet[*message.Manager](ctx)
}

// GetMessageManager 从容器获取 *message.Manager。
func GetMessageManager(ctx context.Context) (*message.Manager, error) {
	return ioc.Get[*message.Manager](ctx)
}
