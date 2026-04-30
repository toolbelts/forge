package message

import "errors"

var (
	// ErrNoBackendConfigured 表示 New() 时既未配置 email 也未配置 sms 后端。
	ErrNoBackendConfigured = errors.New("message: no backend configured")
	// ErrNoEmailSender 表示 SendEmail 时未配置任何邮件后端。
	ErrNoEmailSender = errors.New("message: email sender not configured")
	// ErrNoSmsSender 表示 SendSms 时未配置任何短信后端。
	ErrNoSmsSender = errors.New("message: sms sender not configured")
	// ErrNoCompatibleSmsSender 表示已配置的 sms sender 都不支持当前消息所需的 mode。
	ErrNoCompatibleSmsSender = errors.New("message: no sms sender supports requested mode")
	// ErrNoCompatibleEmailSender 表示已配置的 email sender 都不接受当前收件人(域名 Include/Exclude 全部不命中)。
	ErrNoCompatibleEmailSender = errors.New("message: no email sender accepts recipient")
	// ErrTemplateNotFound 表示 SendEmail 在 template mode 时 templateId 在注册表中不存在。
	ErrTemplateNotFound = errors.New("message: email template not found")
	// ErrInvalidEmailMessage 表示 EmailMessage 缺必填字段或字段冲突。
	ErrInvalidEmailMessage = errors.New("message: invalid email message")
	// ErrInvalidSmsMessage 表示 SmsMessage 既未提供 Content 也未提供 TemplateId。
	ErrInvalidSmsMessage = errors.New("message: invalid sms message")
)
