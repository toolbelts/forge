package message

import (
	"context"
	"strings"
)

// SmsMessage 表示一条待发送的短信。
//
// 一条 SmsMessage 只允许一个目标号码;多号码需求由调用方循环。这样区号筛选、mode
// 兼容性判断、provider fallback 都只对单个号码生效一次,错误语义清晰。
//
// 同时承载两种发送 mode,实际使用哪一个由具体 sender 决定:
//
//   - raw mode    : Content 非空 → 直接把这段文本下发(Twilio / Aliyun 国际)。
//   - template mode: TemplateId 非空 → 用 provider 端模板 + Params 参数填充
//     (BytePlus / Aliyun 国内)。
//
// Params 仅在 template mode 起效;BytePlus 序列化为 JSON,Aliyun 国内为 name=value 列表。
type SmsMessage struct {
	To         string         // 必填,E.164 格式手机号,如 "+8613800138000"
	SignName   string         // 部分 provider 需要(如 Aliyun 国内,模板 mode);Twilio 忽略
	Content    string         // raw mode 内容
	TemplateId string         // template mode 模板 id
	Params     map[string]any // template mode 参数
}

// smsMode 描述一个 sms sender 支持的发送模式。
type smsMode uint8

const (
	smsModeRaw      smsMode = iota + 1 // 仅支持直发内容
	smsModeTemplate                    // 仅支持模板 ID
	smsModeBoth                        // 都支持(后续可由 sender 升级)
)

// smsSender 是单个短信后端的发送接口。
type smsSender interface {
	Send(ctx context.Context, msg SmsMessage) error
	Mode() smsMode
	Name() string // 用于日志/错误标识,如 "twilio:primary"
	// Accepts 判断 sender 是否愿意承接给定号码,主要按区号(国家码)前缀筛选。
	// 实现一般通过嵌入 regionFilter 自动获得;未配置 include/exclude 时一律返回 true。
	Accepts(to string) bool
}

// regionFilter 给 sms sender 提供区号(国家码)前缀级别的可发送筛选。
//
// 语义:
//   - Include 非空:号码必须命中 Include 中的某个前缀,才有可能被发送。
//   - Exclude 非空:号码命中任一前缀立即拒绝(优先级高于 Include 命中)。
//   - 两者皆空:任何号码都接受,等同于不筛选。
//
// 前缀直接和号码的「数字部分」做 strings.HasPrefix 比较。号码会先剥离
// 头部的 '+' 或 '00' 国际拨号前缀,再做匹配,因此 yaml 里写 "86" 即可匹配
// "+8613800138000"、"008613800138000"、"8613800138000" 这三种常见写法。
type regionFilter struct {
	Include []string
	Exclude []string
}

// Accepts 实现 smsSender.Accepts,通过结构体嵌入即可让具体 sender 自动获得。
func (r regionFilter) Accepts(to string) bool {
	digits := stripPhonePrefix(to)
	if len(r.Include) > 0 && !regionPrefixMatch(digits, r.Include) {
		return false
	}
	if len(r.Exclude) > 0 && regionPrefixMatch(digits, r.Exclude) {
		return false
	}
	return true
}

// stripPhonePrefix 去掉号码开头的 '+' 或 '00' 国际拨号前缀,返回纯数字部分。
// 不做更进一步的格式校验,异常输入会原样返回(随后 Accepts 走默认匹配语义)。
func stripPhonePrefix(s string) string {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, "+"):
		return s[1:]
	case strings.HasPrefix(s, "00"):
		return s[2:]
	default:
		return s
	}
}

// regionPrefixMatch 任一前缀 hit digits 即返回 true。空前缀("")会匹配任意号码,
// 调用方应自行避免在配置中写空字符串。
func regionPrefixMatch(digits string, prefixes []string) bool {
	for _, p := range prefixes {
		if p == "" {
			continue
		}
		if strings.HasPrefix(digits, p) {
			return true
		}
	}
	return false
}

// inferMode 根据消息字段判断需要哪种 mode。
// TemplateId 优先(template 比 raw 信息更明确),其次 Content。
// 两者都空返回 ok=false。
func (msg SmsMessage) inferMode() (smsMode, bool) {
	switch {
	case msg.TemplateId != "":
		return smsModeTemplate, true
	case msg.Content != "":
		return smsModeRaw, true
	default:
		return 0, false
	}
}

// supports 判断 sender 的 Mode 能否覆盖请求的 mode。
func (have smsMode) supports(need smsMode) bool {
	return have == need || have == smsModeBoth
}
