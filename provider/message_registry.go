package provider

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/message"
)

// 本文件是自定义邮件 / 短信后端的注册表,形状照 database/sql 的 driver 注册:
// 业务仓库自己实现 message.SmsSender / message.EmailSender,用一个 type 名挂进
// yaml 的 providers[],forge 不需要认识任何具体供应商。
//
// # ★ 注册时机
//
// 必须在 MessageProvider.Setup 之前,所以请在包级 init() 或 main 里 ioc.Run 之前注册。
// 不要放进业务 Provider 的 Setup:MessageProvider 可能被别处的 MustGetMessageManager 提前拉起。
// 晚了的表现是 Setup 报 unknown provider type —— 错误文本会把已注册的类型列出来,
// 「没注册」与「注册晚了」一眼能分。
//
// ⚠️ message.sms.enabled / message.email.enabled 为 false 时,对应的 providers[]
// 根本不会被解析,自定义后端同样不会被构造。

// SmsProviderConfig 是 message.sms.providers[] 里的一条 entry。
//
// 通用字段强类型,其余全在 Raw 里由自定义 sender 自己解(forge 不认识它们)。
type SmsProviderConfig struct {
	Type           string
	Name           string
	Timeout        time.Duration
	IncludeRegions []string
	ExcludeRegions []string
	// Raw 是这条 entry 的**全部** yaml 键(含上面这几个)。viper 会把键统一小写。
	Raw map[string]any
}

// EmailProviderConfig 是 message.email.providers[] 里的一条 entry,语义同 SmsProviderConfig。
type EmailProviderConfig struct {
	Type           string
	Name           string
	From           string
	FromName       string
	Timeout        time.Duration
	IncludeDomains []string
	ExcludeDomains []string
	Raw            map[string]any
}

// SmsSenderFactory 按一条 yaml entry 造一个短信后端。
type SmsSenderFactory func(SmsProviderConfig) (message.SmsSender, error)

// EmailSenderFactory 按一条 yaml entry 造一个邮件后端。
type EmailSenderFactory func(EmailProviderConfig) (message.EmailSender, error)

// builtinSmsTypes / builtinEmailTypes 是 buildSmsOpts / buildEmailOpts 里 switch 认的类型。
//
// ★ 单独列一份是为了让 RegisterXxx 能拒绝覆盖内置类型;两份必须相等,
// 由 TestBuiltinTypeSetsMatchSwitch 钉住 —— 加了内置类型忘了加进这里,
// 后果是它能被业务悄悄换掉,而那种替换不报错。
var (
	builtinSmsTypes   = []string{"aliyun", "aliyun-cn", "byteplus", "twilio"}
	builtinEmailTypes = []string{"sendgrid", "smtp"}
)

var (
	senderMu       sync.RWMutex
	smsFactories   = map[string]SmsSenderFactory{}
	emailFactories = map[string]EmailSenderFactory{}
)

// RegisterSmsSender 注册一个自定义短信后端类型,之后 yaml 里 `type: <typ>` 即可使用。
//
// ⚠️ 重复注册、覆盖内置类型、typ 为空、f 为 nil,一律 panic:
// 注册发生在进程启动的单线程阶段,这类错误应该让进程起不来,
// 而不是跑到发码那一刻才炸。
func RegisterSmsSender(typ string, f SmsSenderFactory) {
	if typ == "" || f == nil {
		panic("provider/message: RegisterSmsSender needs a non-empty type and a non-nil factory")
	}
	if slices.Contains(builtinSmsTypes, typ) {
		panic("provider/message: cannot override built-in sms provider type " + typ)
	}
	senderMu.Lock()
	defer senderMu.Unlock()
	if _, dup := smsFactories[typ]; dup {
		panic("provider/message: sms provider type " + typ + " is registered twice")
	}
	smsFactories[typ] = f
}

// RegisterEmailSender 注册一个自定义邮件后端类型。语义同 RegisterSmsSender。
func RegisterEmailSender(typ string, f EmailSenderFactory) {
	if typ == "" || f == nil {
		panic("provider/message: RegisterEmailSender needs a non-empty type and a non-nil factory")
	}
	if slices.Contains(builtinEmailTypes, typ) {
		panic("provider/message: cannot override built-in email provider type " + typ)
	}
	senderMu.Lock()
	defer senderMu.Unlock()
	if _, dup := emailFactories[typ]; dup {
		panic("provider/message: email provider type " + typ + " is registered twice")
	}
	emailFactories[typ] = f
}

// lookupSmsFactory / lookupEmailFactory 取已注册的工厂。
func lookupSmsFactory(typ string) (SmsSenderFactory, bool) {
	senderMu.RLock()
	defer senderMu.RUnlock()
	f, ok := smsFactories[typ]
	return f, ok
}

func lookupEmailFactory(typ string) (EmailSenderFactory, bool) {
	senderMu.RLock()
	defer senderMu.RUnlock()
	f, ok := emailFactories[typ]
	return f, ok
}

// registeredSmsTypes / registeredEmailTypes 给错误文本用:
// 「我明明注册了」这个问题，只要错误里带上清单就能十秒定位。
func registeredSmsTypes() []string {
	senderMu.RLock()
	defer senderMu.RUnlock()
	return slices.Sorted(maps.Keys(smsFactories))
}

func registeredEmailTypes() []string {
	senderMu.RLock()
	defer senderMu.RUnlock()
	return slices.Sorted(maps.Keys(emailFactories))
}

// RegionFilter 把 include/exclude 转成可直接嵌入 sender 的过滤器。
//
// ★ 提供它是因为 Accepts 由 sender 自己实现:工厂忘了把这两个字段接上去,
// 表现是 include_regions 写了却不生效 —— 一条静默失效的配置。
func (c SmsProviderConfig) RegionFilter() message.RegionFilter {
	return message.RegionFilter{Include: c.IncludeRegions, Exclude: c.ExcludeRegions}
}

// DomainFilter 是邮件侧的对应物。
func (c EmailProviderConfig) DomainFilter() message.DomainFilter {
	return message.DomainFilter{Include: c.IncludeDomains, Exclude: c.ExcludeDomains}
}

// Decode 把这条 entry 解进调用方自己的结构体(mapstructure tag)。
func (c SmsProviderConfig) Decode(out any) error { return decodeEntry(c.Raw, out) }

// Decode 同 SmsProviderConfig.Decode。
func (c EmailProviderConfig) Decode(out any) error { return decodeEntry(c.Raw, out) }

// decodeEntry 把一条 yaml entry 解进结构体。
//
// ★★ DecodeHook 必须与 viper 的默认值一致。viper 的 UnmarshalKey 自带
// StringToTimeDuration 与 StringToSlice(",");先解成 map[string]any 之后
// `timeout: 3s` 留在 map 里是字符串,这里不挂同样的 hook 就会**静默**变成 0
// (= 用默认超时),没有任何报错。
func decodeEntry(raw map[string]any, out any) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
		WeaklyTypedInput: true,
		Result:           out,
	})
	if err != nil {
		return fmt.Errorf("provider/message: build decoder: %w", err)
	}
	return dec.Decode(raw)
}

// rawEntries 把 providers[] 读成原始 map 列表。
//
// ★ 读两次(先 map 再逐条 decode)而不是给结构体加 `mapstructure:",remain"`:
// remain 只收**没被其它字段消费掉**的键,而通用结构体已经声明了 access_key /
// region / from / endpoint 这些常见名字 —— 自定义 sender 只要撞上其中一个,
// 那个字段就不会进 Raw,而且不报错,只是取到零值。
func rawEntries(v *viper.Viper, key string) ([]map[string]any, error) {
	var raws []map[string]any
	if err := v.UnmarshalKey(key, &raws); err != nil {
		return nil, fmt.Errorf("provider/message: parse %s: %w", key, err)
	}
	return raws, nil
}
