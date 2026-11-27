package provider

import (
	"context"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/message"
)

// fakeSms 是用例里的自定义短信后端。
type fakeSms struct {
	message.RegionFilter
	name string
	cfg  fakeSmsConfig
	sent []message.SmsMessage
	fail error
}

// fakeSmsConfig 故意同时包含【自定义字段】与【与内置同名的字段】。
type fakeSmsConfig struct {
	Appkey    string        `mapstructure:"appkey"`
	AccessKey string        `mapstructure:"access_key"`
	Timeout   time.Duration `mapstructure:"timeout"`
	Retries   int           `mapstructure:"retries"`
}

func (f *fakeSms) Name() string          { return f.name }
func (f *fakeSms) Mode() message.SmsMode { return message.SmsModeBoth }
func (f *fakeSms) Send(_ context.Context, msg message.SmsMessage) error {
	if f.fail != nil {
		return f.fail
	}
	f.sent = append(f.sent, msg)
	return nil
}

// newTestViper 造一份只开短信的配置。
func newTestViper(providers []map[string]any) *viper.Viper {
	v := viper.New()
	v.Set("message.sms.enabled", true)
	v.Set("message.sms.providers", providers)
	return v
}

// TestRegisterSmsSenderRoundTrip 走完整链路：注册 → yaml 用这个 type → Manager 用得上它。
//
// ★ 顺便钉住顺序：providers[] 的顺序就是 fallback 优先级，自定义后端排第一就该第一个被调用。
func TestRegisterSmsSenderRoundTrip(t *testing.T) {
	var built *fakeSms
	registerOnce(t, "unit-wa", func(c SmsProviderConfig) (message.SmsSender, error) {
		var cfg fakeSmsConfig
		if err := c.Decode(&cfg); err != nil {
			return nil, err
		}
		built = &fakeSms{RegionFilter: c.RegionFilter(), name: c.Name, cfg: cfg}
		return built, nil
	})

	v := newTestViper([]map[string]any{{
		"type": "unit-wa", "name": "wa", "timeout": "3s", "retries": 2,
		"appkey": "k1", "access_key": "shared", "include_regions": []string{"62"},
	}})
	mgr := setupManager(t, v)

	if built == nil {
		t.Fatal("工厂没有被调用：自定义 type 没有走到注册表")
	}
	// 自定义字段与【与内置同名的】字段都要拿得到 —— 后者是 `mapstructure:",remain"` 写法会丢的那一类。
	if built.cfg.Appkey != "k1" || built.cfg.AccessKey != "shared" || built.cfg.Retries != 2 {
		t.Errorf("自定义字段没解全: %+v", built.cfg)
	}
	// timeout 写的是 "3s"：先解成 map 再 decode 时不挂 hook 的话，这里会静默变成 0。
	if built.cfg.Timeout != 3*time.Second {
		t.Errorf("timeout = %v, want 3s（decodeEntry 的 DecodeHook 没跟上 viper）", built.cfg.Timeout)
	}
	// RegionFilter 要真的接上：接漏了表现是 include_regions 写了却不生效。
	if !built.Accepts("+6281234") || built.Accepts("+8613800138000") {
		t.Error("include_regions 没有接进 RegionFilter")
	}
	if err := mgr.SendSms(t.Context(), message.SmsMessage{To: "+6281234", Content: "hi"}); err != nil {
		t.Fatalf("SendSms: %v", err)
	}
	if len(built.sent) != 1 {
		t.Errorf("自定义 sender 收到 %d 条，want 1", len(built.sent))
	}
}

// TestCustomSenderFallsBackToNext 钉住 fallback：自定义后端失败要落到下一个。
//
// 这就是「WhatsApp 发不出去就走短信」的全部机制，不需要额外的编排代码。
func TestCustomSenderFallsBackToNext(t *testing.T) {
	first := &fakeSms{name: "wa", fail: context.DeadlineExceeded}
	second := &fakeSms{name: "sms"}
	registerOnce(t, "unit-first", func(SmsProviderConfig) (message.SmsSender, error) { return first, nil })
	registerOnce(t, "unit-second", func(SmsProviderConfig) (message.SmsSender, error) { return second, nil })

	mgr := setupManager(t, newTestViper([]map[string]any{
		{"type": "unit-first", "name": "wa"},
		{"type": "unit-second", "name": "sms"},
	}))
	if err := mgr.SendSms(t.Context(), message.SmsMessage{To: "+6281234", Content: "hi"}); err != nil {
		t.Fatalf("SendSms: %v", err)
	}
	if len(second.sent) != 1 {
		t.Errorf("第一个失败之后没有落到第二个：second 收到 %d 条", len(second.sent))
	}
}

// TestRegisterPanics 钉住三条 panic：覆盖内置、重复注册、空参数。
func TestRegisterPanics(t *testing.T) {
	for name, fn := range map[string]func(){
		"覆盖内置 sms": func() {
			RegisterSmsSender("twilio", func(SmsProviderConfig) (message.SmsSender, error) { return nil, nil })
		},
		"覆盖内置 email": func() {
			RegisterEmailSender("smtp", func(EmailProviderConfig) (message.EmailSender, error) { return nil, nil })
		},
		"空 type": func() { RegisterSmsSender("", func(SmsProviderConfig) (message.SmsSender, error) { return nil, nil }) },
		"nil 工厂": func() { RegisterSmsSender("unit-nil", nil) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("应当 panic")
				}
			}()
			fn()
		})
	}
	t.Run("重复注册", func(t *testing.T) {
		registerOnce(t, "unit-dup", func(SmsProviderConfig) (message.SmsSender, error) { return &fakeSms{}, nil })
		defer func() {
			if recover() == nil {
				t.Error("重复注册应当 panic")
			}
		}()
		RegisterSmsSender("unit-dup", func(SmsProviderConfig) (message.SmsSender, error) { return &fakeSms{}, nil })
	})
}

// TestUnknownTypeErrorListsTypes 钉住未知 type 的错误文本同时列出内置与已注册清单。
//
// 「我明明注册了」这个问题，只有错误带上两份清单才能十秒分清是没注册还是注册晚了。
func TestUnknownTypeErrorListsTypes(t *testing.T) {
	registerOnce(t, "unit-listed", func(SmsProviderConfig) (message.SmsSender, error) { return &fakeSms{}, nil })
	_, err := buildSmsOpts(newTestViper([]map[string]any{{"type": "nope", "name": "x"}}))
	if err == nil {
		t.Fatal("未知 type 应当报错")
	}
	for _, want := range []string{"nope", "twilio", "unit-listed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误里缺 %q: %v", want, err)
		}
	}
}

// TestBuiltinTypeSetsMatchSwitch 钉住 builtinSmsTypes / builtinEmailTypes 与 switch 的 case 集合相等。
//
// ★ 漏一个的后果是它能被业务用 RegisterXxx 悄悄换掉，而那种替换不报错。
func TestBuiltinTypeSetsMatchSwitch(t *testing.T) {
	src := readSource(t, "message.go")
	for _, c := range []struct {
		fn    string
		types []string
	}{
		{"buildSmsOpts", builtinSmsTypes},
		{"buildEmailOpts", builtinEmailTypes},
	} {
		got := caseLabels(t, src, c.fn)
		want := slices.Clone(c.types)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s 的 case = %v，常量表 = %v", c.fn, got, want)
		}
	}
}

// readSource 读本包的一个源文件。
func readSource(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// registerOnce 注册一个用例专用 type，并在用例结束时摘掉，避免用例之间互相污染。
func registerOnce(t *testing.T, typ string, f SmsSenderFactory) {
	t.Helper()
	RegisterSmsSender(typ, f)
	t.Cleanup(func() {
		senderMu.Lock()
		delete(smsFactories, typ)
		senderMu.Unlock()
	})
}

// setupManager 跑一遍 MessageProvider 的 Setup，返回容器里的 Manager。
func setupManager(t *testing.T, v *viper.Viper) *message.Manager {
	t.Helper()
	ctx := ioc.New().WithContext(context.Background())
	ioc.FromContext(ctx).Instance(v)
	p := &MessageProvider{}
	if err := p.Setup(ctx); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	mgr, err := GetMessageManager(ctx)
	if err != nil {
		t.Fatalf("GetMessageManager: %v", err)
	}
	return mgr
}

// caseLabels 抠出一个函数里 switch 的 case 字面量。
func caseLabels(t *testing.T, src, fn string) []string {
	t.Helper()
	start := strings.Index(src, "func "+fn+"(")
	if start < 0 {
		t.Fatalf("找不到 %s", fn)
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	var out []string
	for _, m := range regexp.MustCompile(`(?m)^\s*case "([^"]+)":`).FindAllStringSubmatch(body, -1) {
		out = append(out, m[1])
	}
	return out
}
