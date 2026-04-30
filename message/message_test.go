package message

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakeEmailSender 是 emailSender 的测试 stub,记录 Send 调用并按预设序列返回错误。
type fakeEmailSender struct {
	name    string
	errs    []error // 第 N 次 Send 返回 errs[N];超界则返回 nil
	calls   int
	lastTo  string
	lastCc  []string
	lastBcc []string
	accepts func(addr string) bool
}

func (f *fakeEmailSender) Name() string { return f.name }
func (f *fakeEmailSender) Accepts(addr string) bool {
	if f.accepts != nil {
		return f.accepts(addr)
	}
	return true
}

func (f *fakeEmailSender) Send(_ context.Context, msg EmailMessage) error {
	defer func() { f.calls++ }()
	f.lastTo = msg.To
	f.lastCc = msg.Cc
	f.lastBcc = msg.Bcc
	if f.calls < len(f.errs) {
		return f.errs[f.calls]
	}
	return nil
}

type fakeSmsSender struct {
	name    string
	mode    smsMode
	errs    []error
	calls   int
	lastTo  string
	accepts func(to string) bool
}

func (f *fakeSmsSender) Name() string  { return f.name }
func (f *fakeSmsSender) Mode() smsMode { return f.mode }
func (f *fakeSmsSender) Accepts(to string) bool {
	if f.accepts != nil {
		return f.accepts(to)
	}
	return true
}
func (f *fakeSmsSender) Send(_ context.Context, msg SmsMessage) error {
	defer func() { f.calls++ }()
	f.lastTo = msg.To
	if f.calls < len(f.errs) {
		return f.errs[f.calls]
	}
	return nil
}

func validEmail() EmailMessage {
	return EmailMessage{
		To:      "a@b.com",
		Subject: "subj",
		Text:    "hi",
	}
}

func TestSendEmail_NoSender(t *testing.T) {
	m := &Manager{templates: map[string]*emailTemplate{}}
	if err := m.SendEmail(context.Background(), validEmail()); !errors.Is(err, ErrNoEmailSender) {
		t.Fatalf("expected ErrNoEmailSender, got %v", err)
	}
}

func TestSendEmail_FallbackOnFirstFailure(t *testing.T) {
	primary := &fakeEmailSender{name: "p", errs: []error{fmt.Errorf("dial timeout")}}
	fallback := &fakeEmailSender{name: "f"}
	m := &Manager{
		email:     []emailSender{primary, fallback},
		templates: map[string]*emailTemplate{},
	}
	if err := m.SendEmail(context.Background(), validEmail()); err != nil {
		t.Fatalf("expected fallback success, got %v", err)
	}
	if primary.calls != 1 || fallback.calls != 1 {
		t.Fatalf("expected both called once, got primary=%d fallback=%d", primary.calls, fallback.calls)
	}
}

func TestSendEmail_AllFailReturnsLastErr(t *testing.T) {
	last := fmt.Errorf("last error")
	m := &Manager{
		email: []emailSender{
			&fakeEmailSender{name: "p", errs: []error{fmt.Errorf("first error")}},
			&fakeEmailSender{name: "f", errs: []error{last}},
		},
		templates: map[string]*emailTemplate{},
	}
	err := m.SendEmail(context.Background(), validEmail())
	if err == nil {
		t.Fatal("expected error when all senders fail")
	}
	if !errors.Is(err, last) {
		t.Fatalf("expected wrapped last error, got %v", err)
	}
}

func TestSendEmail_StopsOnFirstSuccess(t *testing.T) {
	primary := &fakeEmailSender{name: "p"}
	fallback := &fakeEmailSender{name: "f"}
	m := &Manager{
		email:     []emailSender{primary, fallback},
		templates: map[string]*emailTemplate{},
	}
	if err := m.SendEmail(context.Background(), validEmail()); err != nil {
		t.Fatalf("send: %v", err)
	}
	if primary.calls != 1 {
		t.Fatalf("primary should be called once, got %d", primary.calls)
	}
	if fallback.calls != 0 {
		t.Fatalf("fallback should not be called, got %d", fallback.calls)
	}
}

func TestSendEmail_InvalidMessage(t *testing.T) {
	m := &Manager{
		email:     []emailSender{&fakeEmailSender{name: "p"}},
		templates: map[string]*emailTemplate{},
	}
	cases := []EmailMessage{
		{},                                // missing everything
		{To: "a@b", Subject: "s"},         // missing body
		{To: "a@b", Text: "t"},            // missing subject
		{Subject: "s", Text: "t"},         // missing to
	}
	for i, msg := range cases {
		if err := m.SendEmail(context.Background(), msg); !errors.Is(err, ErrInvalidEmailMessage) {
			t.Fatalf("case %d expected ErrInvalidEmailMessage, got %v", i, err)
		}
	}
}

func TestSendEmail_TemplateNotFound(t *testing.T) {
	m := &Manager{
		email:     []emailSender{&fakeEmailSender{name: "p"}},
		templates: map[string]*emailTemplate{},
	}
	err := m.SendEmail(context.Background(), EmailMessage{To: "a@b", TemplateId: "missing"})
	if !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("expected ErrTemplateNotFound, got %v", err)
	}
}

func TestSendEmail_TemplateRendersAndDispatches(t *testing.T) {
	tpl, err := compileEmailTemplate("welcome", "Hi {{.Name}}", "<p>code:{{.Code}}</p>", "")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sender := &fakeEmailSender{name: "p"}
	m := &Manager{
		email:     []emailSender{sender},
		templates: map[string]*emailTemplate{"welcome": tpl},
	}
	err = m.SendEmail(context.Background(), EmailMessage{
		To:         "a@b",
		TemplateId: "welcome",
		Params:     map[string]any{"Name": "Bob", "Code": "X"},
	})
	if err != nil {
		t.Fatalf("send template: %v", err)
	}
	if sender.calls != 1 || sender.lastTo != "a@b" {
		t.Fatalf("expected one send to a@b, got calls=%d lastTo=%v", sender.calls, sender.lastTo)
	}
}

// TestSendEmail_DomainFilterIncludeOnly include 命中才路由,不命中且无其它 sender → ErrNoCompatibleEmailSender。
func TestSendEmail_DomainFilterIncludeOnly(t *testing.T) {
	cn := &fakeEmailSender{name: "cn", accepts: func(addr string) bool {
		return domainFilter{Include: []string{".cn"}}.Accepts(addr)
	}}
	m := &Manager{
		email:     []emailSender{cn},
		templates: map[string]*emailTemplate{},
	}
	if err := m.SendEmail(context.Background(), EmailMessage{
		To:      "a@example.cn",
		Subject: "s",
		Text:    "hi",
	}); err != nil {
		t.Fatalf(".cn should route to cn: %v", err)
	}
	if cn.calls != 1 {
		t.Fatalf("cn should be called once, got %d", cn.calls)
	}

	cn.calls = 0
	err := m.SendEmail(context.Background(), EmailMessage{
		To:      "a@gmail.com",
		Subject: "s",
		Text:    "hi",
	})
	if !errors.Is(err, ErrNoCompatibleEmailSender) {
		t.Fatalf("@gmail.com with cn-only should be ErrNoCompatibleEmailSender, got %v", err)
	}
	if cn.calls != 0 {
		t.Fatalf("cn should not be called, got %d", cn.calls)
	}
}

// TestSendEmail_DomainFilterRoutesPerCall 域名互斥的两个 sender,两次独立调用 SendEmail
// 应分别打到对应 provider,Cc/Bcc 跟随选中的 sender 一次性下发。
func TestSendEmail_DomainFilterRoutesPerCall(t *testing.T) {
	cn := &fakeEmailSender{name: "cn", accepts: func(addr string) bool {
		return domainFilter{Include: []string{".cn"}}.Accepts(addr)
	}}
	intl := &fakeEmailSender{name: "intl", accepts: func(addr string) bool {
		return domainFilter{Exclude: []string{".cn"}}.Accepts(addr)
	}}
	m := &Manager{
		email:     []emailSender{cn, intl},
		templates: map[string]*emailTemplate{},
	}

	for _, to := range []string{"a@example.cn", "b@gmail.com"} {
		if err := m.SendEmail(context.Background(), EmailMessage{
			To:      to,
			Cc:      []string{"cc@x.com"},
			Bcc:     []string{"bcc@y.com"},
			Subject: "s",
			Text:    "hi",
		}); err != nil {
			t.Fatalf("send to %s: %v", to, err)
		}
	}
	if cn.calls != 1 || cn.lastTo != "a@example.cn" {
		t.Fatalf("cn should send a@example.cn, got calls=%d lastTo=%q", cn.calls, cn.lastTo)
	}
	if intl.calls != 1 || intl.lastTo != "b@gmail.com" {
		t.Fatalf("intl should send b@gmail.com, got calls=%d lastTo=%q", intl.calls, intl.lastTo)
	}
	for _, s := range []*fakeEmailSender{cn, intl} {
		if len(s.lastCc) != 1 || s.lastCc[0] != "cc@x.com" {
			t.Fatalf("%s should receive Cc, got %+v", s.name, s.lastCc)
		}
		if len(s.lastBcc) != 1 || s.lastBcc[0] != "bcc@y.com" {
			t.Fatalf("%s should receive Bcc, got %+v", s.name, s.lastBcc)
		}
	}
}

// TestSendEmail_FailureSurfacesTo 单 To 全部 sender 失败时,错误文本应包含 To。
func TestSendEmail_FailureSurfacesTo(t *testing.T) {
	a := &fakeEmailSender{name: "a", errs: []error{fmt.Errorf("a boom")}}
	m := &Manager{
		email:     []emailSender{a},
		templates: map[string]*emailTemplate{},
	}
	err := m.SendEmail(context.Background(), EmailMessage{
		To:      "x@y.com",
		Subject: "s",
		Text:    "hi",
	})
	if err == nil {
		t.Fatal("expected error when sender fails")
	}
	if !strings.Contains(err.Error(), "x@y.com") {
		t.Fatalf("error should name failed recipient, got %v", err)
	}
}

func TestDomainFilter_AcceptsLogic(t *testing.T) {
	cases := []struct {
		name   string
		filter domainFilter
		addr   string
		want   bool
	}{
		{"empty allows all", domainFilter{}, "a@example.cn", true},
		{"include hit suffix", domainFilter{Include: []string{".cn"}}, "a@example.cn", true},
		{"include hit exact", domainFilter{Include: []string{"gmail.com"}}, "b@gmail.com", true},
		{"include miss", domainFilter{Include: []string{".cn"}}, "b@gmail.com", false},
		{"exclude hit", domainFilter{Exclude: []string{".cn"}}, "a@example.cn", false},
		{"exclude miss", domainFilter{Exclude: []string{".cn"}}, "b@gmail.com", true},
		{"include+exclude both hit → reject", domainFilter{Include: []string{".cn"}, Exclude: []string{".cn"}}, "a@example.cn", false},
		{"case-insensitive address", domainFilter{Include: []string{".cn"}}, "A@Example.CN", true},
		{"case-insensitive suffix", domainFilter{Include: []string{".CN"}}, "a@example.cn", true},
		{"no @ falls back to default match", domainFilter{Include: []string{"example.cn"}}, "example.cn", true},
		{"empty suffix in list ignored", domainFilter{Include: []string{"", ".cn"}}, "b@gmail.com", false},
		{"sub-domain matches parent suffix", domainFilter{Include: []string{".cn"}}, "x@a.b.cn", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.filter.Accepts(tc.addr); got != tc.want {
				t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestSendSms_NoSender(t *testing.T) {
	m := &Manager{}
	if err := m.SendSms(context.Background(), SmsMessage{To: "+1", Content: "hi"}); !errors.Is(err, ErrNoSmsSender) {
		t.Fatalf("expected ErrNoSmsSender, got %v", err)
	}
}

func TestSendSms_NoCompatibleMode(t *testing.T) {
	// 仅 Twilio (raw),消息却用 template mode
	m := &Manager{
		sms: []smsSender{&fakeSmsSender{name: "t", mode: smsModeRaw}},
	}
	err := m.SendSms(context.Background(), SmsMessage{To: "+1", TemplateId: "tpl"})
	if !errors.Is(err, ErrNoCompatibleSmsSender) {
		t.Fatalf("expected ErrNoCompatibleSmsSender, got %v", err)
	}
}

func TestSendSms_FilterByMode(t *testing.T) {
	rawSender := &fakeSmsSender{name: "t", mode: smsModeRaw, errs: []error{fmt.Errorf("twilio fail")}}
	tplSender := &fakeSmsSender{name: "bp", mode: smsModeTemplate}
	m := &Manager{sms: []smsSender{rawSender, tplSender}}

	// template mode → 跳过 raw sender,直接打到 template sender
	err := m.SendSms(context.Background(), SmsMessage{To: "+1", TemplateId: "tpl"})
	if err != nil {
		t.Fatalf("expected template sender to succeed, got %v", err)
	}
	if rawSender.calls != 0 {
		t.Fatalf("raw sender should not be called for template mode, got %d", rawSender.calls)
	}
	if tplSender.calls != 1 {
		t.Fatalf("template sender should be called once, got %d", tplSender.calls)
	}
}

func TestSendSms_FallbackBetweenCompatibleSenders(t *testing.T) {
	a := &fakeSmsSender{name: "a", mode: smsModeRaw, errs: []error{fmt.Errorf("a fail")}}
	b := &fakeSmsSender{name: "b", mode: smsModeRaw}
	m := &Manager{sms: []smsSender{a, b}}
	if err := m.SendSms(context.Background(), SmsMessage{To: "+1", Content: "hi"}); err != nil {
		t.Fatalf("expected fallback success, got %v", err)
	}
	if a.calls != 1 || b.calls != 1 {
		t.Fatalf("expected a=1 b=1, got a=%d b=%d", a.calls, b.calls)
	}
}

func TestSendSms_BothModeAcceptsAnything(t *testing.T) {
	s := &fakeSmsSender{name: "x", mode: smsModeBoth}
	m := &Manager{sms: []smsSender{s}}
	for _, msg := range []SmsMessage{
		{To: "+1", Content: "raw"},
		{To: "+1", TemplateId: "t"},
	} {
		if err := m.SendSms(context.Background(), msg); err != nil {
			t.Fatalf("mode=both should accept any, got %v on %+v", err, msg)
		}
	}
}

func TestSendSms_InvalidMessage(t *testing.T) {
	m := &Manager{sms: []smsSender{&fakeSmsSender{mode: smsModeBoth}}}
	cases := []SmsMessage{
		{},
		{To: "+1"},      // neither Content nor TemplateId
		{Content: "hi"}, // no To
	}
	for i, msg := range cases {
		if err := m.SendSms(context.Background(), msg); !errors.Is(err, ErrInvalidSmsMessage) {
			t.Fatalf("case %d expected ErrInvalidSmsMessage, got %v", i, err)
		}
	}
}

func TestNew_NoBackendConfigured(t *testing.T) {
	if _, err := New(); !errors.Is(err, ErrNoBackendConfigured) {
		t.Fatalf("expected ErrNoBackendConfigured, got %v", err)
	}
}

func TestNew_TemplateOnlyStillNeedsBackend(t *testing.T) {
	// 只注册模板没有任何 sender → 仍应报 ErrNoBackendConfigured
	_, err := New(WithEmailTemplate("welcome", "s", "<p>x</p>", ""))
	if !errors.Is(err, ErrNoBackendConfigured) {
		t.Fatalf("expected ErrNoBackendConfigured, got %v", err)
	}
}

func TestNew_BadTemplateFailsFast(t *testing.T) {
	_, err := New(
		WithSmtp(SmtpConfig{Name: "x", Host: "h", Port: 587, From: "a@b"}),
		WithEmailTemplate("bad", "{{ .Unclosed", "", ""),
	)
	if err == nil {
		t.Fatal("expected template parse error to surface from New")
	}
}

// TestSendSms_RegionFilterIncludeOnly include_regions 命中才路由到该 sender,
// 不命中且无其它兼容 sender → ErrNoCompatibleSmsSender。
func TestSendSms_RegionFilterIncludeOnly(t *testing.T) {
	cn := &fakeSmsSender{name: "cn", mode: smsModeRaw, accepts: func(to string) bool {
		return regionFilter{Include: []string{"86"}}.Accepts(to)
	}}
	m := &Manager{sms: []smsSender{cn}}
	if err := m.SendSms(context.Background(), SmsMessage{To: "+8613800138000", Content: "hi"}); err != nil {
		t.Fatalf("+86 should route to cn: %v", err)
	}
	if cn.calls != 1 {
		t.Fatalf("cn should be called once, got %d", cn.calls)
	}
	// +1 号码该 sender 不接受,没有其它 sender → 应得 ErrNoCompatibleSmsSender
	cn.calls = 0
	err := m.SendSms(context.Background(), SmsMessage{To: "+15551234567", Content: "hi"})
	if !errors.Is(err, ErrNoCompatibleSmsSender) {
		t.Fatalf("+1 with cn-only should be ErrNoCompatibleSmsSender, got %v", err)
	}
	if cn.calls != 0 {
		t.Fatalf("cn should not be called for excluded region, got %d", cn.calls)
	}
}

// TestSendSms_RegionFilterRoutesPerCall 区号互斥的两个 sender,不同号码独立调用各走各的,互不重叠。
func TestSendSms_RegionFilterRoutesPerCall(t *testing.T) {
	cn := &fakeSmsSender{name: "cn", mode: smsModeRaw, accepts: func(to string) bool {
		return regionFilter{Include: []string{"86"}}.Accepts(to)
	}}
	intl := &fakeSmsSender{name: "intl", mode: smsModeRaw, accepts: func(to string) bool {
		return regionFilter{Exclude: []string{"86"}}.Accepts(to)
	}}
	m := &Manager{sms: []smsSender{cn, intl}}
	for _, to := range []string{"+8613800138000", "+15551234567"} {
		if err := m.SendSms(context.Background(), SmsMessage{To: to, Content: "hi"}); err != nil {
			t.Fatalf("send %s: %v", to, err)
		}
	}
	if cn.calls != 1 || cn.lastTo != "+8613800138000" {
		t.Fatalf("cn should send only +86, got calls=%d lastTo=%q", cn.calls, cn.lastTo)
	}
	if intl.calls != 1 || intl.lastTo != "+15551234567" {
		t.Fatalf("intl should send only +1, got calls=%d lastTo=%q", intl.calls, intl.lastTo)
	}
}

// TestSendSms_FailureSurfacesTo 单号码所有兼容 sender 都失败时,错误文本应包含该号码。
func TestSendSms_FailureSurfacesTo(t *testing.T) {
	a := &fakeSmsSender{name: "a", mode: smsModeRaw, errs: []error{fmt.Errorf("boom")}}
	m := &Manager{sms: []smsSender{a}}
	err := m.SendSms(context.Background(), SmsMessage{To: "+15551234567", Content: "hi"})
	if err == nil {
		t.Fatal("expected error when sender fails")
	}
	if !strings.Contains(err.Error(), "+15551234567") {
		t.Fatalf("error should name failed recipient, got %v", err)
	}
}

func TestRegionFilter_AcceptsLogic(t *testing.T) {
	cases := []struct {
		name   string
		filter regionFilter
		to     string
		want   bool
	}{
		{"empty allows all", regionFilter{}, "+8613800138000", true},
		{"include hit", regionFilter{Include: []string{"86"}}, "+8613800138000", true},
		{"include miss", regionFilter{Include: []string{"86"}}, "+15551234567", false},
		{"exclude hit", regionFilter{Exclude: []string{"86"}}, "+8613800138000", false},
		{"exclude miss", regionFilter{Exclude: []string{"86"}}, "+15551234567", true},
		{"include+exclude both hit → reject", regionFilter{Include: []string{"86"}, Exclude: []string{"86"}}, "+8613800138000", false},
		{"00 prefix recognized", regionFilter{Include: []string{"86"}}, "008613800138000", true},
		{"no + or 00 still works", regionFilter{Include: []string{"86"}}, "8613800138000", true},
		{"longer prefix selectivity", regionFilter{Include: []string{"852"}}, "+85298765432", true},
		{"longer prefix not matching shorter", regionFilter{Include: []string{"852"}}, "+8613800138000", false},
		{"empty prefix in list ignored", regionFilter{Include: []string{"", "86"}}, "+15551234567", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.filter.Accepts(tc.to); got != tc.want {
				t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestIsTransientErr(t *testing.T) {
	if IsTransientErr(nil) {
		t.Fatal("nil should be non-transient")
	}
	if IsTransientErr(ErrNoEmailSender) {
		t.Fatal("config-class sentinel should be non-transient")
	}
	if IsTransientErr(ErrTemplateNotFound) {
		t.Fatal("template sentinel should be non-transient")
	}
	if !IsTransientErr(fmt.Errorf("network blip")) {
		t.Fatal("unknown error should be considered transient (worth retry)")
	}
}
