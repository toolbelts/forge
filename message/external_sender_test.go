package message

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// externalSms 模拟一个【forge 之外】实现的短信后端。
//
// ★ 这个类型只用到导出的标识符：SmsSender / SmsMode / SmsModeRaw / RegionFilter。
// 它编译得过，就证明外部包也能实现同一个接口——那正是导出这几个名字的全部目的。
type externalSms struct {
	RegionFilter
	name string
	sent []SmsMessage
	err  error
}

func (e *externalSms) Send(_ context.Context, msg SmsMessage) error {
	if e.err != nil {
		return e.err
	}
	e.sent = append(e.sent, msg)
	return nil
}

func (e *externalSms) Mode() SmsMode { return SmsModeRaw }
func (e *externalSms) Name() string  { return e.name }

// externalEmail 同理，验证邮件侧。
type externalEmail struct {
	DomainFilter
	sent []EmailMessage
}

func (e *externalEmail) Send(_ context.Context, msg EmailMessage) error {
	e.sent = append(e.sent, msg)
	return nil
}

func (e *externalEmail) Name() string { return "external-email" }

// TestWithSmsSenderRegistersExternalBackend 覆盖外部 sender 的注册与发送。
func TestWithSmsSenderRegistersExternalBackend(t *testing.T) {
	ext := &externalSms{name: "kirim:primary"}

	m, err := New(WithSmsSender(ext))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m.SendSms(context.Background(), SmsMessage{To: "+62812345678", Content: "123456"}); err != nil {
		t.Fatalf("SendSms: %v", err)
	}
	if len(ext.sent) != 1 {
		t.Fatalf("外部 sender 应当收到 1 条，实得 %d", len(ext.sent))
	}
	if ext.sent[0].Content != "123456" {
		t.Errorf("正文 = %q", ext.sent[0].Content)
	}
}

// TestExternalSenderJoinsFallbackChain 保证外部 sender 与内置 backend 共享
// 同一条 fallback 链，顺序按 Option 注册顺序。
func TestExternalSenderJoinsFallbackChain(t *testing.T) {
	first := &externalSms{name: "first", err: errors.New("upstream down")}
	second := &externalSms{name: "second"}

	m, err := New(WithSmsSender(first), WithSmsSender(second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m.SendSms(context.Background(), SmsMessage{To: "+62812345678", Content: "x"}); err != nil {
		t.Fatalf("SendSms: %v", err)
	}
	if len(second.sent) != 1 {
		t.Fatal("第一个失败之后应当回落到第二个")
	}
}

// TestExternalSenderRespectsRegionFilter 保证嵌入 RegionFilter 就能白拿区号筛选。
//
// ★ 这是 RegionFilter 必须导出的理由：不导出的话，外部实现只能自己再写一遍
// 前缀匹配，而「+62 / 0062 / 62 三种写法都要认」这种细节抄错了不会有人发现。
func TestExternalSenderRespectsRegionFilter(t *testing.T) {
	ext := &externalSms{
		RegionFilter: RegionFilter{Include: []string{"62"}},
		name:         "id-only",
	}
	m, err := New(WithSmsSender(ext))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 印尼号：接受。
	if err := m.SendSms(context.Background(), SmsMessage{To: "+62812345678", Content: "x"}); err != nil {
		t.Fatalf("印尼号应当发得出去: %v", err)
	}
	// 中国号：这个 sender 不接，而且没有别的 backend 可回落。
	err = m.SendSms(context.Background(), SmsMessage{To: "+8613800138000", Content: "x"})
	if err == nil {
		t.Fatal("没有 sender 愿意承接时应当报错")
	}
	if len(ext.sent) != 1 {
		t.Fatalf("只该发出印尼那一条，实得 %d 条", len(ext.sent))
	}
}

// TestWithEmailSenderRegistersExternalBackend 覆盖邮件侧。
func TestWithEmailSenderRegistersExternalBackend(t *testing.T) {
	ext := &externalEmail{}
	m, err := New(WithEmailSender(ext))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m.SendEmail(context.Background(), EmailMessage{
		To: "user@example.com", Subject: "hi", Text: "body",
	}); err != nil {
		t.Fatalf("SendEmail: %v", err)
	}
	if len(ext.sent) != 1 {
		t.Fatalf("外部 email sender 应当收到 1 封，实得 %d", len(ext.sent))
	}
}

// TestWithNilSenderIsAConfigError 保证传 nil 是配置错误而不是静默忽略。
//
// 静默忽略的表现是「配了却不生效」，而那在发短信这件事上要到用户
// 收不到验证码时才会被发现。
func TestWithNilSenderIsAConfigError(t *testing.T) {
	if _, err := New(WithSmsSender(nil)); err == nil {
		t.Error("WithSmsSender(nil) 应当报错")
	} else if !strings.Contains(err.Error(), "nil sender") {
		t.Errorf("错误信息应当点明是 nil sender: %v", err)
	}
	if _, err := New(WithEmailSender(nil)); err == nil {
		t.Error("WithEmailSender(nil) 应当报错")
	}
}
