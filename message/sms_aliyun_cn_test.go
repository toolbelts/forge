package message

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newAliyunCnTestSender(t *testing.T, endpoint, signName string) *aliyunCnSmsSender {
	t.Helper()
	s, err := newAliyunCnSmsSender(AliyunCnSmsConfig{
		Name:      "test",
		AccessKey: "ak",
		SecretKey: "sk",
		Endpoint:  endpoint,
		SignName:  signName,
	})
	if err != nil {
		t.Fatalf("new aliyun-cn: %v", err)
	}
	return s
}

func TestAliyunCn_DefaultEndpoint(t *testing.T) {
	s, err := newAliyunCnSmsSender(AliyunCnSmsConfig{
		AccessKey: "ak",
		SecretKey: "sk",
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if s.baseURL != aliyunCnSmsDefaultBaseURL {
		t.Fatalf("expected default endpoint %q, got %q", aliyunCnSmsDefaultBaseURL, s.baseURL)
	}
}

// TestAliyunCn_RequestShape 检查发出的 HTTP 请求结构 - 包含全部公共参数 + 业务参数 + 签名。
func TestAliyunCn_RequestShape(t *testing.T) {
	var captured *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Code":"OK","Message":"OK","RequestId":"R1","BizId":"B1"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "上海公司")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
		Params:     map[string]any{"code": "1234"},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if captured.Method != "GET" {
		t.Fatalf("expected GET, got %s", captured.Method)
	}
	q := captured.URL.Query()
	for _, k := range []string{
		"Format", "Version", "AccessKeyId", "SignatureMethod",
		"Timestamp", "SignatureVersion", "SignatureNonce",
		"Action", "PhoneNumbers", "SignName", "TemplateCode", "Signature",
	} {
		if q.Get(k) == "" {
			t.Fatalf("missing required query param %q", k)
		}
	}
	if q.Get("Action") != aliyunCnSmsAction {
		t.Fatalf("Action: %q", q.Get("Action"))
	}
	if q.Get("Version") != aliyunCnSmsVersion {
		t.Fatalf("Version: %q", q.Get("Version"))
	}
	if q.Get("PhoneNumbers") != "13800138000" {
		t.Fatalf("PhoneNumbers: %q", q.Get("PhoneNumbers"))
	}
	if q.Get("SignName") != "上海公司" {
		t.Fatalf("SignName mismatch: %q", q.Get("SignName"))
	}
	if q.Get("TemplateCode") != "SMS_001" {
		t.Fatalf("TemplateCode: %q", q.Get("TemplateCode"))
	}
	if q.Get("TemplateParam") != `{"code":"1234"}` {
		t.Fatalf("TemplateParam: %q", q.Get("TemplateParam"))
	}
}

// TestAliyunCn_SignNameMessageOverridesConfig 验证 SmsMessage.SignName 优先级高于 config。
func TestAliyunCn_SignNameMessageOverridesConfig(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query().Get("SignName")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Code":"OK"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "DefaultSign")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
		SignName:   "OverrideSign",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if got != "OverrideSign" {
		t.Fatalf("expected per-message SignName to win, got %q", got)
	}
}

// TestAliyunCn_SignNameConfigFallback 验证 config-level SignName 在 msg 未提供时被使用。
func TestAliyunCn_SignNameConfigFallback(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query().Get("SignName")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Code":"OK"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "ConfigSign")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if got != "ConfigSign" {
		t.Fatalf("expected ConfigSign, got %q", got)
	}
}

func TestAliyunCn_RejectMissingSignName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be hit when both SignName empty")
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
	})
	if !errors.Is(err, ErrInvalidSmsMessage) {
		t.Fatalf("expected ErrInvalidSmsMessage, got %v", err)
	}
}

func TestAliyunCn_RequiresTemplateId(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be hit when TemplateId empty")
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "Sign")
	err := s.Send(context.Background(), SmsMessage{
		To:      "13800138000",
		Content: "hi",
	})
	if !errors.Is(err, ErrInvalidSmsMessage) {
		t.Fatalf("expected ErrInvalidSmsMessage, got %v", err)
	}
}

// TestAliyunCn_BizErrorPropagated 200 OK 但 Code != "OK" 也要抛错。
func TestAliyunCn_BizErrorPropagated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Code":"isv.SMS_SIGNATURE_ILLEGAL","Message":"签名不合法","RequestId":"R2"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "Sign")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
	})
	if err == nil {
		t.Fatal("expected error on biz Code != OK")
	}
	if !strings.Contains(err.Error(), "isv.SMS_SIGNATURE_ILLEGAL") {
		t.Fatalf("expected biz code in error, got %v", err)
	}
}

func TestAliyunCn_HttpErrorPropagated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`forbidden`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "Sign")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
	})
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected 403 in error, got %v", err)
	}
}

func TestAliyunCn_Mode(t *testing.T) {
	s := newAliyunCnTestSender(t, "http://x", "Sign")
	if s.Mode() != SmsModeTemplate {
		t.Fatalf("aliyun-cn should be template mode, got %v", s.Mode())
	}
	if !strings.HasPrefix(s.Name(), "aliyun-cn") {
		t.Fatalf("unexpected name %q", s.Name())
	}
}

// TestWithAliyunCnSms_SkipsWhenIncomplete 验证 missing creds 时静默跳过。
func TestWithAliyunCnSms_SkipsWhenIncomplete(t *testing.T) {
	c := &config{}
	WithAliyunCnSms(AliyunCnSmsConfig{Name: "no-secret", AccessKey: "ak"})(c)
	WithAliyunCnSms(AliyunCnSmsConfig{Name: "no-key", SecretKey: "sk"})(c)
	if len(c.smsSpecs) != 0 {
		t.Fatalf("expected 0 sms specs from incomplete configs, got %d", len(c.smsSpecs))
	}
}

func TestWithAliyunCnSms_AllowsEmptySignName(t *testing.T) {
	// SignName 在 cfg 为空也应注册成功 - 由 SmsMessage 提供
	c := &config{}
	WithAliyunCnSms(AliyunCnSmsConfig{
		Name:      "ok",
		AccessKey: "ak",
		SecretKey: "sk",
	})(c)
	if len(c.smsSpecs) != 1 {
		t.Fatalf("expected sender registered without SignName, got %d", len(c.smsSpecs))
	}
}

// TestAliyunCn_DefaultIncludeRegion 验证 aliyun-cn 留空 IncludeRegions 时默认 ["86"]。
// 与文档承诺一致:国内通道仅承接 +86 号码。
func TestAliyunCn_DefaultIncludeRegion(t *testing.T) {
	s := newAliyunCnTestSender(t, "http://x", "Sign")
	if !s.Accepts("+8613800138000") {
		t.Fatal("+86 should be accepted by default")
	}
	if s.Accepts("+15551234567") {
		t.Fatal("+1 should be rejected by default include=[86]")
	}
}

// TestAliyunCn_ExplicitIncludeRegionsOverridesDefault 显式 include 覆盖默认。
func TestAliyunCn_ExplicitIncludeRegionsOverridesDefault(t *testing.T) {
	s, err := newAliyunCnSmsSender(AliyunCnSmsConfig{
		AccessKey:      "ak",
		SecretKey:      "sk",
		IncludeRegions: []string{"852"},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if s.Accepts("+8613800138000") {
		t.Fatal("explicit include=[852] should reject +86")
	}
	if !s.Accepts("+85298765432") {
		t.Fatal("explicit include=[852] should accept +852")
	}
}

// TestAliyunCn_QueryEncodingHasSignature 在签名步骤后 Signature 一定带上,Encode 不会丢失。
func TestAliyunCn_QueryEncodingHasSignature(t *testing.T) {
	var raw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Code":"OK"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunCnTestSender(t, srv.URL, "Sign")
	err := s.Send(context.Background(), SmsMessage{
		To:         "13800138000",
		TemplateId: "SMS_001",
		Params:     map[string]any{"code": "1234"},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	q, _ := url.ParseQuery(raw)
	if q.Get("Signature") == "" {
		t.Fatal("Signature missing from final query")
	}
}
