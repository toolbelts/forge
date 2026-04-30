package message

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newAliyunTestSender(t *testing.T, endpoint string) *aliyunSmsSender {
	t.Helper()
	s, err := newAliyunSmsSender(AliyunSmsConfig{
		Name:      "test",
		AccessKey: "ak",
		SecretKey: "sk",
		Endpoint:  endpoint,
	})
	if err != nil {
		t.Fatalf("new aliyun: %v", err)
	}
	return s
}

func TestAliyun_RequestShape(t *testing.T) {
	var captured *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ResponseCode":"OK"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{
		To:       "+8613800138000",
		Content:  "hello world",
		SignName: "MySign",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if captured.Method != "GET" {
		t.Fatalf("expected GET, got %s", captured.Method)
	}
	q := captured.URL.Query()
	requiredKeys := []string{
		"Format", "Version", "AccessKeyId", "SignatureMethod",
		"Timestamp", "SignatureVersion", "SignatureNonce",
		"Action", "To", "Message", "Signature",
	}
	for _, k := range requiredKeys {
		if q.Get(k) == "" {
			t.Fatalf("missing required query param %s", k)
		}
	}
	if q.Get("Action") != aliyunSmsAction {
		t.Fatalf("Action: %s", q.Get("Action"))
	}
	if q.Get("To") != "+8613800138000" {
		t.Fatalf("To: %s", q.Get("To"))
	}
	if q.Get("Message") != "hello world" {
		t.Fatalf("Message: %s", q.Get("Message"))
	}
	if q.Get("From") != "MySign" {
		t.Fatalf("From should map from SignName, got %s", q.Get("From"))
	}
}

func TestAliyun_RequiresContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be hit when Content empty")
	}))
	t.Cleanup(srv.Close)

	s := newAliyunTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{To: "+1", TemplateId: "T"})
	if err == nil {
		t.Fatal("expected error when Content empty")
	}
}

// TestAliyunSignV1_KnownVector 用 Aliyun 文档示例验证 V1 签名实现。
//
// 取自 Aliyun 通用 V1 签名文档示例(简化为最小参数集),覆盖签名链所有关键步骤。
// 参考: https://help.aliyun.com/document_detail/315526.html (示例公开,数值固定)。
func TestAliyunSignV1_KnownVector(t *testing.T) {
	params := url.Values{
		"AccessKeyId":      []string{"testid"},
		"Action":           []string{"DescribeRegions"},
		"Format":           []string{"XML"},
		"SignatureMethod":  []string{"HMAC-SHA1"},
		"SignatureNonce":   []string{"3ee8c1b8-83d3-44af-a94f-4e0ad82fd6cf"},
		"SignatureVersion": []string{"1.0"},
		"Timestamp":        []string{"2016-02-23T12:46:24Z"},
		"Version":          []string{"2014-05-26"},
	}
	got := aliyunSignV1("GET", params, "testsecret")
	want := "OLeaidS1JvxuMvnyHOwuJ+uX5qY="
	if got != want {
		t.Fatalf("aliyun sign mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestAliyunPercentEncode(t *testing.T) {
	cases := []struct{ in, out string }{
		{"a b", "a%20b"},
		{"a+b", "a%2Bb"},
		{"*", "%2A"},
		{"~", "~"},
		{"hello", "hello"},
	}
	for _, tc := range cases {
		if got := aliyunPercentEncode(tc.in); got != tc.out {
			t.Fatalf("encode %q: want %q, got %q", tc.in, tc.out, got)
		}
	}
}

func TestAliyun_HttpErrorPropagated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"Code":"InvalidAccessKeyId"}`))
	}))
	t.Cleanup(srv.Close)

	s := newAliyunTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{To: "+1", Content: "hi"})
	if err == nil || !strings.Contains(err.Error(), "InvalidAccessKeyId") {
		t.Fatalf("expected upstream error to surface, got %v", err)
	}
}

func TestAliyun_Mode(t *testing.T) {
	s := newAliyunTestSender(t, "http://x")
	if s.Mode() != smsModeRaw {
		t.Fatalf("aliyun should be raw mode, got %v", s.Mode())
	}
}
