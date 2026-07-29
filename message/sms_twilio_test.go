package message

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// twilioFakeServer 用 httptest 替换真实 Twilio,记录最近一次请求。
type twilioFakeServer struct {
	srv    *httptest.Server
	last   *http.Request
	body   url.Values
	status int // 默认 200
}

func newTwilioFakeServer(t *testing.T) *twilioFakeServer {
	t.Helper()
	tf := &twilioFakeServer{status: http.StatusCreated}
	tf.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		tf.last = r
		tf.body = r.PostForm
		w.WriteHeader(tf.status)
		_, _ = w.Write([]byte(`{"sid":"SM_TEST"}`))
	}))
	t.Cleanup(tf.srv.Close)
	return tf
}

func newTwilioTestSender(t *testing.T, baseURL string) *twilioSender {
	t.Helper()
	s, err := newTwilioSender(TwilioConfig{
		Name:       "test",
		AccountSid: "AC123",
		AuthToken:  "tok",
		From:       "+15550000000",
	})
	if err != nil {
		t.Fatalf("new twilio: %v", err)
	}
	s.baseURL = baseURL
	return s
}

func TestTwilio_RequestShape(t *testing.T) {
	srv := newTwilioFakeServer(t)
	s := newTwilioTestSender(t, srv.srv.URL)

	err := s.Send(context.Background(), SmsMessage{
		To:      "+15551234567",
		Content: "hello",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if srv.last.Method != "POST" {
		t.Fatalf("expected POST, got %s", srv.last.Method)
	}
	if !strings.HasSuffix(srv.last.URL.Path, "/2010-04-01/Accounts/AC123/Messages.json") {
		t.Fatalf("unexpected path: %s", srv.last.URL.Path)
	}
	if got := srv.body.Get("To"); got != "+15551234567" {
		t.Fatalf("To mismatch: %q", got)
	}
	if got := srv.body.Get("From"); got != "+15550000000" {
		t.Fatalf("From mismatch: %q", got)
	}
	if got := srv.body.Get("Body"); got != "hello" {
		t.Fatalf("Body mismatch: %q", got)
	}
	if got := srv.last.Header.Get("Authorization"); !strings.HasPrefix(got, "Basic ") {
		t.Fatalf("expected Basic auth header, got %q", got)
	}
}

func TestTwilio_RequiresContent(t *testing.T) {
	srv := newTwilioFakeServer(t)
	s := newTwilioTestSender(t, srv.srv.URL)
	err := s.Send(context.Background(), SmsMessage{
		To:         "+15551234567",
		TemplateId: "tpl",
	})
	if err == nil {
		t.Fatal("expected error when Content empty")
	}
}

func TestTwilio_PropagatesHttpError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":21211,"message":"invalid To"}`))
	}))
	t.Cleanup(srv.Close)

	s := newTwilioTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{To: "bad", Content: "hi"})
	if err == nil {
		t.Fatal("expected error on 400 response")
	}
	if !strings.Contains(err.Error(), "21211") {
		t.Fatalf("expected upstream body in error, got %v", err)
	}
}

func TestTwilio_Mode(t *testing.T) {
	s := newTwilioTestSender(t, "http://x")
	if s.Mode() != SmsModeRaw {
		t.Fatalf("twilio should be raw mode, got %v", s.Mode())
	}
	if !strings.HasPrefix(s.Name(), "twilio:") {
		t.Fatalf("unexpected name %q", s.Name())
	}
}
