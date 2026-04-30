package message

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/goccy/go-json"
)

func newBytePlusTestSender(t *testing.T, baseURL string) *bytePlusSender {
	t.Helper()
	s, err := newBytePlusSender(BytePlusConfig{
		Name:       "test",
		AccessKey:  "AKLT0000",
		SecretKey:  "secret",
		Region:     "ap-singapore-1",
		SmsAccount: "acct",
		Sign:       "MySign",
	})
	if err != nil {
		t.Fatalf("new byteplus: %v", err)
	}
	s.baseURL = baseURL
	return s
}

func TestBytePlus_RequestShape(t *testing.T) {
	var captured *http.Request
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		captured = r
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ResponseMetadata":{"Action":"SendSms"}}`))
	}))
	t.Cleanup(srv.Close)

	s := newBytePlusTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{
		To:         "+8613800138000",
		TemplateId: "ST_001",
		Params:     map[string]any{"code": "1234"},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if captured.Method != "POST" {
		t.Fatalf("expected POST, got %s", captured.Method)
	}
	q := captured.URL.Query()
	if q.Get("Action") != bytePlusAction || q.Get("Version") != bytePlusVersion {
		t.Fatalf("query mismatch: %v", q)
	}
	if captured.Header.Get("Authorization") == "" {
		t.Fatal("missing Authorization header")
	}
	if !strings.HasPrefix(captured.Header.Get("Authorization"), bytePlusAlgorithm+" Credential=AKLT0000/") {
		t.Fatalf("unexpected Authorization: %s", captured.Header.Get("Authorization"))
	}
	if captured.Header.Get("X-Date") == "" || captured.Header.Get("X-Content-Sha256") == "" {
		t.Fatal("missing required signing headers")
	}

	var body bytePlusSendSmsBody
	if err := json.Unmarshal(capturedBody, &body); err != nil {
		t.Fatalf("body json: %v", err)
	}
	if body.SmsAccount != "acct" || body.Sign != "MySign" || body.TemplateID != "ST_001" {
		t.Fatalf("body fields: %+v", body)
	}
	if body.PhoneNumbers != "+8613800138000" {
		t.Fatalf("phone numbers: %q", body.PhoneNumbers)
	}
	if body.TemplateParam != `{"code":"1234"}` {
		t.Fatalf("template param: %q", body.TemplateParam)
	}
}

func TestBytePlus_RequiresTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be hit")
	}))
	t.Cleanup(srv.Close)

	s := newBytePlusTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{To: "+1", Content: "raw"})
	if err == nil {
		t.Fatal("expected error when only Content set")
	}
}

func TestBytePlus_HttpErrorPropagated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"Error":"InvalidSign"}`))
	}))
	t.Cleanup(srv.Close)

	s := newBytePlusTestSender(t, srv.URL)
	err := s.Send(context.Background(), SmsMessage{To: "+1", TemplateId: "T"})
	if err == nil || !strings.Contains(err.Error(), "InvalidSign") {
		t.Fatalf("expected upstream error to surface, got %v", err)
	}
}

func TestVolcSigningKey_Deterministic(t *testing.T) {
	k1 := volcSigningKey("secret", "20260101", "r", "svc")
	k2 := volcSigningKey("secret", "20260101", "r", "svc")
	if string(k1) != string(k2) {
		t.Fatal("signing key should be deterministic for same input")
	}
	k3 := volcSigningKey("secret", "20260102", "r", "svc")
	if string(k1) == string(k3) {
		t.Fatal("signing key should differ when date changes")
	}
}

func TestCanonicalQueryString_SortsByKey(t *testing.T) {
	got := canonicalQueryString(url.Values{
		"Version": []string{"v1"},
		"Action":  []string{"a"},
	})
	if got != "Action=a&Version=v1" {
		t.Fatalf("unexpected canonical query: %q", got)
	}
}

func TestBytePlus_Mode(t *testing.T) {
	s := newBytePlusTestSender(t, "http://x")
	if s.Mode() != smsModeTemplate {
		t.Fatalf("byteplus should be template mode, got %v", s.Mode())
	}
}
