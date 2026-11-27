package provider

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// notifyRecorder 记录通知的参数与调用次数。
type notifyRecorder struct {
	ctx     context.Context
	title   string
	content string
	calls   int
	err     error
}

// Send 保存发送参数并返回指定错误。
func (n *notifyRecorder) Send(ctx context.Context, title, content string) error {
	n.ctx, n.title, n.content = ctx, title, content
	n.calls++
	return n.err
}

// TestAppNotifierSend 验证业务标题补齐来源、已有前缀不重复且正文与错误原样传递。
func TestAppNotifierSend(t *testing.T) {
	for _, title := range []string{
		"aurax feed heartbeat",
		"[test-host] aurax feed heartbeat",
		"aurax.worker aurax feed heartbeat",
		"[test-host] aurax.worker aurax feed heartbeat",
	} {
		t.Run(title, func(t *testing.T) {
			errSend := errors.New("send failed")
			recorder := &notifyRecorder{err: errSend}
			n := &appNotifier{notifier: recorder, hostname: "test-host", appName: "aurax.worker"}
			ctx := t.Context()
			if err := n.Send(ctx, title, "feed down\nshard 0/1"); !errors.Is(err, errSend) {
				t.Fatalf("send error = %v, want %v", err, errSend)
			}
			if recorder.title != "[test-host] aurax.worker aurax feed heartbeat" {
				t.Errorf("title = %q", recorder.title)
			}
			if recorder.ctx != ctx || recorder.content != "feed down\nshard 0/1" || recorder.calls != 1 {
				t.Errorf("send parameters changed: %+v", recorder)
			}
		})
	}
}

// TestNotifyProviderLark 验证容器取出的业务通知器、生命周期与 panic 都通过同一带来源的通道。
func TestNotifyProviderLark(t *testing.T) {
	texts := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			t.Errorf("decode lark request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		texts <- body.Content.Text
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	v := viper.New()
	v.Set("notify.lark.webhook", server.URL)
	ctx := ioc.New().WithContext(t.Context())
	ioc.FromContext(ctx).Instance(v)
	ioc.FromContext(ctx).Instance(AppName("aurax.api"))
	ioc.FromContext(ctx).Instance(&BuildInfo{Version: "v1"})
	p := &NotifyProvider{}
	if err := p.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	n := MustGetNotifier(ctx)
	if err := n.Send(ctx, "aurax payout amount mismatch", "order 123"); err != nil {
		t.Fatal(err)
	}
	if got, want := <-texts, "["+hostname+"] aurax.api aurax payout amount mismatch\norder 123"; got != want {
		t.Fatalf("business notification = %q, want %q", got, want)
	}
	for _, event := range []string{notifyEventStarted, notifyEventStopped} {
		title, content := buildLifecycleMessage(hostname, "aurax.api", event, p.buildInfo)
		p.sendLifecycle(ctx, event)
		if got, want := <-texts, title+"\n"+content; got != want {
			t.Errorf("%s notification = %q, want %q", event, got, want)
		}
	}
	title, content := buildPanicMessage(hostname, "aurax.api", "/test", "boom", []byte("stack"))
	if err := n.Send(ctx, title, content); err != nil {
		t.Fatal(err)
	}
	if got, want := <-texts, title+"\n"+content; got != want {
		t.Errorf("panic notification = %q, want %q", got, want)
	}
}

// TestNotifyProviderNoop 验证没有通知渠道时依旧成功返回。
func TestNotifyProviderNoop(t *testing.T) {
	ctx := ioc.New().WithContext(t.Context())
	ioc.FromContext(ctx).Instance(viper.New())
	ioc.FromContext(ctx).Instance(AppName("aurax.worker"))
	ioc.FromContext(ctx).Instance(&BuildInfo{})
	p := &NotifyProvider{}
	if err := p.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := MustGetNotifier(ctx).Send(ctx, "aurax feed heartbeat", "feed down"); err != nil {
		t.Fatal(err)
	}
}
