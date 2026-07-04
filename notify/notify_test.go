package notify

import (
	"context"
	"errors"
	"testing"
)

type fakeNotifier struct {
	err   error
	calls int
}

func (f *fakeNotifier) Send(_ context.Context, _, _ string) error {
	f.calls++
	return f.err
}

// TestNew_NoValidBackendsNoop 全部 Option 参数为空时退化为 noop,Send 返回 nil。
func TestNew_NoValidBackendsNoop(t *testing.T) {
	n := New(WithTelegram("", ""), WithLark(""))
	if err := n.Send(context.Background(), "t", "c"); err != nil {
		t.Fatalf("noop send: %v", err)
	}
}

// TestMultiNotifier_AllSuccess 全部后端成功时返回 nil,且每个后端各被调用一次。
func TestMultiNotifier_AllSuccess(t *testing.T) {
	a, b := &fakeNotifier{}, &fakeNotifier{}
	m := &multiNotifier{notifiers: []Notifier{a, b}}
	if err := m.Send(context.Background(), "t", "c"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if a.calls != 1 || b.calls != 1 {
		t.Fatalf("expected each notifier called once, got a=%d b=%d", a.calls, b.calls)
	}
}

// TestMultiNotifier_JoinsAllErrors 单个后端失败不阻断其余后端,
// 返回值聚合所有失败(errors.Is 对每个都命中)。
func TestMultiNotifier_JoinsAllErrors(t *testing.T) {
	errA := errors.New("a down")
	errB := errors.New("b down")
	a := &fakeNotifier{err: errA}
	ok := &fakeNotifier{}
	b := &fakeNotifier{err: errB}
	m := &multiNotifier{notifiers: []Notifier{a, ok, b}}

	err := m.Send(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error when some notifiers fail")
	}
	if !errors.Is(err, errA) {
		t.Fatalf("expected joined error to include first failure, got %v", err)
	}
	if !errors.Is(err, errB) {
		t.Fatalf("expected joined error to include last failure, got %v", err)
	}
	if a.calls != 1 || ok.calls != 1 || b.calls != 1 {
		t.Fatalf("expected all notifiers attempted, got a=%d ok=%d b=%d", a.calls, ok.calls, b.calls)
	}
}
