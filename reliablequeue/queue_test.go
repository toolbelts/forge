package reliablequeue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func startTestQueue(t *testing.T, queue *Queue) {
	t.Helper()
	if err := queue.Start(context.Background()); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		select {
		case <-queue.Stop():
		case <-time.After(time.Second):
			t.Fatal("stop queue timed out")
		}
	})
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met")
}

func TestNewRequiresUniversalClientAndStickyWaitClient(t *testing.T) {
	if _, err := New(nil, ""); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("nil client error = %v", err)
	}
	cluster := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = cluster.Close() })
	if _, err := New(cluster, "", WithWaitReplicas(1, time.Second)); !errors.Is(err, ErrUnsupportedClient) {
		t.Fatalf("cluster wait error = %v", err)
	}
}

func TestPublishStoresStableMessage(t *testing.T) {
	_, client := newTestClient(t)
	queue, err := New(client, "test:")
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	want := Message{Id: "message-1", Payload: []byte(`{"value":42}`), Metadata: map[string]string{"trace_id": "abc"}}
	if err := queue.Publish(context.Background(), "asset.posting", want); err != nil {
		t.Fatalf("publish: %v", err)
	}
	entries, err := client.XRange(context.Background(), queue.streamKey("asset.posting"), "-", "+").Result()
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	got, err := decodeEntry(entries[0])
	if err != nil {
		t.Fatalf("decode entry: %v", err)
	}
	if got.Id != want.Id || string(got.Payload) != string(want.Payload) || got.Metadata["trace_id"] != "abc" {
		t.Fatalf("message = %#v", got)
	}
}

func TestHandlerPublishesResultBeforeAckAndDelete(t *testing.T) {
	_, client := newTestClient(t)
	queue, err := New(client, "test:", WithBlockTimeout(10*time.Millisecond))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.Subscribe(
		"asset.command", "asset", func(_ context.Context, message Message) (Result, error) {
			return Result{Publications: []Publication{{
				Topic:   "asset.result",
				Message: Message{Id: "result-" + message.Id, Payload: []byte(`{"status":"succeeded"}`)},
			}}}, nil
		},
		WithDeleteAfterAck(true),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	startTestQueue(t, queue)
	if err := queue.Publish(context.Background(), "asset.command", Message{Id: "command-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("publish command: %v", err)
	}
	eventually(t, func() bool {
		resultLength, _ := client.XLen(context.Background(), queue.streamKey("asset.result")).Result()
		commandLength, _ := client.XLen(context.Background(), queue.streamKey("asset.command")).Result()
		return resultLength == 1 && commandLength == 0
	})
}

func TestHandlerErrorIsRecoveredFromPending(t *testing.T) {
	server, client := newTestClient(t)
	queue, err := New(
		client, "test:",
		WithBlockTimeout(10*time.Millisecond),
		WithClaimIdle(10*time.Millisecond),
		WithRecoveryInterval(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	var calls atomic.Int64
	if err := queue.Subscribe(
		"retry.command", "retry", func(_ context.Context, _ Message) (Result, error) {
			if calls.Add(1) == 1 {
				return Result{}, errors.New("temporary")
			}
			return Result{}, nil
		},
		WithDeleteAfterAck(true),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	startTestQueue(t, queue)
	if err := queue.Publish(context.Background(), "retry.command", Message{Id: "retry-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	eventually(t, func() bool { return calls.Load() >= 1 })
	server.FastForward(time.Second)
	eventually(t, func() bool {
		length, _ := client.XLen(context.Background(), queue.streamKey("retry.command")).Result()
		return calls.Load() >= 2 && length == 0
	})
}

func TestPermanentErrorPublishesDeadLetter(t *testing.T) {
	_, client := newTestClient(t)
	queue, err := New(client, "test:", WithBlockTimeout(10*time.Millisecond), WithDlqTopic("asset.dlq"))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.Subscribe(
		"invalid.command", "invalid", func(_ context.Context, _ Message) (Result, error) {
			return Result{}, Permanent(errors.New("unsupported version"))
		},
		WithDeleteAfterAck(true),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	startTestQueue(t, queue)
	if err := queue.Publish(context.Background(), "invalid.command", Message{Id: "invalid-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	eventually(t, func() bool {
		dlqLength, _ := client.XLen(context.Background(), queue.streamKey("asset.dlq")).Result()
		inputLength, _ := client.XLen(context.Background(), queue.streamKey("invalid.command")).Result()
		return dlqLength == 1 && inputLength == 0
	})
	entries, err := client.XRange(context.Background(), queue.streamKey("asset.dlq"), "-", "+").Result()
	if err != nil {
		t.Fatalf("range dlq: %v", err)
	}
	message, err := decodeEntry(entries[0])
	if err != nil {
		t.Fatalf("decode dlq: %v", err)
	}
	if message.Metadata["reliablequeue_original_topic"] != "invalid.command" ||
		message.Metadata["reliablequeue_error"] != "unsupported version" {
		t.Fatalf("dlq metadata = %#v", message.Metadata)
	}
}

func TestWaitFailureLeavesPublishedMessageForRetry(t *testing.T) {
	_, client := newTestClient(t)
	queue, err := New(client, "test:", WithWaitReplicas(1, 20*time.Millisecond))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	err = queue.Publish(context.Background(), "asset.command", Message{Id: "wait-1", Payload: []byte(`{}`)})
	if !errors.Is(err, ErrReplicationUnconfirmed) {
		t.Fatalf("publish error = %v", err)
	}
	length, err := client.XLen(context.Background(), queue.streamKey("asset.command")).Result()
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if length != 1 {
		t.Fatalf("stream length = %d", length)
	}
}

func TestResultWaitFailureDoesNotAckInput(t *testing.T) {
	_, client := newTestClient(t)
	queue, err := New(
		client, "test:",
		WithWaitReplicas(1, 20*time.Millisecond),
		WithBlockTimeout(10*time.Millisecond),
		WithClaimIdle(time.Hour),
		WithRecoveryInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	var handled atomic.Bool
	if err := queue.Subscribe(
		"wait.command", "wait", func(_ context.Context, message Message) (Result, error) {
			handled.Store(true)
			return Result{Publications: []Publication{{
				Topic: "wait.result", Message: Message{Id: "result-" + message.Id, Payload: []byte(`{}`)},
			}}}, nil
		},
		WithDeleteAfterAck(true),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	startTestQueue(t, queue)
	if err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: queue.streamKey("wait.command"), Values: []any{
			messageIdField, "command-1", payloadField, `{}`, metadataField, `{}`,
		},
	}).Err(); err != nil {
		t.Fatalf("seed command: %v", err)
	}
	eventually(t, func() bool {
		pending, err := client.XPending(context.Background(), queue.streamKey("wait.command"), "wait").Result()
		return err == nil && handled.Load() && pending.Count == 1
	})
}

type recordingMetrics struct {
	mu          sync.Mutex
	published   int
	consumed    int
	redelivered int
	dlq         int
}

func (metrics *recordingMetrics) PublishTotal(string) {
	metrics.mu.Lock()
	metrics.published++
	metrics.mu.Unlock()
}
func (*recordingMetrics) PublishError(string)       {}
func (*recordingMetrics) ReplicaWaitTimeout(string) {}
func (metrics *recordingMetrics) ConsumeTotal(_ string, redelivered bool) {
	metrics.mu.Lock()
	metrics.consumed++
	if redelivered {
		metrics.redelivered++
	}
	metrics.mu.Unlock()
}
func (*recordingMetrics) HandlerError(string, bool)                {}
func (*recordingMetrics) Pending(string, string, int64)            {}
func (*recordingMetrics) ProcessingDuration(string, time.Duration) {}
func (metrics *recordingMetrics) DlqTotal(string) {
	metrics.mu.Lock()
	metrics.dlq++
	metrics.mu.Unlock()
}

func TestMetricsRecordPublishConsumeRedeliveryAndDlq(t *testing.T) {
	server, client := newTestClient(t)
	metrics := &recordingMetrics{}
	queue, err := New(
		client, "metrics:",
		WithMetrics(metrics),
		WithBlockTimeout(10*time.Millisecond),
		WithClaimIdle(10*time.Millisecond),
		WithRecoveryInterval(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	var calls atomic.Int64
	if err := queue.Subscribe(
		"metrics.command", "metrics", func(_ context.Context, _ Message) (Result, error) {
			if calls.Add(1) == 1 {
				return Result{}, errors.New("retry")
			}
			return Result{}, Permanent(errors.New("dead letter"))
		},
		WithDeleteAfterAck(true),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	startTestQueue(t, queue)
	if err := queue.Publish(context.Background(), "metrics.command", Message{Id: "metrics-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	eventually(t, func() bool { return calls.Load() == 1 })
	server.FastForward(time.Second)
	eventually(t, func() bool {
		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		return metrics.published == 2 && metrics.consumed == 2 &&
			metrics.redelivered == 1 && metrics.dlq == 1
	})
}
