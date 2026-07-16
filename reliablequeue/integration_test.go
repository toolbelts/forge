package reliablequeue

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestIntegrationWaitReplication(t *testing.T) {
	addr := os.Getenv("FORGE_RELIABLEQUEUE_REDIS_ADDR")
	if addr == "" {
		t.Skip("FORGE_RELIABLEQUEUE_REDIS_ADDR is not set")
	}
	replicas := 1
	if raw := os.Getenv("FORGE_RELIABLEQUEUE_WAIT_REPLICAS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid FORGE_RELIABLEQUEUE_WAIT_REPLICAS %q", raw)
		}
		replicas = parsed
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	queue, err := New(client, "forge:integration:", WithWaitReplicas(replicas, 2*time.Second))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	message := Message{Id: "wait-" + strconv.FormatInt(time.Now().UnixNano(), 10), Payload: []byte(`{}`)}
	if err := queue.Publish(context.Background(), "wait", message); err != nil {
		t.Fatalf("publish with replica confirmation: %v", err)
	}
}
