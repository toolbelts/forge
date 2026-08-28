package provider

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/toolbelts/forge/ioc"
)

func TestDbcacheProviderInvalidationSubscriptionLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		store       string
		enabled     bool
		channel     string
		wantChannel string
		wantSubs    int64
	}{
		{
			name:        "tiered default channel",
			store:       "tiered",
			enabled:     true,
			wantChannel: "dbcache:users:invalidate",
			wantSubs:    1,
		},
		{
			name:        "tiered explicit channel",
			store:       "tiered",
			enabled:     true,
			channel:     "app:users:invalidate",
			wantChannel: "app:users:invalidate",
			wantSubs:    1,
		},
		{
			name:        "tiered disabled",
			store:       "tiered",
			wantChannel: "dbcache:users:invalidate",
			wantSubs:    0,
		},
		{
			name:        "redis publishes without subscribing",
			store:       "redis",
			enabled:     true,
			wantChannel: "dbcache:users:invalidate",
			wantSubs:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			ctx := newProviderTestContext(t, map[string]any{
				"dbcache.users.store":                tc.store,
				"dbcache.users.redis":                "cache",
				"dbcache.users.invalidation.enabled": tc.enabled,
				"dbcache.users.invalidation.channel": tc.channel,
			})
			ioc.MustInstanceNamed[redis.UniversalClient](ctx, "cache", client)

			p := &DbcacheProvider{}
			if err := p.Register(ctx); err != nil {
				t.Fatalf("register: %v", err)
			}
			waitForProviderSubscribers(t, client, tc.wantChannel, tc.wantSubs)
			if err := p.Shutdown(ctx); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			waitForProviderSubscribers(t, client, tc.wantChannel, 0)
		})
	}
}

func waitForProviderSubscribers(t *testing.T, client redis.UniversalClient, channel string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		counts, err := client.PubSubNumSub(ctx, channel).Result()
		if err == nil && counts[channel] == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait subscribers channel=%q want=%d: counts=%v err=%v", channel, want, counts, err)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
