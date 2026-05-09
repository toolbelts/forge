package provider

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

func TestInstrumentationEnabledPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*viper.Viper)
		component string
		trace     bool
		metrics   bool
	}{
		{
			name: "default follows total switch",
			setup: func(v *viper.Viper) {
				v.Set("trace.enabled", true)
				v.Set("metrics.enabled", false)
			},
			component: observabilityComponentRedis,
			trace:     true,
			metrics:   false,
		},
		{
			name: "global instrumentation overrides total switch",
			setup: func(v *viper.Viper) {
				v.Set("trace.enabled", true)
				v.Set("trace.instrumentation.enabled", false)
				v.Set("metrics.enabled", false)
				v.Set("metrics.instrumentation.enabled", true)
			},
			component: observabilityComponentDatabase,
			trace:     false,
			metrics:   true,
		},
		{
			name: "component overrides global instrumentation",
			setup: func(v *viper.Viper) {
				v.Set("trace.enabled", true)
				v.Set("trace.instrumentation.enabled", false)
				v.Set("trace.instrumentation.grpc", true)
				v.Set("metrics.enabled", false)
				v.Set("metrics.instrumentation.enabled", true)
				v.Set("metrics.instrumentation.grpc", false)
			},
			component: observabilityComponentGrpc,
			trace:     true,
			metrics:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := viper.New()
			tt.setup(v)

			if got := traceInstrumentationEnabled(v, tt.component); got != tt.trace {
				t.Fatalf("traceInstrumentationEnabled() = %v, want %v", got, tt.trace)
			}
			if got := metricsInstrumentationEnabled(v, tt.component); got != tt.metrics {
				t.Fatalf("metricsInstrumentationEnabled() = %v, want %v", got, tt.metrics)
			}
		})
	}
}

func TestRedisProviderInstrumentationFlags(t *testing.T) {
	t.Parallel()

	s := miniredis.RunT(t)
	v := viper.New()
	v.Set("redis.default.addr", s.Addr())
	v.Set("trace.enabled", true)
	v.Set("trace.instrumentation.redis", false)
	v.Set("metrics.enabled", false)
	v.Set("metrics.instrumentation.redis", true)
	ctx := providerTestContext(v)

	p := &RedisProvider{}
	if err := p.Register(ctx); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	defer func() {
		if err := p.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}()

	if p.traceEnabled {
		t.Fatalf("traceEnabled = true, want false")
	}
	if !p.metricsEnabled {
		t.Fatalf("metricsEnabled = false, want true")
	}
}

func TestProviderInstrumentationFlags(t *testing.T) {
	t.Parallel()

	t.Run("database", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("trace.enabled", false)
		v.Set("metrics.enabled", true)
		v.Set("metrics.instrumentation.database", false)
		p := &DatabaseProvider{}
		if err := p.Register(providerTestContext(v)); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		if p.otelEnabled {
			t.Fatalf("otelEnabled = true, want false")
		}
	})

	t.Run("grpc", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("grpc.enabled", true)
		v.Set("grpc.addr", "127.0.0.1:0")
		v.Set("trace.enabled", false)
		v.Set("metrics.enabled", false)
		v.Set("trace.instrumentation.grpc", true)
		ctx := providerTestContext(v)
		p := &GrpcProvider{}
		if err := p.Register(ctx); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		defer func() {
			if err := p.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
		}()
		if err := p.Setup(ctx); err != nil {
			t.Fatalf("Setup() error = %v", err)
		}
		if !p.otelEnabled {
			t.Fatalf("otelEnabled = false, want true")
		}
	})

	t.Run("gateway", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("gateway.enabled", true)
		v.Set("gateway.addr", "127.0.0.1:0")
		v.Set("gateway.grpc_endpoint", "127.0.0.1:1")
		v.Set("trace.enabled", true)
		v.Set("trace.instrumentation.gateway", false)
		v.Set("metrics.enabled", false)
		v.Set("metrics.instrumentation.gateway", true)
		ctx := providerTestContext(v)
		p := &GatewayProvider{}
		if err := p.Register(ctx); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		defer func() {
			if err := p.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
		}()
		if !p.otelEnabled {
			t.Fatalf("otelEnabled = false, want true")
		}
		if p.traceEnabled {
			t.Fatalf("traceEnabled = true, want false")
		}
	})

	t.Run("jobqueue", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("jobqueue.enabled", true)
		v.Set("metrics.enabled", true)
		v.Set("metrics.instrumentation.jobqueue", false)
		ctx := providerTestContext(v)
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		defer func() { _ = client.Close() }()
		ioc.MustInstanceNamed(ctx, jobqueueDefaultRedisName, client)

		p := &JobQueueProvider{}
		if err := p.Register(ctx); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		if p.metricsEnabled {
			t.Fatalf("metricsEnabled = true, want false")
		}
	})
}

func providerTestContext(v *viper.Viper) context.Context {
	ctx := ioc.NewContainer().WithContext(context.Background())
	ctx = log.Logger.WithContext(ctx)
	ioc.MustInstance(ctx, v)
	return ctx
}
